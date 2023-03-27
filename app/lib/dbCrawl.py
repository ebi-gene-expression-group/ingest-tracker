"""
functions that crawl databases to extract tracking metadata

"""

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "08/11/2019"

import mysql.connector
from tqdm import tqdm
import psycopg
import json
import pandas as pd
import sys
import logging


class db_crawler:

    def __init__(self, db_config, status_crawl):

        # initialize
        with open(db_config) as d:
            self.db_config = json.load(d)
        self.status_crawl = status_crawl

        self.atlas_eligibility_status = self.get_atlas_eligibility_status()
        self.accession_urls = self.get_accession_urls()
        self.db_vs_crawler_check()

    def db_connect(self, name):
        """
        connects to a db by name given credentials in the config file
        """
        connection_details = self.db_config.get(name)
        # return db connector
        if connection_details['dbtype'] == 'mysql':
            db = mysql.connector.connect(host=connection_details['host'],
                                         user=connection_details['user'],
                                         password=connection_details['password'],
                                         port=connection_details['port'],
                                         database=name)
        elif connection_details['dbtype'] == 'postgres':
            db = psycopg.connect(host=connection_details['host'],
                                 user=connection_details['user'],
                                 password=connection_details['password'],
                                 port=connection_details['port'],
                                 dbname=name)
        else:
            raise ValueError('DB type {} not interpreted. Review db_config.json.'.format(connection_details['dbtype']))
        return db

    def get_columns(self, db, table, columns):
        """
        returns dataframe with specified columns given a db object and table
        columns[0] taken as primary key
        """
        cursor = db.cursor()
        query = "SELECT {} FROM {}".format(', '.join(columns), table)
        cursor.execute(query)
        result = []
        for row in cursor:
            if row[0]:
                result.append({k: row[idx] for idx, k in enumerate(columns)})

        return pd.DataFrame(result).set_index(columns[0])

    def db_vs_crawler_check(self):
        """
        Looks for accessions in production db that were not picked up in nfs crawl
        """
        db = self.db_connect('gxpatlaspro')
        logging.debug("bulk atlasprod connected")
        bulk_access = self.get_columns(db, 'experiment', ['accession'])
        bulk_access['bulk/sc'] = 'bulk'
        logging.info("query to bulk atlasprod for experiments not found in nfs crawl")

        db = self.db_connect('gxpscxapro')
        logging.debug("single-cell atlasprod connected")
        sc_access = self.get_columns(db, 'experiment', ['accession'])
        sc_access['bulk/sc'] = 'sc'
        logging.info("query to single-cell atlasprod for experiments not found in nfs crawl")

        df = pd.concat([bulk_access, sc_access])  # data needed to construct url
        db_accession_list = df.index.to_list()
        crawler_accessions_list = list(self.status_crawl.accession_final_status.keys())
        diff = [x for x in db_accession_list if x not in crawler_accessions_list]
        if len(diff) > 0:
            print('WARNING: {} accessions were found in production DB but were not picked up by crawler\n {}\nThese have not been added to the tracker.'.format(len(diff), str(diff)))

    def get_accession_urls(self):
        """
        returns clickthrough url for accession if the accession is published at www or wwwdev
        """
        db = self.db_connect('gxpatlaspro')
        logging.debug("bulk atlasprod connected")
        bulk_access = self.get_columns(db, 'experiment', ['accession', 'private', 'access_key'])
        bulk_access['bulk/sc'] = 'bulk'
        logging.info("query to bulk atlasprod for urls")

        db = self.db_connect('gxpscxapro')
        logging.debug("single-cell atlasprod connected")
        sc_access = self.get_columns(db, 'experiment', ['accession', 'private', 'access_key'])
        sc_access['bulk/sc'] = 'sc'
        logging.info("query to bulk atlasprod for urls")

        url_map_data = pd.concat([bulk_access, sc_access])  # data needed to construct url

        url_map = {}

        for accession, row in url_map_data.iterrows():
            access_key = row['access_key']
            private = row['private']
            bulk_sc = row['bulk/sc']
            status = self.status_crawl.accession_final_status.get(accession, None)

            if status == 'published' and bulk_sc == 'sc' and not private:
                url = 'https://www.ebi.ac.uk/gxa/sc/experiments/{}'.format(accession)
            elif status == 'published_dev' and bulk_sc == 'sc' and not private:
                url = 'https://wwwdev.ebi.ac.uk/gxa/sc/experiments/{}'.format(accession)

            elif status == 'published' and bulk_sc == 'bulk' and not private:
                url = 'https://www.ebi.ac.uk/gxa/experiments/{}'.format(accession)
            elif status == 'published_dev' and bulk_sc == 'bulk' and not private:
                url = 'https://wwwdev.ebi.ac.uk/gxa/experiments/{}'.format(accession)

            elif bulk_sc == 'bulk' and private:
                url = 'https://wwwdev.ebi.ac.uk/gxa/experiments/{}/Results?accessKey={}'.format(accession, access_key)

            # todo add logic to determine if private samples are www or wwwdev
            # elif status == 'published' and bulk_sc == 'bulk' and private:
            #     url = 'https://www.ebi.ac.uk/gxa/experiments/{}/Results?accessKey={}'.format(accession, access_key)
            else:
                print('Accession: {} could not be mapped to a url.'.format(accession))
                url = None

            url_map[accession] = url

        return url_map

    def get_atlas_eligibility_status(self):
        """
        returns accession keyed dict with status
        """
        db = self.db_connect('gxpatlaspro')
        logging.debug("bulk atlasprod connected")
        rnaseq_atlas_eligibility = self.get_columns(db, 'rnaseq_atlas_eligibility', ['ae2_acc', 'status']).rename(columns={"status": "eligibility_status"})
        logging.info("query to bulk atlasprod for atlas eligibility")

        db = self.db_connect('ae_autosubs')
        logging.debug("autosubs connected")
        autosubs_atlas_fail_score = self.get_columns(db, 'experiments', ['accession', 'atlas_fail_score']).rename(columns={"atlas_fail_score": "eligibility_status"})
        logging.info("query to autosubs for atlas eligibility")

        eligibility_dict_ = pd.concat([rnaseq_atlas_eligibility, autosubs_atlas_fail_score])['eligibility_status'].to_dict()

        # removes entries that do not have a status in self.accession_final_status (lots removed)
        # todo the removed entries could be captured as 'external' projects
        eligibility_dict = {k: v for k, v in eligibility_dict_.items() if k in self.status_crawl.accession_final_status}

        return eligibility_dict
