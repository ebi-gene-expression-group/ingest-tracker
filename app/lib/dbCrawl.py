'''
functions that crawl databases to extract tracking metadata
'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "08/11/2019"

import mysql.connector
from tqdm import tqdm
import psycopg2
import json

class db_crawler:

    def __init__(self, db_config, status_crawl):

        with open(db_config) as d:
            self.db_config = json.load(d)
        self.status_crawl = status_crawl
        self.atlas_eligibility_by_accession = self.get_atlas_eligibility_status()


    def get_atlas_eligibility_status(self): # this func connects to various atlas db because output is not in logs

        atlas_eligibility_status = {}
        for name, connection_details in self.db_config.items():
            print('\nSearching {} db for atlas eligibility score'.format(name))
            if connection_details['dbtype'] == 'mysql':
                db = mysql.connector.connect(host=connection_details['host'],
                                       user=connection_details['user'],
                                       password=connection_details['password'],
                                       port=connection_details['port'],
                                       database=name)
            elif connection_details['dbtype'] == 'postgres':
                db = psycopg2.connect(host=connection_details['host'],
                                       user=connection_details['user'],
                                       password=connection_details['password'],
                                       port=connection_details['port'],
                                       dbname=name)
            else:
                raise ValueError('DB type {} not interpreted. Review db_config.json.'.format(connection_details['dbtype']))


            cursor = db.cursor()
            query = "SELECT {} FROM {}".format(connection_details['atlas_eligibility_status_columns'], connection_details['atlas_eligibility_status_table'])
            cursor.execute(query)

            for row in tqdm(cursor):
                accession = row[0]
                atlas_fail_score = row[1]
                if atlas_fail_score and accession not in atlas_eligibility_status:
                    atlas_eligibility_status[accession] = atlas_fail_score

        print('\n{} atlas status checks were detected in atlas databases'.format(len(atlas_eligibility_status)))
        pop_list = []
        for accession in atlas_eligibility_status.keys():
            if accession not in self.status_crawl.accession_final_status:
                pop_list.append(accession)
        for accession in pop_list:
            atlas_eligibility_status.pop(accession)
        print('{} of these accessions were detected in source_config locations and these were added to the state tracker\n'.format(len(atlas_eligibility_status)))

        return atlas_eligibility_status # remove entries that do not have a status in self.accession_final_status
