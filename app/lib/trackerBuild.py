"""
Main controller script for tracker module
Builds tracker output for google sheet using lots of modules in package

Crawls paths in config to find all accessions
Crawls for idf/sdrf for metadata
Crawls dbs for metadata
assembles tracker info in dataframes
exports to google sheets
exports to pickle dump
"""

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "08/11/2019"

from app.lib import statusCrawl
from app.lib import fileCrawler
from app.lib import dbCrawl
from app.lib.googleAPI import google_sheet_output
from datetime import datetime
import pandas as pd
from collections import OrderedDict
from collections import defaultdict
import pickle
import json
import os
import sys
import requests
import re
import math
import numpy as np
import time
import psycopg
import logging


class tracker_build:
    def __init__(self, sources_config, db_config, atlas_supported_species, spreadsheetname, google_client_secret=None):
        logging.debug("Starting tracker build in debug model")

        # robust tries with backoff
        tries = 4
        initial_delay = 5
        backoff_rate = 10

        for n in range(tries + 1):
            if n != 0:
                print('Retry no. {}/{}'.format(n, tries))
                print('Waiting {} sec'.format(initial_delay))
                time.sleep(initial_delay)
                initial_delay = initial_delay * backoff_rate
            try:
                # configuration
                self.timestamp = datetime.fromtimestamp(datetime.now().timestamp()).isoformat()
                self.status_type_order = ['external', 'incoming', 'loading', 'analysing', 'processed', 'published_dev', 'published']
                self.google_client_secret = google_client_secret
                self.spreadsheetname = spreadsheetname
                self.atlas_supported_species = self.get_atlas_species(atlas_supported_species)

                # crawling
                self.status_crawl = statusCrawl.atlas_status(sources_config, self.status_type_order)  # accession search on nfs, glob func
                logging.info("Atlas status crawled")
                self.db_crawl = dbCrawl.db_crawler(db_config, self.status_crawl)  # db lookups for metadata and urls
                logging.info("Database crawled")
                self.file_metadata = fileCrawler.file_crawler(self.status_crawl, sources_config)  # in file crawling on nfs
                logging.info("File metadata crawled")

                # output
                output_dfs = self.df_compiler()  # this function should be edited to change the information exported to the google sheets output
                logging.info("Compile the output into a dataframe")

                # automatically generate Expression Atlas config files for atlas-eligible bulk RNA-seq studies
                logging.debug('discover_exp dataframe head:\n {}'.format(output_dfs["Discover Experiments"].head()))
                output_dfs["Discover Experiments"] = self.auto_config(sources_config, df=output_dfs["Discover Experiments"])
                logging.info("Create config.auto for bulk atlas RNA-seq exps")

                # exported to dev - https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit#gid=0
                google_sheet_output(google_client_secret, output_dfs, self.spreadsheetname)
                logging.info("Save the output into google spreadsheets")

                # self.pickle_out()
                break
            except (KeyboardInterrupt, SystemExit):
                sys.exit()
            except psycopg.OperationalError:
                logging.error('Problem related to Atlas Production server. Please check if confidentials are up-to-date.\nRemember to update it in db_config.json on cluster if necessary.')
                raise
            except:
                print('Attempt {} FAILED'.format(n + 1))
                print("Unexpected error:", sys.exc_info()[0])
                if n == tries:
                    raise RuntimeError('Hit {} max retries. See errors above'.format(tries))
                continue

    @staticmethod
    def get_atlas_species(supported_species):
        """
        Supported species taken from list fo files in github here https://github.com/ebi-gene-expression-group/atlas-annotations/tree/develop/annsrcs
        For each git directory find the api url and pass to this function e.g. https://api.github.com/repos/ebi-gene-expression-group/atlas-annotations/git/trees/763aa3ef034348daa0e189d0c52c17edc9a97afc
        Pass as many dir as you need with -q arg.
        These directories contain files whose name are the species we support.
        This function just returns file names as a list.
        These are the species names that Atlas supports.
        """

        species_list = []
        for url in supported_species:
            response = requests.request("GET", url)
            assert response.status_code == 200, 'Bad response {} for URL {}'.format(response.status_code, url)
            data = response.json()
            for doc in data.get('tree'):
                species_name = re.sub('[^A-Za-z0-9]+', ' ', doc.get('path')).lower()  # sanitise special chars
                species_list.append(species_name)
        return species_list

    def get_species_status(self):
        """
        Assigns status to organisms based on support in Atlas and presence in ENSEMBLE (latter feature in dev)
        """
        species_status = defaultdict(str)
        for accession, organism in self.file_metadata.extracted_metadata.get('Organism').items():
            if organism.lower() in self.atlas_supported_species:
                species_status[accession] = 'Supported in Atlas'
            else:
                species_status[accession] = 'Not supported'
        return species_status

    @staticmethod
    def get_already_ingested_warn(in_df, ex_df):
        """
        This code flags the following three conditions:
        1. Check if any secondary accessions in the internal sheet are in the secondary accessions external sheet
        2. Check if any primary accessions in the internal sheet are in the secondary accessions external sheet (CURD usacase)
        3. If primary GEO accessions in the discovery are converted to GSE. Do these match any secondary accessions in the internal sheet?
        """

        already_ingested_warning = {}

        def reverse_dictionary(d):
            rev_d = defaultdict(list)
            for key, value_list in d.items():
                assert type(value_list) == list or math.isnan(value_list), 'This method only works with list dict types. Value: "{}" is type "{}"'.format(value_list, type(value_list))
                if type(value_list) == list:
                    for v in value_list:
                        rev_d[v].append(key)
            return rev_d

        in_2nd_acc = in_df['Secondary Accessions'].to_dict()
        ex_2nd_acc = ex_df['Secondary Accessions'].to_dict()

        reverse_in_2nd_acc = reverse_dictionary(in_2nd_acc)  # reverse dict used for lookup

        for accession, secondary_accession in ex_2nd_acc.items():
            if isinstance(secondary_accession, list) and isinstance(accession, str):
                assert all(isinstance(item, str) for item in secondary_accession), 'Wrong datatype in secondary accession list: {}'.format(str(secondary_accession))

                # Special treatment for GEO
                geo_prefix = 'E-GEOD-'  # warning hard coded
                geo_replacement = 'GSE'  # warning hard coded
                if accession.startswith(geo_prefix):
                    all_accessions = [accession] + secondary_accession + [accession.replace(geo_prefix, geo_replacement)]
                else:
                    all_accessions = [accession] + secondary_accession

                for a in all_accessions:
                    hits = []
                    if a in reverse_in_2nd_acc:
                        hits += reverse_in_2nd_acc.get(a)
                    if hits:
                        m = 'WARNING Already Ingested. See {}'.format(' & '.join(hits))
                        already_ingested_warning[accession] = m

        # add new dict to external df
        ex_df['Already Ingested'] = pd.Series(already_ingested_warning)
        return ex_df

    @staticmethod
    def formatting(col):
        """
        Rules for each cell in dataframe applied afterwards
        This is slightly slower than pre deciding but allows formatting to be applied generally.
        """

        for k, v in col.items():
            if isinstance(v, list):
                col.loc[k] = ' & '.join(v)

    def df_compiler(self):
        """
        Combines experiment accession keyed dictionaries.
        Add more dict to input_dict to add mor info to tracker.
        You may want to adjust column auto column widths in googleAPI.py
        """

        print('Combining results into summary dataframe {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))

        input_dicts = {"Status": self.status_crawl.accession_final_status,
                       "Tech Type": self.status_crawl.tech,
                       "Web Link": self.db_crawl.accession_urls,
                       "Discovery Location": self.status_crawl.path_by_accession,
                       "Investigation Title": self.file_metadata.extracted_metadata.get('Investigation Title'),
                       "Experiment Type": self.file_metadata.extracted_metadata.get('Experiment Type'),
                       "Analysis Type": self.file_metadata.extracted_metadata.get('Analysis Type'),
                       "Organism": self.file_metadata.extracted_metadata.get('Organism'),
                       "Organism Status": self.get_species_status(),
                       "Single-cell Experiment Type": self.file_metadata.extracted_metadata.get('Single-cell Experiment Type'),
                       "Secondary Accessions": self.file_metadata.extracted_metadata.get('Secondary Accession'),
                       "IDF": self.status_crawl.idf_path_by_accession,
                       "SDRF": self.status_crawl.sdrf_path_by_accession,
                       "Last Modified": self.file_metadata.mod_time,
                       "Atlas Eligibility": self.db_crawl.atlas_eligibility_status,
                       "GeneQuantSoft": self.file_metadata.extracted_metadata.get('GeneQuantSoft'),
                       "GQSVersion": self.file_metadata.extracted_metadata.get('GQSVersion'),
                       "MappingSoft": self.file_metadata.extracted_metadata.get('MappingSoft'),
                       "MappingSoftVersion": self.file_metadata.extracted_metadata.get('MappingSoftVersion'),
                       "E!Version": self.file_metadata.extracted_metadata.get('E!Version'),
                       "TransQuantSoft": self.file_metadata.extracted_metadata.get('TransQuantSoft'),
                       "TQSVersion": self.file_metadata.extracted_metadata.get('TQSVersion'),
                       "Curator": {**self.file_metadata.curators_by_acession, **self.file_metadata.extracted_metadata.get('Curator')},
                       "min_status": self.status_crawl.accession_min_status,  # filter
                       "max_status": self.status_crawl.accession_min_status  # filter
                       }
        input_data = {}
        for colname, input_dict in input_dicts.items():
            for accession, value in input_dict.items():
                if accession not in input_data:
                    input_data[accession] = {colname: value}
                else:
                    input_data[accession].update({colname: value})

        # df parsing/filtering
        full_df = pd.DataFrame.from_dict(input_data, orient='index')
        nan_filtered_df = full_df[pd.notnull(full_df['Status'])]  # filter if status is missing (ID found in DB not in config loc)

        nan_filtered_df['min_order_index'] = nan_filtered_df.apply(lambda x: self.status_type_order.index(x['min_status']), axis=1)  # add index column

        external_df_ = nan_filtered_df[(nan_filtered_df["min_order_index"] < 1)].rename_axis(index='Accession')  # filter out loading and lower (index based see status_type_order!)
        internal_df = nan_filtered_df[(nan_filtered_df["min_order_index"] >= 1)].rename_axis(index='Accession')  # filter out loading and lower (index based see status_type_order!)

        # Add warn if already ingested. Internal vs external sheets.
        external_df = self.get_already_ingested_warn(internal_df, external_df_)

        # order and collect dfs
        output_dfs = OrderedDict()
        output_dfs["Discover Experiments"] = external_df.drop(['Web Link',
                                                               'GeneQuantSoft',
                                                               'GQSVersion',
                                                               'MappingSoft',
                                                               'MappingSoftVersion',
                                                               'E!Version',
                                                               'TransQuantSoft',
                                                               'TQSVersion',
                                                               'Curator',
                                                               'Experiment Type',
                                                               'Single-cell Experiment Type',
                                                               'Tech Type'
                                                               ], axis=1)  # remove columns from specific df
        output_dfs["Track Ingested Experiments"] = internal_df.drop(['Organism Status'], axis=1)

        # remove these columns from all dfs
        remove_cols = ['min_status', 'max_status', 'min_order_index']
        for name, df in output_dfs.items():
            output_dfs[name] = df.drop(remove_cols, axis=1)

        # add value formatting function here e.g. list and none handling
        for name, df in output_dfs.items():
            df.apply(self.formatting)

        return output_dfs

    @staticmethod
    def auto_config(sources_config, df):
        df["AutoConfig Location"] = ""

        # bacterial studies are not ingested into Altas anymore, so not create auto configs for them.
        # currently simply exclude bacteria in atlas-eligible species list at:
        # https://github.com/ebi-gene-expression-group/atlas-annotations/tree/develop/annsrcs/ensembl
        # todo: can implement a programmatic way
        fungi_list = ['Aspergillus fumigatus', 'Aspergillus nidulans', 'Saccharomyces cerevisiae',
                      'Schizosaccharomyces pombe', 'Yarrowia lipolytica']
        logging.info("Exclude fungi studies from auto config creation")

        # get path to conan_incoming from sources_config, in which should have one conan_incoming folder only
        with open(sources_config) as f:
            sources_config_json = json.load(f)
        conan_incoming = [x for x, y in sources_config_json.items() if "conan_incoming" in x][0]
        logging.info("get conan_incoming path %s from sources_config", conan_incoming)

        for i, row in df.iterrows():
            logging.debug("check %s", i)  # i is the accession, eg: E-MTAB-12730

            if row["Atlas Eligibility"] == "PASS" \
                    and (row["Organism Status"] == "Supported in Atlas" and row["Organism"] not in fungi_list) \
                    and ("E-MTAB" in i or "E-GEOD" in i) \
                    and ("seq" in row["Analysis Type"] and "RNA" in row["Analysis Type"] and "single" not in row["Analysis Type"]):
                exp = i
                exp_path = conan_incoming + '/' + exp
                logging.debug('%s is qualified to generate configs automatically', exp)

                # remove empty exp folder if it exists in conan_incoming
                if os.path.exists(exp_path) and len(os.listdir(exp_path)) == 0:
                    os.remove(path)
                    logging.debug('delete %s\'s empty folder in conan_incoming and continue', exp)

                # if exp folder not existed in conan_incoming, create the folder and generate config files
                if not os.path.exists(exp_path):
                    os.makedirs(exp_path)

                    # first try differential by default
                    exitcode = os.system(
                        'conda run -n curation gxa_generateConfigurationForExperiment.pl -e ' + exp + ' -t differential -p ' + (
                            "annotare" if "MTAB" in exp else "geo") + ' -o ' + exp)
                    if exitcode != 0:  # baseline
                        os.system(
                            'conda run -n curation gxa_generateConfigurationForExperiment.pl -e ' + exp + ' -t baseline -p ' + (
                                "annotare" if "MTAB" in exp else "geo") + ' -o ' + exp)
                        logging.debug("%s baseline auto config created", exp)
                    else:
                        logging.debug("%s differential auto config created", exp)

                    df.loc[i, "AutoConfig Location"] = exp_path
                    logging.debug("add path to %s auto config in output table", exp)

        return df

    def pickle_out(self):
        if not os.path.exists('logs'):
            os.makedirs('logs')

        # binary log of results
        filename = 'logs/' + str(self.timestamp) + '.atlas_status.log'
        filehandler = open(filename, 'wb')
        pickle.dump(self, filehandler)
        print('Crawler results dumped to pickle')

        # human readable log
        filename = 'logs/last_run_text.log'
        data = {
            "Primary accessions found": list(self.status_crawl.all_primary_accessions),
            "Detected empty file error": list(self.file_metadata.emptyfile_error_paths),
            "Detected unicode errors": list(self.file_metadata.unicode_error_paths)
        }
        with open(filename, 'w') as filehandler:
            json.dump(data, filehandler)


'''
Profiling performed before refactoring

Top 3 modules by % runtime:

idf_sdrf_metadata_scraper 69% This has since been updated and is 10x faster
get_latest_idf_sdrf 20%
get_file_modified_date 8.3%

idf_sdrf_metadata_scraper: opens and reads files therefore takes some time. Other tweeks were not faster.
get_latest_idf_sdrf: not looked at improving this yet
get_file_modified_date: This could be used to checkup against last pickled run output to avoid opening files that were already read if speed becomes a blocker
'''
