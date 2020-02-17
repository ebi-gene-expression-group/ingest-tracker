'''
Main controller script for tracker module
Builds tracker output for google sheet using lots of modules in package

Crawls paths in config to find all accessions
Crawls for idf/sdrf for metadata
Crawls dbs for metadata
assembles tracker info in dataframes
exports to google sheets
exports to pickle dump
'''
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

class tracker_build:
    def __init__(self, sources_config, db_config, atlas_supported_species, google_client_secret=None, google_output=True,spreadsheetname="DEV Ingest Status"):

        # configuration
        self.timestamp = datetime.fromtimestamp(datetime.now().timestamp()).isoformat()
        self.status_type_order = ['external', 'incoming', 'loading', 'analysing', 'processed', 'published_dev', 'published']
        self.google_client_secret = google_client_secret
        self.spreadsheetname = spreadsheetname
        self.atlas_supported_species = self.get_atlas_species(atlas_supported_species)

        # crawling
        self.status_crawl = statusCrawl.atlas_status(sources_config, self.status_type_order) # accession search on nfs, glob func
        self.file_metadata = fileCrawler.file_crawler(self.status_crawl, sources_config) # in file crawling on nfs
        self.db_crawl = dbCrawl.db_crawler(db_config, self.status_crawl) # db lookups for metadata and urls
        # output
        if google_output:
            output_dfs = self.df_compiler()  # this function should be edited to change the information exported to the google sheets output
            # exported to https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit#gid=0
            google_sheet_output(google_client_secret, output_dfs, self.spreadsheetname)

        self.pickle_out()

    def get_atlas_species(self, supported_species):
        '''
        Supported species taken from list fo files in git hub here https://github.com/ebi-gene-expression-group/atlas-annotations/tree/develop/annsrcs
        For each git directory find the api url and pass to this function e.g. https://api.github.com/repos/ebi-gene-expression-group/atlas-annotations/git/trees/763aa3ef034348daa0e189d0c52c17edc9a97afc
        Pass as many dir as you need with -q arg.
        These directories contain files who's name are the species we support.
        This function just returns file names as a list.
        These are the species names that Atlas supports.
        '''

        species_list = []
        for url in supported_species:
            response = requests.request("GET", url)
            assert response.status_code == 200, 'Bad response {} for URL {}'.format(response.status_code, url)
            data = response.json()
            for doc in data.get('tree'):
                species_name = re.sub('[^A-Za-z0-9]+', ' ', doc.get('path')).lower() # sanitise special chars
                species_list.append(species_name)
        return species_list

    def get_species_status(self):
        '''
        Assigns status to organisms based on supoort in Atlas and presence in ENSEMBLE (latter feature in dev)
        '''
        species_status = defaultdict(str)
        for accession, organism in self.file_metadata.extracted_metadata.get('Organism').items():
            if organism.lower() in self.atlas_supported_species:
                species_status[accession] = 'Supported in Atlas'
            else:
                species_status[accession] = 'Not supported'
        return species_status


    def df_compiler(self):
        '''
        Combines experiment accession keyed dictionaries.
        Add more dict to input_dict to add mor info to tracker.
        You may want to adjust column auto column widths in googleAPI.py
        '''


        print('Combining results into summary dataframe {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))

        input_dicts = {"Status": self.status_crawl.accession_final_status,
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
                       "min_status": self.status_crawl.accession_min_status, # filter
                       "max_status": self.status_crawl.accession_min_status # filter
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
        nan_filtered_df = full_df[pd.notnull(full_df['Status'])] # filter if status is missing (ID found in DB not in config loc)

        nan_filtered_df['min_order_index'] = nan_filtered_df.apply(lambda x: self.status_type_order.index(x['min_status']), axis=1) # add index column

        external_df = nan_filtered_df[(nan_filtered_df["min_order_index"] < 1)]  # filter out loading and lower (index based see status_type_order!)
        internal_df = nan_filtered_df[(nan_filtered_df["min_order_index"] >= 1)]  # filter out loading and lower (index based see status_type_order!)

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
                                                               'Single-cell Experiment Type'
                                                               ], axis=1) # remove columns from specific df
        output_dfs["Track Ingested Experiments"] = internal_df.drop[
                                                                    'Organism Status'
                                                                            ]

        # remove these columns from all dfs
        remove_cols = ['min_status', 'max_status', 'min_order_index']
        for name, df in output_dfs.items():
            output_dfs[name] = df.drop(remove_cols, axis=1)

        return output_dfs

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