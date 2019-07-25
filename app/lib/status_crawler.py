'''
crawls fg log files for status of experiments in system
'''
__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "15/07/2019"

import json
import os
import pandas as pd
import re
import glob
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe
from datetime import datetime
import csv

# # set working dir to path of this script.
# abspath = os.path.abspath(__file__)
# dname = os.path.dirname(abspath)
# os.chdir(dname)
# os.chdir('../../') # LOCAL TEST ONLY!!!

class atlas_status:
    def __init__(self, sources_config, google_client_secret, google_output=True):

        # configuration
        self.google_client_secret = google_client_secret
        with open(sources_config) as f:
            self.sources_config = json.load(f)
        self.status_types = self.get_status_types()
        self.timestamp = datetime.fromtimestamp(datetime.now().timestamp()).isoformat()

        # accession search
        self.found_accessions = self.accession_search() # scans dir in config to find '*.idf.txt' or accession directories
        self.accession_final_status = self.status_tracker() # determines status of each dataset based on location of files
        self.secondary_accession_mapper() # extracts secondary accessions from idf making a map dict and a complete set (all_secondary_accessions) for duplication checks
        self.get_latest_idf_sdrf()  # finds path to latest idf and sdrf file


        # metadata crawling
        self.mod_time = self.get_file_modified_date()
        self.extracted_metadata = self.idf_sdrf_metadata_scraper()

        # output
        if google_output:
            output_df = self.df_compiler() # this function should be edited to change the information exported to the google sheets output
            self.google_sheet_output(output_df) # table exported to https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit#gid=0

    def get_status_types(self):
        status_types = set()
        for x, y in self.sources_config.items():
            for n in y['stage']:
                status_types.add(n)
        return list(status_types)

    def accession_search(self):
        found_accessions = {}
        for path, info in self.sources_config.items():


            pre_accessions = os.listdir(path)
            accession_regex = re.compile('^E-(GEOD|MTAB|PROT|ENAD|AFMX|CURD|EHCA|MEXP|TABM|NASC|ERAD|GEUV|JJRD|ATMX|HCAD|MIMR|CBIL|MAXD)-[0-9]+$')
            for pre_accession in pre_accessions:
                accession = pre_accession.strip('.idf.txt')
                if accession_regex.match(accession):
                    found_accessions[(path, accession)] = dict(
                                                                    # config_path=path,
                                                                    accession=accession,
                                                                    tech=info.get('tech', None),
                                                                    stage=info.get('stage', None),
                                                                    resource=info.get('resource', None),
                                                                    source=info.get('source', None)
                    )
        return found_accessions

    def status_tracker(self):
        accession_status = {}
        for key, value in self.found_accessions.items():
            accession = value['accession']
            stage = value['stage']

            if accession not in accession_status:
                status_bool = {}

            # accurate bool table but final status determination is not conservative
                for status in self.status_types:
                    status_bool[status] = (status in stage)
                accession_status[accession] = status_bool
            else:
                for status in stage:
                    accession_status[accession][status] = True

            # todo determine and support bespoke logic to determine true state of dual paths

        accession_final_status = {}
        status_type_order = ['external','incoming','loading','analysing','processed','published']
        assert set(self.status_types) == set(status_type_order), 'Unrecognised status in config. Please update status type order list.'
        for accession, status_bool in accession_status.items():
            for status_type in reversed(status_type_order):
                if status_bool.get(status_type):
                    accession_final_status[accession] = status_type
                    break
        # self.status_totals = pd.DataFrame.from_dict(self.accession_final_status, orient='index')[0].value_counts()
        return accession_final_status

    def secondary_accession_mapper(self):
        idf_files = []
        for path, values in self.sources_config.items():
            f = [f for f in glob.glob(path + "**/*.idf.txt", recursive=True)]
            idf_files += f

        self.all_secondary_accessions = set()
        self.secondary_accessions_mapping = {}
        for idf_file in idf_files:
            accession = idf_file.split('/')[-1].strip('.idf.txt')
            with open (idf_file, "r") as f:
                contents = f.read()
                search = re.findall(r'Comment \[SecondaryAccession\](.*?)\n', contents)
                if search:
                    secondary_accessions = [x.strip('\t') for x in search]
                    if len(secondary_accessions) == 1 and len(secondary_accessions[0]) == 0:
                        continue
                    else:
                        self.secondary_accessions_mapping[accession] = secondary_accessions
                        self.all_secondary_accessions.update(secondary_accessions)

    def get_latest_idf_sdrf(self):
        global idf_path, sdrf_path
        files_found = {}
        for path, metadata in self.sources_config.items():
            idf_list = glob.glob(path + '/*/*.idf.txt') + glob.glob(path + '/*.idf.txt')
            sdrf_list = glob.glob(path + '/*/*.sdrf.txt') + glob.glob(path + '/*.sdrf.txt')
            files_found[path] = dict(idf_list=idf_list,sdrf_list=sdrf_list)

        self.latest_idf = {}
        self.latest_sdrf = {}
        for accession, status in self.accession_final_status.items():
            look_in = []
            for path, metadata in self.sources_config.items():
                if status in metadata['stage']:
                    look_in.append(path)

            idf_accession_pattern1 = path + '/' + accession + '/' + accession + '.idf.txt'
            idf_accession_pattern2 = path + '/' + accession + '.idf.txt'
            sdrf_accession_pattern1 = path + '/' + accession + '/' + accession + '.sdrf.txt'
            sdrf_accession_pattern2 = path + '/' + accession + '.sdrf.txt'

            for look_path in look_in:
                idf_paths = files_found.get(look_path).get('idf_list')
                sdrf_paths = files_found.get(look_path).get('sdrf_list')

                if idf_accession_pattern1 in idf_paths:
                    idf_path = idf_accession_pattern1
                elif idf_accession_pattern2 in idf_paths:
                    idf_path = idf_accession_pattern2
                else:
                    idf_path = None
                if sdrf_accession_pattern1 in sdrf_paths:
                    sdrf_path = sdrf_accession_pattern1
                elif sdrf_accession_pattern2 in sdrf_paths:
                    sdrf_path = sdrf_accession_pattern2
                else:
                    sdrf_path = None

                if sdrf_path and idf_path:
                    self.latest_idf[accession] = idf_path
                    self.latest_sdrf[accession] = sdrf_path
                    break
                else:
                    continue
            self.latest_idf[accession] = idf_path
            self.latest_sdrf[accession] = sdrf_path

    def google_sheet_output(self, output_df):
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.google_client_secret, scope)
        client = gspread.authorize(creds)
        sheet = client.open("Ingest Status") # this is the spreadsheet not the worksheet

        # add new empty worksheet
        sheet.add_worksheet(title="In progress {}".format(self.timestamp), rows=output_df.shape[0]+1, cols=output_df.shape[1]+1)
        # fill new empty worksheet
        gspread_dataframe.set_with_dataframe(sheet.get_worksheet(1), output_df, include_index=True, include_column_header=True)
        # remove old worksheet in pos 0
        sheet.del_worksheet(sheet.get_worksheet(0))

    def df_compiler(self):

        # combine accession keyed dictionaries
        # NB extracted metadata is an extra dict of dicts with various values from metadata scraping
        input_dicts = {"Status":self.accession_final_status,
                    "Secondary Accessions": self.secondary_accessions_mapping,
                    "IDF": self.latest_idf,
                    "SDRF": self.latest_sdrf,
                    "Last Modified": self.mod_time
                   }
        input_dicts.update(self.extracted_metadata)
        input_data = {}
        for colname, input_dict in input_dicts.items():
            for accession, value in input_dict.items():
                if accession not in input_data:
                    input_data[accession] = {colname:value}
                else:
                    input_data[accession].update({colname:value})

        # df parsing
        full_df = pd.DataFrame.from_dict(input_data, orient = 'index')
        filtered_df = full_df[(full_df["Status"] == "external") | (full_df["Status"] != "published")] # filter our published and external results

        return filtered_df

    def idf_sdrf_metadata_scraper(self):

        extracted_metadata = {}

        # edit this list to extract different metadata.
        # NB found some column names contain an erroneous space which is accounted for.
        idf_get = {'Experiment Type': ['Comment[EAExperimentType', 'Comment [EAExperimentType'],
                   'Curator': ['Comment[EACurator]', 'Comment [EACurator]'],
                   'Analysis Type': ['Comment[AEExperimentType]', 'Comment [AEExperimentType]']}
        sdrf_get = {'Single-cell Analysis Type' : ['Comment[library construction]', 'Comment [library construction]'],
                    'Organism' : ['Characteristics[organism]', 'Characteristics [organism]']}
        assert not set(idf_get.keys()).intersection(set(sdrf_get.keys())), 'Keys should be unique in metadata config lists above'


        metadata_get = [{'paths': self.latest_sdrf, 'get_params' : sdrf_get},
                        {'paths': self.latest_idf, 'get_params': idf_get}]

        for metadata_file_type in metadata_get:
            for accession, metadata_file in metadata_file_type.get('paths').items():
                if metadata_file:
                    with open(metadata_file, newline='') as s:
                        reader = csv.DictReader(s, delimiter='\t')
                        for row in reader:
                            for output_colname, input_colname_list in metadata_file_type.get('get_params').items():
                                for input_colname in input_colname_list:
                                    output_value = row.get(input_colname)
                                    if output_value:
                                        break
                                if output_colname not in extracted_metadata:
                                    extracted_metadata[output_colname] = {accession: output_value}
                                else:
                                    extracted_metadata[output_colname].update({accession : output_value})
        return extracted_metadata

    def get_file_modified_date(self):
        mod_time = {}
        for accession, idf_path in self.latest_idf.items():
            if idf_path:
                sdrf_path = self.latest_sdrf.get(accession)
                sdrf_mode_time = os.path.getmtime(sdrf_path)
                idf_mode_time = os.path.getmtime(idf_path)
                mod_time[accession] = datetime.fromtimestamp(max(idf_mode_time, sdrf_mode_time)).isoformat()
        return mod_time



# todo extract eligibility score from db
# def connection_credentials(self, db_name):
#     with open('db_config.json') as f:
#         return SimpleNamespace(**json.load(f).get(db_name))
#
# def substracking_connector(self):
#     host =
#     creds =
#
#     substracking = mysql.connector.connect(
#         host=host,
#         user=creds.user,
#         password=creds.password,
#         port=creds.port
#     )
#     print(substracking)
