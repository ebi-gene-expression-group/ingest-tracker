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
from tqdm import tqdm
import pickle
import time

import sys

# # set working dir to path of this script.
# abspath = os.path.abspath(__file__)
# dname = os.path.dirname(abspath)
# os.chdir(dname)
# os.chdir('../../') # LOCAL TEST ONLY!!!

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('{}  {} ms'.format(method.__name__, (te - ts) * 1000))
        return result
    return timed

class atlas_status:
    @timeit
    def __init__(self, sources_config, google_client_secret, google_output=True):

        # configuration
        self.google_client_secret = google_client_secret
        with open(sources_config) as f:
            self.sources_config = json.load(f)
        self.status_types = self.get_status_types()
        self.status_type_order = ['external', 'incoming', 'loading', 'analysing', 'processed', 'published']
        assert set(self.status_types) == set(
            self.status_type_order), 'Unrecognised status in config. Please update status type order list.'
        self.timestamp = datetime.fromtimestamp(datetime.now().timestamp()).isoformat()
        print('Initialised {}'.format(self.timestamp))

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

        self.pickle_out()

    @timeit
    def get_status_types(self):
        status_types = set()
        for x, y in self.sources_config.items():
            for n in y['stage']:
                status_types.add(n)
        return list(status_types)

    @timeit
    def accession_search(self):
        print('Performing accession search {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        found_accessions = {}
        counter = 0
        for path, info in self.sources_config.items():
            counter += 1
            print('Searching path {} {}/{}'.format(path, counter, len(self.sources_config)))

            pre_accessions = os.listdir(path)
            self.accession_regex = re.compile('^E-(GEOD|MTAB|PROT|ENAD|AFMX|CURD|EHCA|MEXP|TABM|NASC|ERAD|GEUV|JJRD|ATMX|HCAD|MIMR|CBIL|MAXD)-[0-9]+$')
            for pre_accession in pre_accessions:
                if not pre_accession.endswith('.merged.idf.txt'):
                    accession = pre_accession.strip('.idf.txt')
                    if self.accession_regex.match(accession):
                        found_accessions[(path, accession)] = dict(
                                                                        # config_path=path,
                                                                        accession=accession,
                                                                        tech=info.get('tech', None),
                                                                        stage=info.get('stage', None),
                                                                        resource=info.get('resource', None),
                                                                        source=info.get('source', None)
                        )
        print('Found {} accessions in {} directories'.format(len(found_accessions), len(self.sources_config)))
        return found_accessions

    @timeit
    def status_tracker(self):
        print('Calculating status of each project {}'.format(datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
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

        for accession, status_bool in accession_status.items():
            for status_type in reversed(self.status_type_order):
                if status_bool.get(status_type):
                    accession_final_status[accession] = status_type
                    break
        # self.status_totals = pd.DataFrame.from_dict(self.accession_final_status, orient='index')[0].value_counts()
        return accession_final_status

    @timeit
    def secondary_accession_mapper(self):
        print('Finding secondary accessions {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        idf_files = []
        for path, values in tqdm(self.sources_config.items()):
            print('Secondary accession search in {}'.format(path))
            f = [f for f in glob.glob(path + "**/*.idf.txt", recursive=True)]
            idf_files += f

        self.all_secondary_accessions = set()
        self.secondary_accessions_mapping = {}
        for idf_file in tqdm(idf_files):
            accession = idf_file.split('/')[-1].strip('.idf.txt')
            with open (idf_file, "r") as f:
                try:
                    contents = f.read()
                except UnicodeDecodeError:
                    print('Cannot open {}'.format(idf_file))
                search = re.findall(r'Comment \[SecondaryAccession\](.*?)\n', contents)
                if search:
                    secondary_accessions = [x.strip('\t') for x in search]
                    if len(secondary_accessions) == 1 and len(secondary_accessions[0]) == 0:
                        continue
                    else:
                        self.secondary_accessions_mapping[accession] = secondary_accessions
                        self.all_secondary_accessions.update(secondary_accessions)

    @timeit
    def get_latest_idf_sdrf(self):
        print('Getting latest IDF and SDRF paths {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))

        def list_converter(file_list):
            files_found = {}
            for filepath in file_list:
                accession = filepath.split('/')[-1].replace('.idf.txt', '').replace('.sdrf.txt', '')
                if accession in files_found:
                    files_found[accession].append(filepath)
                else:
                    files_found[accession] = [filepath]
            return files_found

        def get_ranked_paths():
            # ranks path by status type, used to estimate out where latest metadata is.
            preordered_paths = []
            latest_stage_ranks = []
            for path, metadata in self.sources_config.items():
                preordered_paths.append(path)
                latest_stage_ranks.append(max([self.status_type_order.index(n) for n in metadata.get('stage')]))
            return [x for _,x in sorted(zip(latest_stage_ranks,preordered_paths))]

        def get_latter_ranked_path(paths_by_accession, ranked_paths):
            ranked_paths_by_accession = {}
            for accession, path_list in paths_by_accession.items():
                if len(path_list) == 1:
                    latest_path = path_list[0]
                else:
                    try:
                        trunc_paths = [v.split('/E-')[0] for v in path_list]  # remove accession specific endings
                        path_ranks = [ranked_paths.index(n) for n in trunc_paths]
                        latest_path = path_list[path_ranks.index(max(path_ranks))]  # get idf/sdrf from latter loc
                    except ValueError:
                        continue #todo this needs more investigation in prod got ValueError: '/nfs/production3/ma/home/atlas3-production/singlecell/experiment/ng' is not in list
                ranked_paths_by_accession[accession] = latest_path
            return ranked_paths_by_accession



        idf_list_ = []
        sdrf_list_ = []
        for path, metadata in tqdm(self.sources_config.items()):
            print('IDF/SDRF path finder exploring {}'.format(path))
            idf_list_ += glob.glob(path + '/*/*.idf.txt') + glob.glob(path + '/*.idf.txt')
            sdrf_list_ += glob.glob(path + '/*/*.sdrf.txt') + glob.glob(path + '/*.sdrf.txt')

        idf_list = [x for x in idf_list_ if self.accession_regex.match(x.split('/')[-1].replace('.idf.txt', '').replace('.sdrf.txt', ''))]
        sdrf_list = [x for x in sdrf_list_ if self.accession_regex.match(x.split('/')[-1].replace('.idf.txt', '').replace('.sdrf.txt', ''))]

        ranked_paths = get_ranked_paths()
        self.idf_path_by_accession = get_latter_ranked_path(list_converter(idf_list), ranked_paths)
        self.sdrf_path_by_accession = get_latter_ranked_path(list_converter(sdrf_list), ranked_paths)

    @timeit
    def idf_sdrf_metadata_scraper(self):
        print('Scraping project metadata {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        extracted_metadata = {}

        # edit this list to extract different metadata.
        # NB found some column names contain an erroneous space which is accounted for.
        idf_get = {'Experiment Type': ['Comment[EAExperimentType', 'Comment [EAExperimentType'],
                   'Curator': ['Comment[EACurator]', 'Comment [EACurator]'],
                   'Analysis Type': ['Comment[AEExperimentType]', 'Comment [AEExperimentType]']}
        sdrf_get = {'Single-cell Experiment Type' : ['Comment[library construction]', 'Comment [library construction]'],
                    'Organism' : ['Characteristics[organism]', 'Characteristics [organism]', 'Characteristics [Organism]']}
        assert not set(idf_get.keys()).intersection(set(sdrf_get.keys())), 'Keys should be unique in metadata config lists above'


        metadata_get = [{'paths': self.sdrf_path_by_accession, 'get_params' : sdrf_get},
                        {'paths': self.idf_path_by_accession, 'get_params': idf_get}]

        for metadata_file_type in metadata_get:
            for accession, metadata_file in metadata_file_type.get('paths').items():
                if metadata_file:
                    try:
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
                    except UnicodeDecodeError:
                        print('Failed to open {} due to UnicodeDecodeError'.format(metadata_file))
                        # todo fix unit decode error affecting some files. They tend to be charset=unknown-8bit

        return extracted_metadata

    @timeit
    def get_file_modified_date(self):
        print("Getting datestamp of project's last modification {}".format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        mod_time = {}
        for accession, idf_path in self.idf_path_by_accession.items():
            sdrf_path = self.sdrf_path_by_accession.get(accession)
            if idf_path and sdrf_path:
                idf_mode_time = os.path.getmtime(idf_path)
                sdrf_mode_time = os.path.getmtime(sdrf_path)
                mod_time[accession] = datetime.fromtimestamp(max(idf_mode_time, sdrf_mode_time)).isoformat()
            elif idf_path:
                idf_mode_time = os.path.getmtime(idf_path)
                mod_time[accession] = datetime.fromtimestamp(idf_mode_time).isoformat()
            elif sdrf_path:
                sdrf_mode_time = os.path.getmtime(sdrf_path)
                mod_time[accession] = datetime.fromtimestamp(sdrf_mode_time).isoformat()
        return mod_time

    @timeit
    def df_compiler(self):
        print('Combining results into summary dataframe {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        # combine accession keyed dictionaries
        # NB extracted metadata is an extra dict of dicts with various values from metadata scraping
        input_dicts = {"Status": self.accession_final_status,
                       "Secondary Accessions": self.secondary_accessions_mapping,
                       "IDF": self.idf_path_by_accession,
                       "SDRF": self.sdrf_path_by_accession,
                       "Last Modified": self.mod_time
                       }
        input_dicts.update(self.extracted_metadata)
        input_data = {}
        for colname, input_dict in input_dicts.items():
            for accession, value in input_dict.items():
                if accession not in input_data:
                    input_data[accession] = {colname: value}
                else:
                    input_data[accession].update({colname: value})

        # df parsing
        full_df = pd.DataFrame.from_dict(input_data, orient='index')
        filtered_df = full_df[(full_df["Status"] == "external") | (
                    full_df["Status"] != "published")]  # filter our published and external results

        return filtered_df

    @timeit
    def google_sheet_output(self, output_df):
        print('Outputting to google sheet {} {}'.format(str(output_df.shape),
                                                        datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.google_client_secret, scope)
        client = gspread.authorize(creds)
        sheet = client.open("Ingest Status")  # this is the spreadsheet not the worksheet

        # add new empty worksheet
        sheet.add_worksheet(title="In progress {}".format(self.timestamp), rows=output_df.shape[0] + 1,
                            cols=output_df.shape[1] + 1)
        # fill new empty worksheet
        gspread_dataframe.set_with_dataframe(sheet.get_worksheet(1), output_df, include_index=True,
                                             include_column_header=True)
        # remove old worksheet in pos 0
        sheet.del_worksheet(sheet.get_worksheet(0))

    @timeit
    def pickle_out(self):
        if not os.path.exists('logs'):
            os.makedirs('logs')
        filename = 'logs/' + str(self.timestamp) + '.atlas_status.log'
        filehandler = open(filename, 'wb')
        pickle.dump(self, filehandler)






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


# todo SPEED Slowest step confirmed to be IDF/SDRF path finder exploring /nfs/production3/ma/home/arrayexpress/ae2_production/data/EXPERIMENT/MTAB
# todo SPEED try replacing glob.glob with os.walk. The very broad */*.idf.txt searches are too open ended and take ages.
# todo SPEED improvements merge secondary accession and other metadata get operations so files are only opened once
# todo SPEED write out object so it can only scrape files that have not been modified
# todo SPEED accessioner only needs accessions, if still too slow I can separate this function