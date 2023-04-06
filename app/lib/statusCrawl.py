'''
crawls dir in sources_config to find accessions in system and where they are

The config defines the status (or range of possible statuses) of a given path
The status_type_order must be a linear definition of status order
'''
__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "15/07/2019"

import json
from datetime import datetime
import os
import re
from collections import defaultdict
from tqdm import tqdm
import glob
import requests

class atlas_status:
    def __init__(self, sources_config, status_type_order):

        # configuration
        with open(sources_config) as f:
            self.sources_config = json.load(f)
        self.status_type_order = status_type_order
        self.accession_regex = re.compile('^E-[A-Z]{4}-\d+$')

        # status tracking
        self.status_types = self.get_status_types()

        assert set(self.status_types) == set(
            self.status_type_order), 'Unrecognised status in config. Please update status type order list.'
        self.timestamp = datetime.fromtimestamp(datetime.now().timestamp()).isoformat()
        print('Initialised {}'.format(self.timestamp))

        # accession search
        # scans dir in config to find '*.idf.txt' or accession directories
        try:
            accession_search = self.accession_search() # in case server is down occasionally
        except requests.exceptions.HTTPError:
            raise
        self.all_primary_accessions = accession_search[0]
        self.found_accessions = accession_search[1]

        # determines status of each dataset based on location of files
        self.accession_final_status = self.status_tracker()

        # sets two variables with min and max status based on status_type_order
        get_min_max_status = self.get_min_max_status()
        self.accession_min_status = get_min_max_status[0]
        self.accession_max_status = get_min_max_status[1]

        # finds path to latest idf and sdrf file
        latest_idf_sdrf = self.get_latest_idf_sdrf()
        self.idf_path_by_accession = latest_idf_sdrf[0]
        self.sdrf_path_by_accession = latest_idf_sdrf[1]
        self.path_by_accession = latest_idf_sdrf[2]
        self.analysis_path_by_accession = latest_idf_sdrf[3]

        self.tech = self.get_tech()

    def get_tech(self):
        tech_dict = {}
        for accession, path in self.path_by_accession.items():
            tech = self.sources_config.get(path).get('tech')
            tech_dict[accession] = sorted(tech)
        return tech_dict


    def get_status_types(self):
        status_types = set()
        for x, y in self.sources_config.items():
            for n in y['stage']:
                status_types.add(n)
        return list(status_types)

    def accession_search(self):

        def accession_match(accession, info, path, all_primary_accessions, found_accessions):
            if self.accession_regex.match(accession):
                all_primary_accessions.add(accession)
                found_accessions[(path, accession)] = dict(
                    # config_path=path,
                    accession=accession,
                    tech=info.get('tech', None),
                    stage=info.get('stage', None),
                    resource=info.get('resource', None),
                    source=info.get('source', None)
                )
            return all_primary_accessions, found_accessions

        print('Performing accession search {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        all_primary_accessions = set()
        found_accessions = {}
        counter = 0

        for path, info in self.sources_config.items():
            counter += 1
            if path.startswith('https://'): # web path handling
                print('query url {} {}/{}'.format(path, counter, len(self.sources_config)))
                resp = requests.get(url=path)
                # check the status_code of the query, in case atlas server is down, eg: HTTPError: 500
                assert resp.ok, resp.raise_for_status()

                # data = resp.json().get('aaData')
                data = resp.json().get('experiments')
                for experiment in data:
                    accession = experiment.get('experimentAccession')
                    accession_match(accession, info, path, all_primary_accessions, found_accessions)
                    # todo pass loadDate or lastUpdate date from web to tracker
            else: # nfs dir handling
                print('Searching path {} {}/{}'.format(path, counter, len(self.sources_config)))

                pre_accessions = os.listdir(path)
                for pre_accession in pre_accessions:
                    if not pre_accession.endswith('.merged.idf.txt'):
                        accession = pre_accession.strip('.idf.txt')
                        accession_match(accession, info, path, all_primary_accessions, found_accessions)

        print('Found {} accessions in {} directories'.format(len(found_accessions), len(self.sources_config)))

        return all_primary_accessions, found_accessions

    def status_tracker(self):
        print('Calculating status of each project {}'.format(datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        accession_status = {}
        accession_status_counter = {}
        for key, value in self.found_accessions.items():
            accession = value['accession']
            stage = value['stage']

            index = self.status_type_order.index(stage[-1])
            if accession not in accession_status_counter:
                accession_status_counter[accession] = index
                accession_status[accession] = ' '.join(stage)
            elif index > accession_status_counter.get(accession):
                accession_status_counter[accession] = index
                accession_status[accession] = ' '.join(stage)

        return accession_status

    def get_min_max_status(self):
        # Some paths define multiple statuses. This narrows it to the latter most status according to status_type_order
        accession_min_status = {}
        accession_max_status = {}
        for accession, status in self.accession_final_status.items():
            accession_min_status[accession] = self.status_type_order[min([self.status_type_order.index(x) for x in status.split(' ')])]
            accession_max_status[accession] = self.status_type_order[max([self.status_type_order.index(x) for x in status.split(' ')])]

        return accession_min_status, accession_max_status

    def get_latest_idf_sdrf(self):
        '''
        Does not return idf/sdrf paths for experiments found on https endpoints.
        Latest loc will be latest found on nfs.
        '''


        print('Getting latest IDF and SDRF paths {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))

        def list_converter(file_list):
            files_found = {}
            for filepath in file_list:
                accession = filepath.split('/')[-1].replace('.idf.txt', '').replace('.sdrf.txt', '').replace('-idf.txt', '').replace('-sdrf.txt', '').replace('-analysis-methods.tsv', '')
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
                        continue
                ranked_paths_by_accession[accession] = latest_path
            return ranked_paths_by_accession

        idf_list_ = []
        sdrf_list_ = []

        # analysis files parsed for metadata on how the analysis was done.
        analysis_list = []

        for path, metadata in tqdm(self.sources_config.items(), unit='Paths in config'):
            print('IDF/SDRF path finder exploring {}'.format(path))
            idf_list_ += glob.glob(path + '/*/*idf.txt') + glob.glob(path + '/*idf.txt')
            sdrf_list_ += glob.glob(path + '/*/*sdrf.txt') + glob.glob(path + '/*sdrf.txt')
            analysis_list += glob.glob(path + '/*/*analysis-methods.tsv') + glob.glob(path + '/*-analysis-methods.tsv')

        idf_list = [x for x in idf_list_ if self.accession_regex.match(x.split('/')[-1].replace('.idf.txt', '').replace('.sdrf.txt', '').replace('-idf.txt', '').replace('-sdrf.txt', ''))]
        sdrf_list = [x for x in sdrf_list_ if self.accession_regex.match(x.split('/')[-1].replace('.idf.txt', '').replace('.sdrf.txt', '').replace('-idf.txt', '').replace('-sdrf.txt', ''))]

        ranked_paths = get_ranked_paths()
        idf_path_by_accession = get_latter_ranked_path(list_converter(idf_list), ranked_paths)
        sdrf_path_by_accession = get_latter_ranked_path(list_converter(sdrf_list), ranked_paths)
        analysis_path_by_accession = get_latter_ranked_path(list_converter(analysis_list), ranked_paths)

        # get the path where the accession was initially found at
        paths_by_accession = defaultdict(list)
        for k, v in self.found_accessions.items():
            paths_by_accession[v.get('accession')].append(k[0])
        path_by_accession = get_latter_ranked_path(paths_by_accession, ranked_paths)

        return idf_path_by_accession, sdrf_path_by_accession, path_by_accession, analysis_path_by_accession
