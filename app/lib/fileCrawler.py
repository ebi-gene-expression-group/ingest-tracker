'''
collection of functions that open files on nfs to extract tracking metadata
'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "08/11/2019"

import glob
from datetime import datetime
from tqdm import tqdm
import re
import csv
import os
import json

class file_crawler:

    def __init__(self, status_crawl, sources_config):

        # configuration
        with open(sources_config) as f:
            self.sources_config = json.load(f)
        self.status = status_crawl

        secondary_accession_mapper = self.secondary_accession_mapper()  # extracts secondary accessions from idf making a map dict and a complete set (all_secondary_accessions) for duplication checks
        self.all_secondary_accessions = secondary_accession_mapper[0]
        self.secondary_accessions_mapping = secondary_accession_mapper[1]

        idf_sdrf_metadata_scrape = self.idf_sdrf_metadata_scraper()
        self.extracted_metadata = idf_sdrf_metadata_scrape[0]
        self.unicode_error_accessions = idf_sdrf_metadata_scrape[1]
        self.unicode_error_paths = idf_sdrf_metadata_scrape[2]

        self.curators_by_acession = self.lookup_curator_file()
        self.mod_time = self.get_file_modified_date()

        '''
        Single cell vs. bulk
        Analysis type: Baseline, Differential, Trajectory
        Platform type: Microarray vs. Sequencing (could be combined with sc vs. bulk)
        Title
        Species
        For single-cell experiments: Library construction type (Smart-seq, 10x, etc) We need this for stats
        '''

    def secondary_accession_mapper(self):
        print('Finding secondary accessions {}'.format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        idf_files = []
        for path, values in tqdm(self.status.sources_config.items(), unit='Paths in config'):
            print('Secondary accession search in {}'.format(path))
            f = [f for f in glob.glob(path + "**/*.idf.txt", recursive=True)]
            idf_files += f

        all_secondary_accessions = set()
        secondary_accessions_mapping = {}
        for idf_file in tqdm(idf_files, unit='IDF files'):
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
                        secondary_accessions_mapping[accession] = secondary_accessions
                        all_secondary_accessions.update(secondary_accessions)
        return all_secondary_accessions, secondary_accessions_mapping

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

        metadata_get = [{'paths': self.status.sdrf_path_by_accession, 'get_params' : sdrf_get},
                        {'paths': self.status.idf_path_by_accession, 'get_params': idf_get}]

        unicode_error_accessions = []
        unicode_error_paths = []
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
                                        if output_value and output_value != None:
                                            break
                                    if output_colname not in extracted_metadata and output_value != None:
                                        extracted_metadata[output_colname] = {accession: output_value}
                                    elif output_value != None:
                                        extracted_metadata[output_colname].update({accession : output_value})
                    except UnicodeDecodeError:
                        unicode_error_accessions.append(accession)
                        unicode_error_paths.append(metadata_file)
                        print('Failed to open {} due to UnicodeDecodeError'.format(metadata_file))
                        # todo fix unit decode error affecting some files. They tend to be charset=unknown-8bit
                        continue
        return extracted_metadata, unicode_error_accessions, unicode_error_paths

    def lookup_curator_file(self):
        curator_signature = {}
        for path, info in self.sources_config.items():
            for file in glob.glob(path + '/E-*/.curator.*'):
                curator = file.split('.')[-1]
                accession = file.split('/')[-2]
                curator_signature[accession] = curator
        return curator_signature

    def get_file_modified_date(self):
        print("Getting datestamp of project's last modification {}".format(
            datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
        mod_time = {}
        for accession, idf_path in self.status.idf_path_by_accession.items():
            sdrf_path = self.status.sdrf_path_by_accession.get(accession)
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
