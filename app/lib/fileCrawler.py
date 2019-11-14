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
import sys
import pandas as pd
import collections


class file_crawler:

    def __init__(self, status_crawl, sources_config):

        # configuration
        with open(sources_config) as f:
            self.sources_config = json.load(f)
        self.status = status_crawl

        secondary_accession_mapper = self.secondary_accession_mapper()  # extracts secondary accessions from idf making a map dict and a complete set (all_secondary_accessions) for duplication checks
        self.all_secondary_accessions = secondary_accession_mapper[0]
        self.secondary_accessions_mapping = secondary_accession_mapper[1]

        # idf_sdrf_metadata_scrape = self.idf_sdrf_metadata_scraper()
        # self.extracted_metadata = idf_sdrf_metadata_scrape[0]
        # self.unicode_error_accessions = idf_sdrf_metadata_scrape[1]
        # self.unicode_error_paths = idf_sdrf_metadata_scrape[2]

        self.unicode_error_paths = []
        self.emptyfile_error_paths = []
        self.extracted_metadata = self.idf_sdrf_metadata_scraper()

        self.curators_by_acession = self.lookup_curator_file()
        self.mod_time = self.get_file_modified_date()

        '''
        Single cell vs. bulk GET from conf file loc
        Analysis type: Baseline, Differential, Trajectory HALF DONE sc Analysis Type need to combine this with pickup loc for bulk.
        Platform type: Microarray vs. Sequencing (could be combined with sc vs. bulk) idf experiment type.
        Title DONE
        Species DONE 'Organism'
        For single-cell experiments: Library construction type (Smart-seq, 10x, etc) We need this for stats DONE 'Single-cell Experiment Type'
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
            with open(idf_file, "r") as f:
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
        '''
        Includes non utf-8 handling: strips unknown characters often in pup title
        Fast method: reads metadata approximately (return first find) for speed improvements
        Strategy assumes search is slowest aspect.
        10x faster than pandas read methods.
        10x faster than string match methods.
        '''

        def file_reader(filename):
            try:
                with open(filename, mode='r', newline='') as s:  # strict text handling
                    fileContent = [x.rstrip().split('\t') for x in list(s)]
            except UnicodeDecodeError:
                self.unicode_error_paths.append(filename)
                with open(filename, mode='rb') as s:  # strip non utf-8
                    fileContent = [x.decode('utf-8', 'ignore').rstrip().split('\t') for x in list(s)]

            if len(fileContent) <= 1:  # defend against empty files
                self.emptyfile_error_paths.append(filename)
                return None
            else:
                return fileContent

        def idf_extract():
            query = {'Experiment Type': re.compile(r'Comment\[EAExperimentType\]|Comment \[EAExperimentType\]'),
                            'Curator': re.compile(r'Comment\[EACurator\]|Comment \[EACurator\]'),
                            'Analysis Type': re.compile(r'Comment\[AEExperimentType\]|Comment \[AEExperimentType\]'),
                            'Investigation Title': re.compile(r'Investigation Title')
                            }

            extracted_metadata = collections.defaultdict(dict)

            for accession, filename in tqdm(self.status.idf_path_by_accession.items(), unit='idf files'):
                fileContent = file_reader(filename)
                for output_key, p in query.items():
                    if fileContent:
                        for line in fileContent:
                            if re.match(p, line[0]):
                                v = line[1]
                                extracted_metadata[output_key].update({accession: v})
                                break
            return extracted_metadata

        def sdrf_extract():

            query = {
                'Single-cell Experiment Type': re.compile(r'Comment\[library construction\]|Comment \[library construction\]'),
                'Organism': re.compile(r'Characteristics\[organism\]|Characteristics \[organism\]|Characteristics \[Organism\]')
                }

            extracted_metadata = collections.defaultdict(dict)

            for accession, filename in tqdm(self.status.sdrf_path_by_accession.items(), unit='sdrf files'):
                fileContent = file_reader(filename)
                if fileContent:
                    for output_key, p in query.items():
                        hits = [ind for ind, x in enumerate(fileContent[0]) if re.match(p, x)]
                        if hits:
                            v = ' & '.join([fileContent[1][x] for x in hits]) # extract value from 1st row only
                            extracted_metadata[output_key].update({accession: v})


            return extracted_metadata

        return idf_extract().update(sdrf_extract())

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
