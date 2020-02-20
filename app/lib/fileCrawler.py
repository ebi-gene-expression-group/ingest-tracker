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
import os
import json
import collections
import numpy as np
import sys


class file_crawler:

    def __init__(self, status_crawl, sources_config):

        # configuration
        with open(sources_config) as f:
            self.sources_config = json.load(f)
        self.status = status_crawl

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
            # extracts entire row as a list

            print('\nExtracting metadata from idf files...\n')
            query = {'Experiment Type': re.compile(r'Comment\[EAExperimentType\]|Comment \[EAExperimentType\]'),
                            'Curator': re.compile(r'Comment\[EACurator\]|Comment \[EACurator\]'),
                            'Analysis Type': re.compile(r'Comment\[AEExperimentType\]|Comment \[AEExperimentType\]'),
                            'Investigation Title': re.compile(r'Investigation Title'),
                            'Secondary Accession': re.compile(r'Comment \[SecondaryAccession\]|Comment\[SecondaryAccession\]')
                            }

            extracted_metadata = collections.defaultdict(dict)

            for accession, filename in tqdm(self.status.idf_path_by_accession.items(), unit='idf files'):
                fileContent = file_reader(filename)
                for output_key, p in query.items():
                    if fileContent:
                        for line in fileContent:
                            if re.match(p, line[0]):
                                try:
                                    # take multiple rows if they are present.
                                    result = line[1:]
                                    if len(result) == 0:
                                        v = np.nan
                                    else:
                                        v = result
                                except IndexError:
                                    continue
                                extracted_metadata[output_key].update({accession: v})
                                break
            return extracted_metadata

        def sdrf_extract():
            # extracts 1st value form 1st row only

            print('\nExtracting metadata from sdrf files...\n')
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


        def analysis_extract():
            print('\nExtracting metadata from analysis-methods files...\n')

            # pattern 1 extracts row based on first column
            # pattern 2 parses value from pattern 1 match
            query = {
                'GeneQuant': (re.compile(r'Gene Quantification|Quantification'),
                                  {'GeneQuantSoft': re.compile('^(.*)(?= version)'),
                                   'GQSVersion': re.compile('(?<=version: )(.*)$')
                                   }),
                'TransQuant': (re.compile(r'Transcript Quantification'),
                                  {'TransQuantSoft': re.compile('^(.*)(?= version)'),
                                   'TQSVersion': re.compile('(?<=version: )(.*)$')
                                   }),
                'Mapping': (re.compile(r'Read Mapping'),
                                    {'MappingSoft': re.compile('\) (.*) version'),
                                    'MappingSoftVersion': re.compile('(?<=\) )(.*)(?= version)'),
                                    'E!Version': re.compile('(?<=Ensembl Genomes release: )(.*)(?=\))')
                                    })
                }

            extracted_metadata = collections.defaultdict(dict)

            for accession, filename in tqdm(self.status.analysis_path_by_accession.items(), unit='analysis files'):
                fileContent = file_reader(filename)
                for key, p in query.items():
                    if fileContent:
                        for line in fileContent:
                            if re.match(p[0], line[0]):
                                try:
                                    v_str = line[1]
                                except IndexError:
                                    continue

                                for output_key, pat in p[1].items():
                                    v_search = pat.search(v_str)
                                    if v_search:
                                        v = v_search.group(1)
                                        extracted_metadata[output_key].update({accession: v})
            return extracted_metadata


        def merge_defaultdicts(d, d1):
            for k, v in d1.items():
                if (k in d):
                    d[k].update(d1[k])
                else:
                    d[k] = d1[k]
            return d

        extracted_analysis_metadata = analysis_extract()
        extracted_idf_metadata = idf_extract()
        extracted_sdrf_metadata = sdrf_extract()
        extracted_metadata = merge_defaultdicts(merge_defaultdicts(extracted_idf_metadata, extracted_sdrf_metadata), extracted_analysis_metadata)

        return extracted_metadata

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
