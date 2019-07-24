import status_crawler
from pathlib import Path
import subprocess

atlas_status = status_crawler.atlas_status()

def duplication_check(args):
    def external_duplication_check(external_accession):  # check if we have already seen this secondary accession in all idfs we have in the source config locations

        if external_accession in atlas_status.all_secondary_accessions:
            for internal_accession, external_accession_list in atlas_status.secondary_accessions_mapping.items():
                if external_accession in external_accession_list:
                    return internal_accession
                else:
                    raise Exception(
                        'This external accession ({}) is already being ingested but conversion to an internal accession was not possible'.format(
                            external_accession))
        else:
            return False

    def internal_duplication_check(external_accession):
        return external_accession in atlas_status.accession_status_table.keys()

    for external_accession in args.external_accession:
        external_dup_check = external_duplication_check(external_accession)
        if external_dup_check:
            raise ValueError('External accession {} has already been ingested into atlas. See internal accession {}.'.format(
                external_accession, external_dup_check))

        internal_dup_check = internal_duplication_check(external_accession)
        if internal_dup_check:
            raise ValueError(
                'Accession {} has already been ingested into atlas. See internal accession {}.'.format(
                    external_accession, external_accession))

def get_temp_mage_tab(args):
    '''
    Should handle getting and converting MAGE-TAB from any supported source (currently only does ae).
    Writes file in temp location.
    Returns path to newly created file.
    If file already exists (as it does for ae experiments it just returns the path to the file.
    '''

    def get_ae_metadata_files(external_accession):  # files already exist so they don't need to be converted so this function returns the path to the file

        ae_import_source = []
        for path, metadata in atlas_status.sources_config.items():
            if 'ae' in metadata['source'] and 'external' in metadata['stage']:
                ae_import_source.append(path)
        assert len(ae_import_source) == 1, 'Multiple path sources for ae ingest not expected.'

        def search_file(pattern, external_accession):
            search = []
            for filename in Path(ae_import_source[0]).glob('**/{}{}'.format(external_accession, pattern)):
                search.append(str(filename))
            assert len(search) == 1, 'Found {} files matching pattern {} for accession {}. Expected 1.'.format(len(search), pattern, external_accession)
            # return searchr[0]

        patterns = {'idf': '.idf.txt', 'sdrf': '.sdrf.txt'}
        metadata_files = {}
        for name, pattern in patterns.items():
            metadata_files[name] = search_file(pattern, external_accession)
        return metadata_files

    temp_mage_tab = {}

    for external_accession in args.external_accession:
        if args.external_source == 'ae':
            temp_mage_tab[external_accession] = get_ae_metadata_files(external_accession)
        else:
            raise ValueError('Source type {} is not supported at this time.'.format(args.external_source))
            # todo add support for other external sources including MAGE-TAB converters

    return temp_mage_tab


def general_MAGETAB_validation(temp_mage_tab): # only works in fg_atlas env

    params = ["perl", "validate_magetab.pl",
              "-i", path_to_idf]

    pipe = subprocess.Popen(params, stdin=subprocess.PIPE)
    pipe.stdin.close()



# todo add validator func
'''
validator we are currently using: https://github.com/ebi-gene-expression-group/fgsubs/blob/master/perl/validate_magetab.pl
It runs these three checksets on top of structural MAGE-TAB format validation: https://github.com/ebi-gene-expression-group/fgsubs/tree/master/perl/modules/EBI/FGPT/CheckSet
I think this currently doesn't run as fg_atlas because it has crazy perl dependencies. But I think we could set this up and use for now.
'''

