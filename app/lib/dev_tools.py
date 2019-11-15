'''
IN DEV FUNC generates new internal atlas accessions for a given datatype/source
'''
__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "29/07/2019"

import re
import os
from datetime import datetime
import pickle
from app.lib import statusCrawl

def atlas_status_from_last_save():
    log_path = '../workflows/logs/'
    file_ending = '.atlas_status.log'
    logs = os.listdir(log_path)
    timestamps = []
    for log in logs:
        if log.endswith(file_ending):
            timestamps.append(datetime.fromisoformat(log.rstrip(file_ending)).timestamp())
    latest_pickle = str(datetime.fromtimestamp(max(timestamps)).isoformat()) + file_ending
    with open(log_path + latest_pickle, 'rb') as f:
        return pickle.load(f)

def accessioner(prefix, sources_config=False, secondary_accession=False):

    def counter_method(prefix, sources_config):
        current_accession = 0
        atlas_status = statusCrawl.atlas_status(sources_config, crawl=False) # partial fast crawl just for accessions
        for accession in atlas_status.all_primary_accessions:
            numeric_part = accession.split('-')[-1]
            prefix_part = accession.split('-')[-2]
            assert numeric_part.isdigit(), 'Accession parsing error parsing error'
            if prefix_part == prefix and int(numeric_part) > current_accession:
                current_accession = int(numeric_part)
            new_accession = str(current_accession + 1)
        return str('E-' + prefix + '-' + new_accession)

    def GEO_method(secondary_accession):
        'GEO series records are handles with bespoke conversion (GSExxx)'
        GSE_regex = re.compile('^GSE[0-9]+$')
        assert GSE_regex.match(secondary_accession), 'Invalid GEO series accession. Should be of format GSExxx'
        numberic_part = secondary_accession.lstrip('GSE')
        return 'E-GEOD-' + str(numberic_part)


    supported_prefix_for_accessioning = {'GEOD': 'GEO_method',
                                         'MTAB': 'no_conversion',
                                         'PROT': 'counter_method',
                                         'ENAD': 'counter_method',
                                         'CURD': 'counter_method',
                                         'EHCA': 'counter_method'}

    if supported_prefix_for_accessioning.get(prefix) == 'counter_method':
        assert sources_config, 'You must provide "sources_config" argument for this prefix type'
        accession = counter_method(prefix, sources_config)

    elif supported_prefix_for_accessioning.get(prefix) == 'GEO_method':
        assert secondary_accession, 'You must provide "secondary_accession" argument for this prefix type'
        accession = GEO_method(secondary_accession)

    elif supported_prefix_for_accessioning.get(prefix) == 'no_conversion':
        assert secondary_accession, 'You must provide "secondary_accession" argument for this prefix type'
        assert secondary_accession.split('-')[-2] == 'MTAB', 'Exprected "MTAB" type secondary_accession e.g. E-MTAB-00000'
        accession = secondary_accession

    else:
        raise ValueError('The prefix {} is not supported by the accessioner'.format(prefix))

    return accession

def external_duplication_check(external_accession):
    # fails duplication check if internal accession is returned
    # passes check if False is returned
    atlas_status = atlas_status_from_last_save()


    if type(external_accession) == str:
        external_accession = list(external_accession)

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

    for external_accession in external_accession:
        external_dup_check = external_duplication_check(external_accession)
        if external_dup_check:
            raise ValueError(
                'External accession {} has already been ingested into atlas. See internal accession {}.'.format(
                    external_accession, external_dup_check))

def internal_duplication_check(accessions):
    if type(accessions) == str:
        external_accession = list(accessions)
    atlas_status = atlas_status_from_last_save()
    for accession in accessions:
        if accession in atlas_status.all_primary_accessions:
            raise ValueError(
                'Internal accession {} has already been ingested into atlas.'.format(accession))

def get_ae_metadata_files(external_accession, sources_config):
    if type(external_accession) == str:
        external_accession = list(external_accession)

    # atlas_status = status_crawler.atlas_status(sources_config, crawl=False)
    atlas_status = atlas_status_from_last_save()
    idf_path_by_accession = {}
    sdrf_path_by_accession = {}
    for accession in external_accession:
        idf_path = atlas_status.idf_path_by_accession.get(accession, False)
        sdrf_path = atlas_status.sdrf_path_by_accession.get(accession, False)
        assert idf_path != None, 'IDF file could not be found for accession {}. This dataset cannot be imported.'.format(accession)
        assert sdrf_path != None, 'SDRF file could not be found for accession {}. This dataset cannot be imported.'.format(accession)
        idf_path_by_accession[accession] = idf_path
        sdrf_path_by_accession[accession] = sdrf_path
    return {'idf paths': idf_path_by_accession, 'sdrf paths':sdrf_path_by_accession}
