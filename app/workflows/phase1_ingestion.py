'''
This script is the first of three that can be triggered by a curator to ingest a dataset into Atlas.

Given an external accession it performs several tasks:

1. Looks at secondary accessions already in the system to help prevent duplication. At the moment this cannot be overidden.
2. Check that provided accession isn't an internal accession already used in the system. Useful when internal accessions are accidentally provided.


'''
__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "16/07/2019"

import argparse
from app.lib import ingest_tools

def parameters():
    SUPPORTED_SOURCES = ['ae', 'geo', 'hca', 'ena', 'pride']
    parser = argparse.ArgumentParser(description='Load an external dataset into Atlas.')
    parser.add_argument("-a", "--external_accession", dest="external_accession",
                        help="The external accession of the experiment. Could be from GEO/HCA/ENA/PRIDE etc",
                        required=True, nargs=1)  # todo add nargs='+' when lists of accessions can be added. Dup check func can handle lists already
    parser.add_argument("-x", "--external_source", dest="external_source",
                        help="Should be one of the supported sources ({})".format(str(SUPPORTED_SOURCES).strip("[]")),
                        required=True, choices=SUPPORTED_SOURCES)
    parser.add_argument("-i", "--ignore_duplication", dest="ignore_duplication",
                        help="Pass flag to ignore duplication errors.",
                        required=False, action='store_true')
    parser.add_argument("-s", "--sources_config", dest="sources_config",
                        help="Configuration file with paths. Private doc available locally.",
                        required=True)
    return parser.parse_args()


if __name__ == '__main__':

    args = parameters()

    # DUPLICATION CHECKS
    if not args.ignore_duplication:
        ingest_tools.external_duplication_check(args.external_accession)
        ingest_tools.internal_duplication_check(args.external_accession)
    else:
        print('Skipping duplication checks')

    # GET MAGE TAB
    if args.external_source == 'ae':
        metadata_files = ingest_tools.get_ae_metadata_files(args.external_accession, args.sources_config)
    else:
        metadata_files = None
        print('This source type is in development') #todo add support for more source types

    print(metadata_files)


    # todo add general MAGETAB validation here!







