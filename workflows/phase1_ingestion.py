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
import ingest_tools

def parameters():
    SUPPORTED_SOURCES = ['ae', 'geo', 'hca', 'ena', 'pride']
    parser = argparse.ArgumentParser(description='Load an external dataset into Atlas.')
    parser.add_argument("-a", "--external_accession", dest="external_accession",
                        help="The external accession of the experiment. Could be from GEO/HCA/ENA/PRIDE etc",
                        required=True, nargs=1)  # todo add nargs='+' when lists of accessions can be added
    parser.add_argument("-s", "--external_source", dest="external_source",
                        help="Should be one of the supported sources ({})".format(str(SUPPORTED_SOURCES).strip("[]")),
                        required=True, choices=SUPPORTED_SOURCES)
    parser.add_argument("-i", "--ignore_duplication", dest="ignore_duplication",
                        help="Pass flag to ignore duplication errors.",
                        required=False, action='store_true')
    return parser.parse_args()


if __name__ == '__main__':

    args = parameters()
    print('ingesting')

    if not args.ignore_duplication:
        ingest_tools.duplication_check(args)
    else:
        print('Skipping duplication checks')

    temp_mage_tab = ingest_tools.get_temp_mage_tab(args)

    ingest_tools.general_MAGETAB_validation(temp_mage_tab) #only works as fg_atlas







