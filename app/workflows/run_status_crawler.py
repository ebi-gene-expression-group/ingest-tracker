'''
Use following params for DEV run
-n
"DEV Ingest Status"
-s
/app/etc/local_test_sources_config.json
-d
/app/etc/db_config.json
-g
/app/etc/client_secret.json
'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "24/07/2019"

import argparse
from app.lib import trackerBuild

def parameters():
    parser = argparse.ArgumentParser(description='Load an external dataset into Atlas.')
    parser.add_argument("-s", "--sources_config", dest="sources_config",
                        help="Configuration file with paths. Private doc available locally.",
                        required=True)
    parser.add_argument("-d", "--db_config", dest="db_config",
                        help="Configuration file with db connection settings. Private doc available locally.",
                        required=True)
    parser.add_argument("-g", "--google_client_secret", dest="google_client_secret",
                        help="Connection info for output Google Sheet. Private doc available locally.",
                        required=True)
    parser.add_argument("-o", "--google_output", dest="google_output",
                        help="Connection info for output Google Sheet. Private doc available locally.",
                        required=False, default=True)
    parser.add_argument("-n", "--sheetname", dest="sheetname",
                        help="Name of output sheet.",
                        required=False, default='DEV Ingest Status')
    return parser.parse_args()


if __name__ == '__main__':

    args = parameters()
    trackerBuild.tracker_build(args.sources_config, args.db_config, args.google_client_secret, args.google_output, args.sheetname)