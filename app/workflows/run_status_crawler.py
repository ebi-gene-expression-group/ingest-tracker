__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "24/07/2019"

import argparse
import status_crawler

def parameters():
    parser = argparse.ArgumentParser(description='Load an external dataset into Atlas.')
    parser.add_argument("-s", "--sources_config", dest="sources_config",
                        help="Configuration file with paths. Private doc available locally.",
                        required=True)
    parser.add_argument("-g", "--google_client_secret", dest="google_client_secret",
                        help="Connection info for output Google Sheet. Private doc available locally.",
                        required=True)
    parser.add_argument("-o", "--google_output", dest="google_output",
                        help="Connection info for output Google Sheet. Private doc available locally.",
                        required=False, default=True)
    return parser.parse_args()


if __name__ == '__main__':

    args = parameters()
    status_crawler.atlas_status(args.sources_config, args.google_client_secret, args.google_output)