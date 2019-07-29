
'''
Quick pickle reader writer. Takes any previous log output '<timestamp>.atlas_status.log' and writes it to the shared Google shpreadsheet.
This is useful to restore the sheet to a previous version.
Designed to be ran as a one off to restore.

'https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit?usp=drive_web&ouid=115288485985716622034'

'''

__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "29/07/2019"

import argparse
import pickle

def parameters():
    parser = argparse.ArgumentParser(description='Load a previously ran atlas status log into the shared google sheet.')
    parser.add_argument("-s", "--sheet", dest="sheet",
                        help="Path to pickle log output e.g. 'logs/2019-07-29T08:19:06.148654.atlas_status.log",
                        required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parameters()
    with open(args.sheet, 'rb') as f:
        atlas_status = pickle.load(f)
    output_df = atlas_status.df_compiler()
    atlas_status.google_sheet_output(output_df)