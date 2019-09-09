'''
writes out and formats summary dataframe to google sheets using google sheet API

Output goes here https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit#gid=736620111
'''
__author__ = "hewgreen"
__license__ = "Apache 2.0"
__date__ = "01/08/2019"

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe
from datetime import datetime
from googleapiclient import discovery


def post_sheet_formatting(credentials, spreadsheet_id, sheetId):
    requests = []

    # Make header row stand out
    requests.append({
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startRowIndex": 0,
          "endRowIndex": 1
        },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "red": 0.0,
              "green": 0.0,
              "blue": 0.0
            },
            "horizontalAlignment" : "CENTER",
            "textFormat": {
              "foregroundColor": {
                "red": 1.0,
                "green": 1.0,
                "blue": 1.0
              },
              "fontSize": 12,
              "bold": True
            }
          }
        },
        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
      }
    })

    # add frozen top row
    requests.append({
      "updateSheetProperties": {
        "properties": {
          "sheetId": sheetId,
          "gridProperties": {
            "frozenRowCount": 1
          }
        },
        "fields": "gridProperties.frozenRowCount"
      }
    })

    # resize columns widths

    requests.append({
      "autoResizeDimensions": {
        "dimensions": {
          "sheetId": sheetId,
          "dimension": "COLUMNS",
          "startIndex": 0,
          "endIndex": 1
        }
      }
    })
    requests.append({
      "autoResizeDimensions": {
        "dimensions": {
          "sheetId": sheetId,
          "dimension": "COLUMNS",
          "startIndex": 4
        }
      }
    })

    # wrapStrategy clip

    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheetId
            },
            "cell": {
                "userEnteredFormat":{
                    "wrapStrategy": "CLIP",
                    "verticalAlignment": "MIDDLE",
                    "numberFormat": {
                        "type": "TEXT"
                    }
                }
            },
            "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)"
        }
    })

    body = {
        'requests': requests
    }
    service = discovery.build('sheets', 'v4', credentials=credentials)
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body).execute()

def google_sheet_output(self, output_df, sheetname):

    self.verboseprint('Outputting to google sheet {} {}'.format(str(output_df.shape),
                                                                datetime.fromtimestamp(
                                                                    datetime.now().timestamp()).isoformat()))
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(self.google_client_secret, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheetname)  # this is the spreadsheet not the worksheet

    # add new empty worksheet
    sheet.add_worksheet(title="In progress {}".format(self.timestamp), rows=output_df.shape[0] + 1,
                        cols=output_df.shape[1] + 1)
    # fill new empty worksheet
    gspread_dataframe.set_with_dataframe(sheet.get_worksheet(1), output_df, include_index=True,
                                         include_column_header=True)
    # remove old worksheet in pos 0
    sheet.del_worksheet(sheet.get_worksheet(0))

    post_sheet_formatting(credentials=creds, spreadsheet_id=sheet.id, sheetId=sheet.sheet1.id)