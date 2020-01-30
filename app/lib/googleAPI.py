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
import time

def google_sheet_output(google_client_secret, output_dfs, spreadsheetname):

    print('Outputting to google sheet {}'.format(datetime.fromtimestamp(datetime.now().timestamp()).isoformat()))
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(google_client_secret, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open(spreadsheetname)  # this is the spreadsheet not the worksheet

    keep_sheets = []

    for title, df in output_dfs.items():
        # add new empty worksheet
        sheetname = '{} {}'.format(title, datetime.now())
        keep_sheets.append(sheetname)

        spreadsheet.add_worksheet(title=sheetname, rows=df.shape[0] + 1, cols=df.shape[1] + 1)
        # fill new empty worksheet
        try:
            gspread_dataframe.set_with_dataframe(spreadsheet.worksheet(sheetname), df, include_index=True, include_column_header=True)
        # except gspread.exceptions.APIError: # I've seen this error and socket timeout error. Catching all for now.
        except:
            time.sleep(60)
            print('Hit Google API error. Waiting 1 min then retrying...')
            try:
                gspread_dataframe.set_with_dataframe(spreadsheet.worksheet(sheetname), df, include_index=True,include_column_header=True)
            except:
                time.sleep(600)
                print('Hit Google API error. Waiting 10 min then retrying...')
                gspread_dataframe.set_with_dataframe(spreadsheet.worksheet(sheetname), df, include_index=True,include_column_header=True)

        post_sheet_formatting(credentials=creds, spreadsheet_id=spreadsheet.id,sheetId=spreadsheet.worksheet(sheetname).id)

    # remove old worksheets
    for sheet in spreadsheet.worksheets():
        title = sheet.title
        if title not in keep_sheets:
            spreadsheet.del_worksheet(spreadsheet.worksheet(title))

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
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheetId,
                "dimension": "COLUMNS",
                "startIndex": 2
            },
            "properties": {
                "pixelSize": 190
            },
            "fields": "pixelSize"
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