from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

class GoogleSheetWriter:

    def __init__(self, gSheetSpreadSheetId):
        self.gSheetSpreadSheetId = gSheetSpreadSheetId
        self.gSheetSpreadSheet = None

    def openConnection(self):
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        store = file.Storage('token.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('credentials.json', 'https://www.googleapis.com/auth/spreadsheets.readonly')
            creds = tools.run_flow(flow, store)
        service = build('sheets', 'v4', http=creds.authorize(Http()))

        # Call the Sheets API
        self.gSheetSpreadSheet = service.spreadsheets()

    def setCellValue(self, sheetName, cellName, cellValue, isFormula):
        try:
            self.gSheetSpreadSheet.values().update(spreadsheetId=self.gSheetSpreadSheetId, range=(sheetName + '!' + cellName), valueInputOption="USER_ENTERED", body={'values':[[cellValue]]} if isFormula else {'values':[['"' + cellValue + '"']]}).execute()
            return True
        except Exception as e:
            print('Unable to write SpreadSheet. Ex details: ' + str(e))
            return False
