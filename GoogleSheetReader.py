from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

class GoogleSheetReader:

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

    def getFirstSheetCellValue(self, cellName, returnFormula):
        result = self.gSheetSpreadSheet.values().get(spreadsheetId=self.gSheetSpreadSheetId, range=cellName, valueRenderOption='FORMULA' if returnFormula else 'FORMATTED_VALUE').execute()
        return result.get('values', [])

    def getFirstSheetColumnValues(self, column, returnFormula):
        result = self.gSheetSpreadSheet.values().get(spreadsheetId=self.gSheetSpreadSheetId, range='$' + column + ':$' + column, valueRenderOption='FORMULA' if returnFormula else 'FORMATTED_VALUE').execute()
        return result.get('values', [])

    def getFirstSheetRangeValues(self, rangeName, returnFormula):
        result = self.gSheetSpreadSheet.values().get(spreadsheetId=self.gSheetSpreadSheetId, range=rangeName, valueRenderOption='FORMULA' if returnFormula else 'FORMATTED_VALUE').execute()
        return result.get('values', [])

    def getCellValue(self, sheetName, cellName, returnFormula):
        result = self.gSheetSpreadSheet.values().get(spreadsheetId=self.gSheetSpreadSheetId, range=(sheetName + '!' + cellName), valueRenderOption='FORMULA' if returnFormula else 'FORMATTED_VALUE').execute()
        return result.get('values', [])

    def getColumnValues(self, sheetName, column, returnFormula):
        result = self.gSheetSpreadSheet.values().get(spreadsheetId=self.gSheetSpreadSheetId, range=(sheetName + '!' + '$' + column + ':$' + column), valueRenderOption='FORMULA' if returnFormula else 'FORMATTED_VALUE').execute()
        return result.get('values', [])

    def getRangeValues(self, sheetName, rangeName, returnFormula):
        result = self.gSheetSpreadSheet.values().get(spreadsheetId=self.gSheetSpreadSheetId, range=(sheetName + '!' + rangeName), valueRenderOption='FORMULA' if returnFormula else 'FORMATTED_VALUE').execute()
        return result.get('values', [])
