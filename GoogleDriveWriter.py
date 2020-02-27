from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from apiclient import errors

class GoogleDriveWriter:

    def uploadFile(self, localFileName, localFilePath, gDriveFolderId):

        store = file.Storage('token_drive.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('gdrivecredentials.json', 'https://www.googleapis.com/auth/drive')
            creds = tools.run_flow(flow, store)
        service = build('drive', 'v2', http=creds.authorize(Http()))
        fileSearchResponse = service.files().list(q="title='"+localFileName+"'", spaces='drive', fields='items(id, title)').execute()

        for fileFound in fileSearchResponse.get('items', []):
            fileFoundId = fileFound.get('id')
            try:
                service.files().delete(fileId=fileFoundId).execute()
            except:
                print('An error occurred at deleting file ' + fileFoundId)

        fileUploaded = service.files().insert(convert=False, body={'title':localFileName,'parents': [{'id': gDriveFolderId}]}, media_body=localFilePath, fields='id').execute()
        return str(fileUploaded.get('id'))
