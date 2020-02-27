from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from apiclient import http, errors
from oauth2client import file, client, tools
import io

class GoogleDriveReader:

    def downloadFileByID(self, gDriveFileId):

        store = file.Storage('token_drive.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('gdrivecredentials.json', 'https://www.googleapis.com/auth/drive')
            creds = tools.run_flow(flow, store)
        service = build('drive', 'v3', http=creds.authorize(Http()))
        request = service.files().get_media(fileId=gDriveFileId)

        fh = io.BytesIO()
        media_request = http.MediaIoBaseDownload(fh, request)

        while True:
            try:
                download_progress, done = media_request.next_chunk()
            except errors:
                print('Unable to write SpreadSheet. Ex details: ' + str(errors))
                return None
            if done:
                return fh
