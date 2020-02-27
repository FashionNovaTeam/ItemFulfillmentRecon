import NetSuiteJdbcAccess
import ShopifyAwsRedShiftDbAccess
import GoogleSheetReader
import GoogleSheetWriter
import GoogleDriveReader
import GoogleDriveWriter
import csv
import sys

#START: Open Database and Service Connections
#analysisDates = ['10/21','10/22','10/23','10/24','10/25','10/26','10/27','10/28','10/29','10/30','10/31','11/1','11/2','11/3','11/4','11/5','11/6','11/7','11/8','11/9','11/10','11/11','11/12','11/13','11/14','11/15','11/16','11/17','11/18','11/19','11/20','11/21','11/22','11/23','11/24','11/25','11/26','11/27','11/28','11/29','11/30','12/1','12/2','12/3','12/4','12/5','12/6','12/7','12/8','12/9','12/10','12/11','12/12','12/13','12/14','12/15','12/16','12/17','12/18','12/19','12/20','12/21','12/22','12/23','12/24','12/25','12/26','12/27','12/28','12/29','12/30','12/31','1/1']
analysisDates = ['1/8/2020']
itemFulfillmentReconGoogleSheet = '1BEQDDmHXui4y_ZNypt5hxryPWtbnPANufqcrcbB01OY'
itemFulfillmentReconGoogleSheetTab = 'OUTBOUND FULFILLMENTS'
gDriveItemFulfillmentReconFolderId = '1l4Ckk4lznWHeEL5jv8MYey5FD_kgWcIO'

#Open NetSuite Connection
nsConnector = NetSuiteJdbcAccess.NetSuiteJdbcAccess('fndbhyperspace@fashionnova.com', 'w3m1sSF4Rol1tO!', '1086', '4541626', '4541626.connect.api.netsuite.com', '1708',)
nsConnector.openConnection()
print('Connected to NetSuite')

#Open Shopify Connection
shfyConnector = ShopifyAwsRedShiftDbAccess.ShopifyAwsRedShiftDbAccess('fashionnova', 'cZxmj9tUXqcrXxUR', 'shopify', 'fashionnova.warehouse.shopifysds.com', '5439')
shfyConnector.openConnection()
print('Connected to Shopify')

#Get Export Item Fulfillment Table Order Numbers
itemFulfillmentTblOrderGSheetReader = GoogleSheetReader.GoogleSheetReader(itemFulfillmentReconGoogleSheet)
itemFulfillmentTblOrderGSheetReader.openConnection()
#END: Open Database and Service Connections
print('Connected to Google')


#Function that downloads CSV file from Google Drive
def getCsvFileFromGoogleDrive(sourceGDriveCsvFileId):
    try:
        googleDriveReader = GoogleDriveReader.GoogleDriveReader()
        return googleDriveReader.downloadFileByID(sourceGDriveCsvFileId)
    except:
        return None

#Function that writes an String list to a local CSV file
def writeStringListToLocalBasicCsvFile(localFilePath, stringList):
    try:
        f = open(localFilePath, 'w')
        csvWriter = csv.writer(f, delimiter=',')
        for i in range(len(stringList)):
            csvWriter.writerow([stringList[i]])
        f.close()
        return True
    except:
        return False

#Function that gets NetSuite Fulfillment Status for an specified Order

#Function that gets Shopify Fulfillment Status for an specified Order


#START: Pull Order Numbers to Analyze from Item Fulfillment Recon Google Sheet
#Matrix of Data
ifReconDate = []
ifReconGSheetRow = []
ifReconOrderFileDownloadResult = []
ifReconOrderNumbers = []
ifReconShopifyFulfilled = []
ifReconShopifyNotFulfilledRefunded = []
ifReconShopifyNotFulfilled = []
ifReconShopifyMissed = []
ifReconNetSuiteFulfilled = []
ifReconNetSuiteCancelled = []
ifReconNetSuiteNotFulfilled = []
ifReconNetSuiteMissed = []

print('Getting values from Google')

itemFulfillmentTblOrderGSheetPlainValues = itemFulfillmentTblOrderGSheetReader.getRangeValues(itemFulfillmentReconGoogleSheetTab, '$A:$L', False)
itemFulfillmentTblOrderGSheetFormulaValues = itemFulfillmentTblOrderGSheetReader.getRangeValues(itemFulfillmentReconGoogleSheetTab, '$A:$L', True)

for i in range(len(itemFulfillmentTblOrderGSheetPlainValues)):
    if i == 0 or str(itemFulfillmentTblOrderGSheetPlainValues[i][0]) not in analysisDates:
        continue
    dataReconciliationDate = None
    dataGSheetCsvOrderNumbersDataFileId = None
    dataGSheetRowNumber = i+1
    try:
        dataReconciliationDate = itemFulfillmentTblOrderGSheetPlainValues[i][0]
        if str(itemFulfillmentTblOrderGSheetFormulaValues[i][4]).startswith('=HYPERLINK("https://drive.google.com/open?id='):
            dataGSheetCsvOrderNumbersDataFileId = str(itemFulfillmentTblOrderGSheetFormulaValues[i][4]).replace('=HYPERLINK("https://drive.google.com/open?id=', '').split('"', 1)[0]
        else:
            dataGSheetCsvOrderNumbersDataFileId = 'Invalid HYPERLINK format.'

        ifReconDate.append(dataReconciliationDate)
        ifReconGSheetRow.append(dataGSheetRowNumber)
        ifReconOrderNumbers.append([])
        ifReconShopifyFulfilled.append([])
        ifReconShopifyNotFulfilledRefunded.append([])
        ifReconShopifyNotFulfilled.append([])
        ifReconShopifyMissed.append([])
        ifReconNetSuiteFulfilled.append([])
        ifReconNetSuiteCancelled.append([])
        ifReconNetSuiteNotFulfilled.append([])
        ifReconNetSuiteMissed.append([])

        if dataGSheetCsvOrderNumbersDataFileId != 'Invalid HYPERLINK format.' and dataGSheetCsvOrderNumbersDataFileId != None:
            print('Starting to Pull Order Numbers for Recon Date:' + str(dataReconciliationDate) + ' File Key: ' + dataGSheetCsvOrderNumbersDataFileId)
            orderNumbersGSheetReader = GoogleSheetReader.GoogleSheetReader(dataGSheetCsvOrderNumbersDataFileId)
            orderNumbersGSheetReader.openConnection()
            orderNumbersGSheet = orderNumbersGSheetReader.getFirstSheetRangeValues('$A:$A', False)
            for gSheetRow in orderNumbersGSheet:
                ifReconOrderNumbers[len(ifReconOrderNumbers) - 1].append(gSheetRow[0])

            print('Finishing to Pull Order Numbers for Recon Date:' + str(dataReconciliationDate) + ' File Key: ' + dataGSheetCsvOrderNumbersDataFileId + ' Total Rows: ' + str(len(ifReconOrderNumbers[len(ifReconOrderNumbers) - 1])))
            #Old Approach loading CSV in memory
            #shfyOrdersFulfilledCsvStream = getCsvFileFromGoogleDrive(dataGSheetCsvOrderNumbersDataFileId)
            #if shfyOrdersFulfilledCsvStream != None:
            #    ifReconOrderFileDownloadResult.append('Successful')
            #    shfyOrdersFulfilledCsvLines = shfyOrdersFulfilledCsvStream.getvalue().splitlines()
            #    for j in range(len(shfyOrdersFulfilledCsvLines)):
            #        if j == 0:
            #            continue
            #        ifReconOrderNumbers[len(ifReconOrderNumbers)-1].append(shfyOrdersFulfilledCsvLines[j].decode('utf-8'))
            #else:
            #    ifReconOrderFileDownloadResult.append('Failed. File could not be downloaded or file content is empty.')
        else:
            ifReconOrderFileDownloadResult.append('Invalid HYPERLINK format in Recon Google Sheet.' if dataGSheetCsvOrderNumbersDataFileId != None else 'HYPERLINK not found in Google Sheet.')
    except IndexError:
        dataGSheetId = None
#END: Pull Order Numbers to Analyze from Item Fulfillment Recon Google Sheet

#START: Analyze Order Fulfillment Status from NetSuite and Shopify

orderNumberCounter = 0
totalOrderCounter = 0
batchSize = 500
for i in range(len(ifReconDate)):
    print('Starting to Query NetSuite and Shopify databases for Recon Date:' + str(ifReconDate[i]) + ' Total Orders: ' + str(len(ifReconOrderNumbers[i])))
    batchOrderNumbers = []
    totalOrderCounter = 0
    for j in range(len(ifReconOrderNumbers[i])):

        #Skip empty order numbers
        if(len(str(ifReconOrderNumbers[i][j])) > 0):
            batchOrderNumbers.append(ifReconOrderNumbers[i][j])

        if(orderNumberCounter/batchSize == 1):
            totalOrderCounter = totalOrderCounter + 1
            print('Pulling ' + str(totalOrderCounter-batchSize) + ' <--> ' + str(totalOrderCounter) + ' of ' + str(len(ifReconOrderNumbers[i])) + ' for recon date: ' + str(ifReconDate[i]))
            nsResultsFetch = nsConnector.executeSqlSelectQuery(
            "SELECT trn.TRANID as order_number, trn.ETAIL_ORDER_ID as shopify_etail_id, trn.TRANSACTION_ID AS netsuite_internal_id, trn.STATUS as netsuite_order_status, orderCommitStatus.LIST_ITEM_NAME as order_commit_status " +
            " FROM TRANSACTIONS trn INNER JOIN ORDER_COMMITMENT_STATE_LIST orderCommitStatus ON trn.ORDER_COMMITMENT_STATUS_ID = orderCommitStatus.LIST_ID "
            " WHERE trn.TRANSACTION_TYPE = 'Sales Order' AND trn.CREATE_DATE > add_months(sysdate(), - 4) AND trn.ETAIL_ORDER_ID IS NOT NULL AND SUBSTRING(trn.TRANID, 1, 2) != 'SO' "
            "AND trn.TRANID IN ('" + ("','".join(batchOrderNumbers)) + "')")

            for nsResultRow in nsResultsFetch:
                if(str(nsResultRow[4]) == 'Shipped'):
                    ifReconNetSuiteFulfilled[i].append(str(nsResultRow[0]))
                if(str(nsResultRow[4]) == 'Cancelled'):
                    ifReconNetSuiteCancelled[i].append(str(nsResultRow[0]))
                if(str(nsResultRow[4]) != 'Shipped' and str(nsResultRow[4]) != 'Cancelled' ):
                    ifReconNetSuiteNotFulfilled[i].append(str(nsResultRow[0]))

            shfyResultsFetch = shfyConnector.executeSqlSelectQuery(
                "SELECT REPLACE(shfyOrders.name, '#', '') as order_number, shfyOrders.fulfillment_status as fulfillment_status, shfyOrders.financial_status as financial_status FROM shopify.orders shfyOrders WHERE shfyOrders.name IN ('#" + ("','#".join(batchOrderNumbers)) + "')")

            for shfyResultRow in shfyResultsFetch:
                if(str(shfyResultRow[1]) != 'unfulfilled'):
                    ifReconShopifyFulfilled[i].append(str(shfyResultRow[0]))
                if(str(shfyResultRow[1]) == 'unfulfilled' and str(shfyResultRow[2]) == 'refunded'):
                    ifReconShopifyNotFulfilledRefunded[i].append(str(shfyResultRow[0]))
                if(str(shfyResultRow[1]) == 'unfulfilled' and str(shfyResultRow[2]) != 'refunded'):
                    ifReconShopifyNotFulfilled[i].append(str(shfyResultRow[0]))

            batchOrderNumbers = []
            orderNumberCounter = 0
        else:
            orderNumberCounter = orderNumberCounter + 1
            totalOrderCounter = totalOrderCounter + 1

    if len(ifReconOrderNumbers[i]) > 0:

        nsResultsFetch = nsConnector.executeSqlSelectQuery(
            "SELECT trn.TRANID as order_number, trn.ETAIL_ORDER_ID as shopify_etail_id, trn.TRANSACTION_ID AS netsuite_internal_id, trn.STATUS as netsuite_order_status, orderCommitStatus.LIST_ITEM_NAME as order_commit_status " +
            " FROM TRANSACTIONS trn INNER JOIN ORDER_COMMITMENT_STATE_LIST orderCommitStatus ON trn.ORDER_COMMITMENT_STATUS_ID = orderCommitStatus.LIST_ID "
            " WHERE trn.TRANSACTION_TYPE = 'Sales Order' AND trn.CREATE_DATE > add_months(sysdate(), - 4) AND trn.ETAIL_ORDER_ID IS NOT NULL AND SUBSTRING(trn.TRANID, 1, 2) != 'SO' "
            "AND trn.TRANID IN ('" + ("','".join(batchOrderNumbers)) + "')")

        for nsResultRow in nsResultsFetch:
            if (str(nsResultRow[4]) == 'Shipped'):
                ifReconNetSuiteFulfilled[i].append(str(nsResultRow[0]))
            if (str(nsResultRow[4]) == 'Cancelled'):
                ifReconNetSuiteCancelled[i].append(str(nsResultRow[0]))
            if (str(nsResultRow[4]) != 'Shipped' and str(nsResultRow[4]) != 'Cancelled'):
                ifReconNetSuiteNotFulfilled[i].append(str(nsResultRow[0]))

        shfyResultsFetch = shfyConnector.executeSqlSelectQuery(
            "SELECT REPLACE(shfyOrders.name, '#', '') as order_number, shfyOrders.fulfillment_status as fulfillment_status, shfyOrders.financial_status as financial_status FROM shopify.orders shfyOrders WHERE shfyOrders.name IN ('#" + (
                "','#".join(batchOrderNumbers)) + "')")

        for shfyResultRow in shfyResultsFetch:
            if (str(shfyResultRow[1]) != 'unfulfilled'):
                ifReconShopifyFulfilled[i].append(str(shfyResultRow[0]))
            if (str(shfyResultRow[1]) == 'unfulfilled' and str(shfyResultRow[2]) == 'refunded'):
                ifReconShopifyNotFulfilledRefunded[i].append(str(shfyResultRow[0]))
            if (str(shfyResultRow[1]) == 'unfulfilled' and str(shfyResultRow[2]) != 'refunded'):
                ifReconShopifyNotFulfilled[i].append(str(shfyResultRow[0]))

        orderNumberCounter = 0;
    print('Finishing to Query NetSuite and Shopify databases for Recon Date:' + str(ifReconDate[i]) + ' Total Orders:' + str(totalOrderCounter))
    totalOrderCounter = 0

#To Do: Look for Missed Orders
#for i in range(len(ifReconDate)):
#   print('Recon Date:' + str(ifReconDate[i]) + '--> Orders Fulfilled in Shopify: ' + str(len(ifReconShopifyFulfilled[i])) + ' --> Orders Fulfilled in NetSuite: ' + str(len(ifReconNetSuiteFulfilled[i])))


#END: Analyze Order Fulfillment Status from NetSuite and Shopify


#START: Building, Exporting and linking CSV Recon files

gDriveWriter = GoogleDriveWriter.GoogleDriveWriter();

itemFulfillmentTblOrderGSheetWriter = GoogleSheetWriter.GoogleSheetWriter(itemFulfillmentReconGoogleSheet)
itemFulfillmentTblOrderGSheetWriter.openConnection()

for i in range(len(ifReconDate)):
    print('Building, Exporting and linking CSV files for Recon Date:' + str(ifReconDate[i]))

    dateSuffixForFileName = str(ifReconDate[i]).replace('/','_') + '_2018.csv';

    if (writeStringListToLocalBasicCsvFile('csvReconFiles/ifReconShopifyFulfilled_' + dateSuffixForFileName, ifReconShopifyFulfilled[i])):
        gDriveFileId = gDriveWriter.uploadFile('ifReconShopifyFulfilled_' + dateSuffixForFileName, 'csvReconFiles/ifReconShopifyFulfilled_' + dateSuffixForFileName, gDriveItemFulfillmentReconFolderId)
        itemFulfillmentTblOrderGSheetWriter.setCellValue(itemFulfillmentReconGoogleSheetTab, 'G'+str(ifReconGSheetRow[i]), '=HYPERLINK("https://drive.google.com/open?id=' + gDriveFileId + '", ' + str(len(ifReconShopifyFulfilled[i])) + ')', True)

    if (writeStringListToLocalBasicCsvFile('csvReconFiles/ifReconShopifyNotFulfilledRefunded_' + dateSuffixForFileName, ifReconShopifyNotFulfilledRefunded[i])):
        gDriveFileId = gDriveWriter.uploadFile('ifReconShopifyNotFulfilledRefunded_' + dateSuffixForFileName, 'csvReconFiles/ifReconShopifyNotFulfilledRefunded_' + dateSuffixForFileName, gDriveItemFulfillmentReconFolderId)
        itemFulfillmentTblOrderGSheetWriter.setCellValue(itemFulfillmentReconGoogleSheetTab, 'I'+str(ifReconGSheetRow[i]), '=HYPERLINK("https://drive.google.com/open?id=' + gDriveFileId + '", ' + str(len(ifReconShopifyNotFulfilledRefunded[i])) + ')', True)

    if (writeStringListToLocalBasicCsvFile('csvReconFiles/ifReconShopifyNotFulfilled_' + dateSuffixForFileName, ifReconShopifyNotFulfilled[i])):
        gDriveFileId = gDriveWriter.uploadFile('ifReconShopifyNotFulfilled_' + dateSuffixForFileName, 'csvReconFiles/ifReconShopifyNotFulfilled_' + dateSuffixForFileName, gDriveItemFulfillmentReconFolderId)
        itemFulfillmentTblOrderGSheetWriter.setCellValue(itemFulfillmentReconGoogleSheetTab, 'J'+str(ifReconGSheetRow[i]), '=HYPERLINK("https://drive.google.com/open?id=' + gDriveFileId + '", ' + str(len(ifReconShopifyNotFulfilled[i])) + ')', True)

    if (writeStringListToLocalBasicCsvFile('csvReconFiles/ifReconNetSuiteFulfilled_' + dateSuffixForFileName, ifReconNetSuiteFulfilled[i])):
        gDriveFileId = gDriveWriter.uploadFile('ifReconNetSuiteFulfilled_' + dateSuffixForFileName, 'csvReconFiles/ifReconNetSuiteFulfilled_' + dateSuffixForFileName, gDriveItemFulfillmentReconFolderId)
        itemFulfillmentTblOrderGSheetWriter.setCellValue(itemFulfillmentReconGoogleSheetTab, 'L'+str(ifReconGSheetRow[i]), '=HYPERLINK("https://drive.google.com/open?id=' + gDriveFileId + '", ' + str(len(ifReconNetSuiteFulfilled[i])) + ')', True)

    if (writeStringListToLocalBasicCsvFile('csvReconFiles/ifReconNetSuiteCancelled_' + dateSuffixForFileName, ifReconNetSuiteCancelled[i])):
        gDriveFileId = gDriveWriter.uploadFile('ifReconNetSuiteCancelled_' + dateSuffixForFileName, 'csvReconFiles/ifReconNetSuiteCancelled_' + dateSuffixForFileName, gDriveItemFulfillmentReconFolderId)
        itemFulfillmentTblOrderGSheetWriter.setCellValue(itemFulfillmentReconGoogleSheetTab, 'N'+str(ifReconGSheetRow[i]), '=HYPERLINK("https://drive.google.com/open?id=' + gDriveFileId + '", ' + str(len(ifReconNetSuiteCancelled[i])) + ')', True)

    if (writeStringListToLocalBasicCsvFile('csvReconFiles/ifReconNetSuiteNotFulfilled_' + dateSuffixForFileName, ifReconNetSuiteNotFulfilled[i])):
        gDriveFileId = gDriveWriter.uploadFile('ifReconNetSuiteNotFulfilled_' + dateSuffixForFileName, 'csvReconFiles/ifReconNetSuiteNotFulfilled_' + dateSuffixForFileName, gDriveItemFulfillmentReconFolderId)
        itemFulfillmentTblOrderGSheetWriter.setCellValue(itemFulfillmentReconGoogleSheetTab, 'O'+str(ifReconGSheetRow[i]), '=HYPERLINK("https://drive.google.com/open?id=' + gDriveFileId + '", ' + str(len(ifReconNetSuiteNotFulfilled[i])) + ')', True)

#END: Building, Exporting and linking CSV Recon files
