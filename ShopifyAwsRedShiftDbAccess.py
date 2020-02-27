import psycopg2

class ShopifyAwsRedShiftDbAccess:

    def __init__(self, shfyUser, shfyPassword, shfyDbName, shfyHostname, shfyPort):
        self.shfyUser = shfyUser
        self.shfyPassword = shfyPassword
        self.shfyDbName = shfyDbName
        self.shfyHostname = shfyHostname
        self.shfyPort = shfyPort
        self.shfyConn = None

    def openConnection(self):
        self.shfyConn = psycopg2.connect(user=self.shfyUser, password=self.shfyPassword, dbname=self.shfyDbName, host=self.shfyHostname, port=self.shfyPort)

    def executeSqlSelectQuery(self, sqlQuery):
        sqlCurSor = self.shfyConn.cursor()
        sqlCurSor.execute(sqlQuery)
        return sqlCurSor.fetchall()
