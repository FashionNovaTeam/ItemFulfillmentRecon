import jaydebeapi

class NetSuiteJdbcAccess:

    def __init__(self, nsUser, nsPassword, nsRole, nsAccount, nsHostname, nsPort):
        self.nsUser = nsUser
        self.nsPassword = nsPassword
        self.nsRole = nsRole
        self.nsAccount = nsAccount
        self.nsHostname = nsHostname
        self.nsPort = nsPort
        self.nsConn = None

    def openConnection(self):
        self.nsConn = jaydebeapi.connect('com.netsuite.jdbc.openaccess.OpenAccessDriver',
                                  'jdbc:ns://'+ self.nsHostname  + ':' + self.nsPort + ';ServerDataSource=NetSuite.com;Encrypted=1;CustomProperties=(AccountID=' + self.nsAccount+ ';RoleID=' + self.nsRole + ')',
                                  [self.nsUser, self.nsPassword],
                                  ['NQjc.jar'],)

    def executeSqlSelectQuery(self, sqlQuery):
        sqlCurSor = self.nsConn.cursor()
        sqlCurSor.execute(sqlQuery)
        return sqlCurSor.fetchall()
