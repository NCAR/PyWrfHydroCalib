# Module file that contains various functions for handling interations
# with the NWM calibration database.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import pwd
import MySQLdb

class Database(object):
    def __init__(self,jobData):
        """
        Initialize databse object to include username, password, dbName,
        etc
        """
        self.connected = False
        self.reportLevel = jobData.report
        self.email = jobData.email
        self.host = 'hydro-c1-web' # Hard-coded for now
        self.uName = jobData.dbUName
        self.pwd = jobData.dbPwd
        self.dbName = 'NWM_Calib_DB'
        self.db = None
    
    def connect(self,jobData):
        """
        Connect to the MySQL Databse Server
        """
        if self.connected:
            jobData.errMsg = "ERROR: Connection to DB already established."
            raise
        
        try:
            db = MySQLdb.connect(self.host,self.uName,self.pwd,self.ddbName)
        except:
            jobData.errMsg = "ERROR: Unable to connect to database: " + self.dbName
            raise
            
        self.db = db
        self.conn = db.cursor()
        self.connected = True
        
    def disconnect(self,jobData):
        """
        Disconnect from MySQL database server and cleanup.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: Connection to DB already disconnected."
            raise
            
        if self.conn is not None: self.conn.close()
        self.conn = None
        self.connected = False
        
        
def getCredentials(jobData):
    # Generic routine for looking up DB user name / password
    # based on who is running program.
    
    dbNames = {'karsten':'karstenCalib'}
    dbPwds = {'karsten':'TEST123'}
    
    # Get user ID
    uId = pwd.getpwuid(os.getuid()).pw_name
    
    if not uId in dbNames:
        print "ERROR: User ID not permitted to access calibration database."
        raise
        
    jobData.dbUName = dbNames[uId]
    jobData.dbPwd = dbPwds[uId]