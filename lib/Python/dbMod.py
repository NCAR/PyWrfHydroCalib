# Module file that contains various functions for handling interations
# with the NWM calibration database.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

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
        self.host = 'hydro-c1-web.rap.ucar.edu'
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
        
        print self.host
        print self.uName
        print self.pwd
        print self.dbName
        try:
            db = MySQLdb.connect(self.host,self.uName,self.pwd,self.dbName)
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
        
    def getJobID(self,jobData):
        """
        Generic function to return job ID based on information passed in
        by the config file. Search will look by outputdirectory where 
        all output will be placed. This is defined by job name, and
        a top-level directory by the user. This should be unique. May
        want to add more constraints in the future.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise

        # Establish job directory uniquely constrained by job name and top level
        # output directory.            
        jobDir = jobData.outDir + "/" + jobData.jobName
        
        sqlCmd = "select jobID from Job_Meta where Job_Directory='%s'" % (jobDir)
        print jobDir
        print sqlCmd
        
        try:
            self.conn.execute(sqlCmd)
            result = self.conn.fetchone()
            print result
        except:
            jobData.errMsg = "ERROR: Unable to execute SQL command to inquire job ID."
            raise
        
        if result is None:
            # This will be a unique value specific to indicating no Job ID has 
            # been entered for this particular unique job situation.
            jobData.jobID = -9999
        else:
            jobData.jobID = result
            
        print jobData.jobId
        
        
