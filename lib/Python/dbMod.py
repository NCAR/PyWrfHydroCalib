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
        
        sqlCmd = "select jobID from Job_Meta where Job_Directory='%s'" % (jobDir) + ";"
        
        try:
            self.conn.execute(sqlCmd)
            result = self.conn.fetchone()
        except:
            jobData.errMsg = "ERROR: Unable to execute SQL command to inquire job ID."
            raise
        
        if result is None:
            # This will be a unique value specific to indicating no Job ID has 
            # been entered for this particular unique job situation.
            jobData.jobID = -9999
        else:
            jobData.jobID = result[0]
        
    def enterJobID(self,jobData):
        """
        Function to create unique Job ID that defines the job being ran.
        This will be uniquely constrained by the job name, along with 
        the top level directory where output is being stored. Additional
        information specific to the job (start/stop dates,iterations,
        job status,etc) will also be entered in.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise
            
        jobDir = jobData.outDir + "/" + jobData.jobName
        sqlCmd = "insert into Job_Meta (Job_Directory) values " + \
                 "('%s');" % (jobDir)
                 
        print sqlCmd
        try:
            self.conn.execute(sqlCmd)
            self.db.commit()
        except:
            jobData.errMsg = "ERROR: Unable to create JobID for job name: " + jobData.jobName
            raise
            
    def queryGageList(self,jobData):
        """
        Function to extract list of gages based on user-provided SQL command.
        """
        listOut = []
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise
            
        try:
            self.conn.execute(str(jobData.gSQL))
            results = self.conn.fetchall()
        except:
            jobData.errMsg = "ERROR: Unable to query domain metadata for gages list."
            raise
            
        if len(results) == 0:
            jobData.errMsg = "ERROR: Gage query returned 0 gages for calibration."
            raise
            
        numGages = len(results)
        for gage in range(0,numGages):
            listOut.append(results[gage][1])
        
        return listOut
        
    def queryGageMeta(self,jobData,tmpMeta):
        """
        Function to query the gages meta table for information specific to
        each GAGES II basin, such as domain files, indices, etc."
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise
            
        sqlCmd = "select * from Domain_Meta where gage_id='" + tmpMeta['gageName'] + "';"
        
        try:
            self.conn.execute(sqlCmd)
            results = self.conn.fetchone()
        except:
            jobData.errMsg = "ERROR: Unable to query domain meta table for gages metadata."
            raise
            
        tmpMeta['geoFile'] = results[12]
        tmpMeta['wrfInput'] = results[13]
        tmpMeta['soilFile'] = results[14]
        tmpMeta['fullDomFile'] = results[15]
        tmpMeta['rtLnk'] = results[16]
        tmpMeta['udMap'] = results[17]
        tmpMeta['gwFile'] = results[18]
        tmpMeta['lkFile'] = results[19]
        
