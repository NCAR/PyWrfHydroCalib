# Module file that contains various functions for handling interations
# with the NWM calibration database.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import MySQLdb
import datetime

class Database(object):
    def __init__(self,jobData):
        """
        Initialize databse object to include username, password, dbName,
        etc
        """
        self.connected = False
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
        print jobDir
        print jobData.bSpinDate.strftime('%Y-%m-%d')
        print jobData.eSpinDate.strftime('%Y-%m-%d')
        print jobData.bCalibDate.strftime('%Y-%m-%d')
        print jobData.eCalibDate.strftime('%Y-%m-%d')
        print jobData.nIter
        print jobData.bValidDate.strftime('%Y-%m-%d')
        print jobData.eValidDate.strftime('%Y-%m-%d')
        print jobData.acctKey
        print jobData.nCores
        print jobData.exe
        sqlCmd = "insert into Job_Meta (Job_Directory,date_su_start,date_su_end," + \
                 "su_complete,date_calib_start,date_calib_end,num_iter," + \
                 "iter_complete,calib_complete,valid_start_date,valid_end_date," + \
                 "valid_complete,acct_key,num_cores,exe,num_gages) values " + \
                 "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (jobDir,jobData.bSpinDate.strftime('%Y-%m-%d'),\
                 jobData.eSpinDate.strftime('%Y-%m-%d'),0,jobData.bCalibDate.strftime('%Y-%m-%d'),\
                 jobData.eCalibDate.strftime('%Y-%m-%d'),jobData.nIter,0,0,\
                 jobData.bValidDate.strftime('%Y-%m-%d'),jobData.eValidDate.strftime('%Y-%m-%d'),\
                 0,jobData.acctKey,jobData.nCores,jobData.exe,len(jobData.gages))
         
        print sqlCmd
        self.conn.execute(sqlCmd)
        self.db.commit()
        #try:
        #    self.conn.execute(sqlCmd)
        #    self.db.commit()
        #except:
        #    jobData.errMsg = "ERROR: Unable to create JobID for job name: " + jobData.jobName
        #    raise
            
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
        
    def jobStatus(self,jobData):
        """
        Function to extract job metadata (including status information) for
        a given job ID.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise
            
        sqlCmd = "select * from Job_Meta where jobID='" + str(jobData.jobID[0]) + "';"
        print sqlCmd        
        
        try:
            self.conn.execute(sqlCmd)
            results = self.conn.fetchone()
        except:
            jobData.errMsg = "ERROR: Unable to extract metadata for job ID: " + str(jobData.jobID[0])
            raise
            
        # Fill jobData object with metadata on job and status.
        jobData.jobDir = results[1]
        jobData.bSpinDate = datetime.datetime.strptime(str(results[2]),'%Y-%m-%d')
        jobData.eSpinDate = datetime.datetime.strptime(str(results[3]),'%Y-%m-%d')
        jobData.spinComplete = int(results[4])
        jobData.bCalibDate = datetime.datetime.strptime(str(results[5]),'%Y-%m-%d')
        jobData.eCalibDate = datetime.datetime.strptime(str(results[6]),'%Y-%m-%d')
        jobData.nIter = int(results[7])
        jobData.calibIter = int(results[8])
        jobData.calibComplete = int(results[9])
        jobData.bValidDate = datetime.datetime.strptime(str(results[10]),'%Y-%m-%d')
        jobData.eValidDate = datetime.datetime.strptime(str(results[11]),'%Y-%m-%d')
        jobData.validComplete = int(results[12])
        jobData.acctKey = results[13]
        jobData.nCores = int(results[14])
        jobData.exe = results[15]

        print jobData.jobDir
        print jobData.bSpinDate
        print jobData.eSpinDate
        print jobData.bCalibDate
        print jobData.eCalibDate
        print jobData.bValidDate
        print jobData.eValidDate
        print jobData.spinComplete
        print jobData.calibComplete
        print jobData.validComplete
        print jobData.nIter
        print jobData.calibIter
        print jobData.acctKey
        print jobData.nCores
        print jobData.exe