# Module file that contains various functions for handling interations
# with the NWM calibration database.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import MySQLdb
import datetime
from slacker import Slacker
import sys
import pandas as pd

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
            raise Exception()
        
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
            raise Exception()
            
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
            raise Exception()

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
            
    def getDomainID(self,jobData,gageName):
        """
        Generic function to return unique ID value for a given basin based on
        the name of the gage.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
            
        sqlCmd = "select domainID from Domain_Meta where gage_id='%s'" % (str(gageName)) + ";"
        
        try:
            self.conn.execute(sqlCmd)
            result = self.conn.fetchone()
        except:
            jobData.errMsg = "ERROR: Unable to locate ID for gage: " + str(gageName)
            raise
            
        if result is None:
            jobData.errMsg = "ERROR: gage: " + str(gageName) + " not found in database."
            raise Exception()
        
        return int(result[0])
        
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
            raise Exception()
            

        # Fill in None contact information with "MISSING" strings. This will be
        # useful when extracting for later. 
        if not jobData.email:
            emailStr = "MISSING"
        else:
            emailStr = str(jobData.email)
        if not jobData.slChan:
            slStr1 = "MISSING"
            slStr2 = "MISSING"
            slStr3 = "MISSING"
        else:
            slStr1 = str(jobData.slChan)
            slStr2 = str(jobData.slToken)
            slStr3 = str(jobData.slUser)
            
        jobDir = jobData.outDir + "/" + jobData.jobName
        sqlCmd = "insert into Job_Meta (Job_Directory,date_su_start,date_su_end," + \
                 "su_complete,date_calib_start,date_calib_end,num_iter," + \
                 "iter_complete,calib_complete,valid_start_date,valid_end_date," + \
                 "valid_complete,acct_key,num_cores,exe,num_gages,owner,email," + \
                 "slack_channel,slack_token,slack_user) values " + \
                 "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (jobDir,jobData.bSpinDate.strftime('%Y-%m-%d'),\
                 jobData.eSpinDate.strftime('%Y-%m-%d'),0,jobData.bCalibDate.strftime('%Y-%m-%d'),\
                 jobData.eCalibDate.strftime('%Y-%m-%d'),jobData.nIter,0,0,\
                 jobData.bValidDate.strftime('%Y-%m-%d'),jobData.eValidDate.strftime('%Y-%m-%d'),\
                 0,jobData.acctKey,jobData.nCores,jobData.exe,len(jobData.gages),jobData.owner,\
                 emailStr,slStr1,slStr2,slStr3)
        
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
            raise Exception()
            
        try:
            self.conn.execute(str(jobData.gSQL))
            results = self.conn.fetchall()
        except:
            jobData.errMsg = "ERROR: Unable to query domain metadata for gages list."
            raise
            
        if len(results) == 0:
            jobData.errMsg = "ERROR: Gage query returned 0 gages for calibration."
            raise Exception()
            
        numGages = len(results)
        for gage in range(0,numGages):
            listOut.append(results[gage][1])
        
        return listOut
        
    def lookupGage(self,jobData,gageName):
        """
        Generic function to check if gage exists in metadata table.
        This is mostly used to ensure the user specified the correct gage.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise
            
        sqlCmd = "Select * from Domain_Meta where gage_id='" + str(gageName) + "';"
        
        try:
            self.conn.execute(sqlCmd)
            results = self.conn.fetchone()
        except:
            jobData.errMsg = "ERROR: Unable to locate gage: " + str(gageName)
            raise
            
        if not results:
            jobData.errMsg = "ERROR: Unable to locate gage: " + str(gageName)
            raise Exception()
        
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
            
        if not results:
            jobData.errMsg = "ERROR: No gage data for: " + tmpMeta['gageName']
            raise Exception()
            
        tmpMeta['gageID'] = results[0]
        tmpMeta['geoFile'] = results[12]
        tmpMeta['wrfInput'] = results[13]
        tmpMeta['soilFile'] = results[14]
        tmpMeta['fullDomFile'] = results[15]
        tmpMeta['rtLnk'] = results[16]
        tmpMeta['udMap'] = results[17]
        tmpMeta['gwFile'] = results[18]
        tmpMeta['lkFile'] = results[19]
        tmpMeta['forceDir'] = results[20]
        tmpMeta['obsFile'] = results[21]
        # PLACEHOLDER
        #tmpMeta['comID'] = results[22]
        tmpMeta['comID'] = -9999
        
    def jobStatus(self,jobData):
        """
        Function to extract job metadata (including status information) for
        a given job ID.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise
            
        sqlCmd = "select * from Job_Meta where jobID='" + str(jobData.jobID) + "';"
        
        try:
            self.conn.execute(sqlCmd)
            results = self.conn.fetchone()
        except:
            jobData.errMsg = "ERROR: Unable to extract metadata for job ID: " + str(jobData.jobID)
            raise
            
        if not results:
            jobData.errMsg = "ERROR: No job data for matching ID of: " + str(jobData.jobID)
            raise Exception()
            
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
        jobData.nGages = int(results[16])
        jobData.owner = results[17]
        jobData.email = results[18]
        jobData.slChan = results[19]
        jobData.slToken = results[20]
        jobData.slUser = results[21]
        
        # Initiate Slack if fields are not MISSING
        if jobData.slChan != "MISSING":
            jobData.email = None
            try:
                jobData.slackObj = Slacker(str(jobData.slToken))
            except:
                jobData.errMsg = "ERROR: Failure to initiate Slack object for user: " + \
                                 str(jobData.slUser) + " channel: " + str(jobData.slChan)
                raise
        else:
            jobData.slChan = None
            jobData.slToken = None
            jobData.slUser = None
            jobData.slackObj = None
        
    def updateJobOwner(self,jobData,newOwner,newEmail,newSlackChannel,newSlackToken,newSlackUName,changeFlag):
        """
        Generic function to update the job owner name and contact information 
        for situations where a different user is re-starting the job and needs
        to take over.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise
            
        sqlCmd1 = "update Job_Meta set Job_Meta.owner='" + str(newOwner) + \
                  "' where jobID='" + str(jobData.jobID) + "';"
        sqlCmd2 = "update Job_Meta set Job_Meta.email='" + str(newEmail) + \
                  "' where jobID='" + str(jobData.jobID) + "';"
        sqlCmd3 = "update Job_Meta set Job_Meta.slack_channel='" + str(newSlackChannel) + \
                  "' where jobID='" + str(jobData.jobID) + "';"
        sqlCmd4 = "update Job_Meta set Job_Meta.slack_token='" + str(newSlackToken) + \
                  "' where jobID='" + str(jobData.jobID) + "';"
        sqlCmd5 = "update Job_Meta set Job_Meta.slack_user='" + str(newSlackUName) + \
                  "' where jobID='" + str(jobData.jobID) + "';"
        sqlCmd6 = "update Job_Meta set Job_Meta.email='MISSING'" + \
                  " where jobID='" + str(jobData.jobID) + "';"
        sqlCmd7 = "update Job_Meta set Job_Meta.slack_channel='MISSING'" + \
                  " where jobID='" + str(jobData.jobID) + "';"
        sqlCmd8 = "update Job_Meta set Job_Meta.slack_token='MISSING'" + \
                  " where jobID='" + str(jobData.jobID) + "';"
        sqlCmd9 = "update Job_Meta set Job_Meta.slack_user='MISSING'" + \
                  " where jobID='" + str(jobData.jobID) + "';"
                  
        try:
            # Update the owner of the job, regardless of whatever options were filled.
            self.conn.execute(sqlCmd1)
            self.db.commit()
            jobData.owner = str(newOwner)
        except:
            jobData.errMsg = "ERROR: Failure to update new owner for: " + str(newOwner)
            raise
            
        if changeFlag != 0:
            if len(newEmail) != 0:
                try:
                    self.conn.execute(sqlCmd2)
                    self.db.commit()
                    self.conn.execute(sqlCmd7)
                    self.db.commit()
                    self.conn.execute(sqlCmd8)
                    self.db.commit()
                    self.conn.execute(sqlCmd9)
                    self.db.commit()
                except:
                    jobData.errMsg = "ERROR: Failure to update email for: " + str(newOwner)
                    raise
                jobData.email = str(newEmail)
                jobData.slChan = None
                jobData.slToken = None
                jobData.slUser = None
                jobData.slackObj = None
                
            if len(newSlackChannel) != 0:
                try:
                    self.conn.execute(sqlCmd3)
                    self.db.commit()
                    self.conn.execute(sqlCmd4)
                    self.db.commit()
                    self.conn.execute(sqlCmd5)
                    self.db.commit()
                    self.conn.execute(sqlCmd6)
                    self.db.commit()
                except:
                    jobData.errMsg = "ERROR: Failure to update Slack information for: " + str(newOwner)
                    raise
                jobData.email = None
                jobData.slChan = str(newSlackChannel)
                jobData.slToken = str(newSlackToken)
                jobData.slUser = str(newSlackUName)
                jobData.slackObj = Slacker(str(jobData.slToken))
                
    def updateSpinupStatus(self,jobData):
        """
        Generic function to update the status of the spinup for a particular job.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
        
        sqlCmd = "update Job_Meta set Job_Meta.su_complete='" + str(jobData.spinComplete) + \
                 "' where jobID='" + str(jobData.jobID) + "';"
                 
        try:
            self.conn.execute(sqlCmd)
            self.db.commit()
        except:
            jobData.errMsg = "ERROR: Failure to update spinup status for job ID: " + str(jobData.jobID)
            raise
            
    def enterCalibParms(self,jobData,calibTbl):
        """
        Generic function to enter model parameter values being calibrated, along
        with their default, min, and max values.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
            
        # Open parameter table and read values in.
        tblData = pd.read_csv(calibTbl)
        if len(tblData) != 14:
            jobData.errMsg = "ERROR: Unexpected calibration parameter table format."
            raise Exception()
            
        for entry in range(0,len(tblData)):
            flag = tblData.calib_flag[entry]
            jobID = int(jobData.jobID)
            paramName = str(tblData.parameter[entry])
            defaultValue = str(tblData.ini[entry])
            minValue = str(tblData.min[entry])
            maxValue = str(tblData.max[entry])
            if flag == 1:
                sqlCmd = "insert into Job_Params (jobID,param,defaultValue,min,max) " + \
                         "values ('%s','%s','%s','%s','%s');" % (jobID,paramName,defaultValue,minValue,maxValue)

            try:
                self.conn.execute(sqlCmd)
                self.db.commit()
            except:
                jobData.errMsg = "ERROR: Unable to enter calibration parameter information for parameter: " + paramName
                raise