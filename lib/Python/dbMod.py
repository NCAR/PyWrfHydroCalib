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
import os

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
                 "su_complete,date_calib_start,date_calib_end,date_calib_start_eval,num_iter," + \
                 "iter_complete,calib_complete,valid_start_date,valid_end_date,valid_start_date_eval," + \
                 "valid_complete,acct_key,num_cores_model,num_cores_R,exe,num_gages,owner,email," + \
                 "slack_channel,slack_token,slack_user) values " + \
                 "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (jobDir,jobData.bSpinDate.strftime('%Y-%m-%d'),\
                 jobData.eSpinDate.strftime('%Y-%m-%d'),0,jobData.bCalibDate.strftime('%Y-%m-%d'),\
                 jobData.eCalibDate.strftime('%Y-%m-%d'),jobData.bCalibEvalDate.strftime('%Y-%m-%d'),\
                 jobData.nIter,0,0,jobData.bValidDate.strftime('%Y-%m-%d'),\
                 jobData.eValidDate.strftime('%Y-%m-%d'),jobData.bValidEvalDate.strftime('%Y-%m-%d'),\
                 0,jobData.acctKey,jobData.nCoresMod,jobData.nCoresR,jobData.exe,len(jobData.gages),\
                 jobData.owner,emailStr,slStr1,slStr2,slStr3)
        
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
        gageNames = []
        gageIDs = []
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
            gageIDs.append(results[gage][0])
            gageNames.append(results[gage][1])
        
        listOut.append(gageIDs)
        listOut.append(gageNames)
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
        tmpMeta['comID'] = results[2]
        tmpMeta['geoFile'] = results[13]
        tmpMeta['wrfInput'] = results[14]
        tmpMeta['soilFile'] = results[15]
        tmpMeta['fullDomFile'] = results[16]
        tmpMeta['rtLnk'] = results[17]
        tmpMeta['udMap'] = results[18]
        tmpMeta['gwFile'] = results[19]
        tmpMeta['lkFile'] = results[20]
        tmpMeta['forceDir'] = results[21]
        tmpMeta['obsFile'] = results[22]
        
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
        jobData.bCalibEvalDate = datetime.datetime.strptime(str(results[7]),'%Y-%m-%d')
        jobData.nIter = int(results[8])
        jobData.calibIter = int(results[9])
        jobData.calibComplete = int(results[10])
        jobData.bValidDate = datetime.datetime.strptime(str(results[11]),'%Y-%m-%d')
        jobData.eValidDate = datetime.datetime.strptime(str(results[12]),'%Y-%m-%d')
        jobData.eValidEvalDate = datetime.datetime.strptime(str(results[13]),'%Y-%m-%d')
        jobData.validComplete = int(results[14])
        jobData.acctKey = results[15]
        jobData.nCoresMod = int(results[16])
        jobData.nCoresR = int(results[17])
        jobData.exe = results[18]
        jobData.nGages = int(results[19])
        jobData.owner = results[20]
        jobData.email = results[21]
        jobData.slChan = results[22]
        jobData.slToken = results[23]
        jobData.slUser = results[24]
        
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
            
    def updateCalibStatus(self,jobData):
        """
        Generic function to update the status of the calibration for a particular job.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
        
        sqlCmd = "update Job_Meta set Job_Meta.calib_complete='" + str(jobData.calibComplete) + \
                 "' where jobID='" + str(jobData.jobID) + "';"
                 
        try:
            self.conn.execute(sqlCmd)
            self.db.commit()
        except:
            jobData.errMsg = "ERROR: Failure to update calibration status for job ID: " + str(jobData.jobID)
            raise
            
    def updateValidationStatus(self,jobData):
        """
        Generic function to update the status of the validation for a particular job.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
        
        sqlCmd = "update Job_Meta set Job_Meta.valid_complete='" + str(jobData.validComplete) + \
                 "' where jobID='" + str(jobData.jobID) + "';"
                 
        try:
            self.conn.execute(sqlCmd)
            self.db.commit()
        except:
            jobData.errMsg = "ERROR: Failure to update validation status for job ID: " + str(jobData.jobID)
            raise
    
    def enterCalibParms(self,jobData,calibTbl):
        """
        Generic function to enter model parameter values being calibrated, along
        with their default, min, and max values. This is done one time.
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
            minValue = str(tblData.minValue[entry])
            maxValue = str(tblData.maxValue[entry])
            if flag == 1:
                sqlCmd = "insert into Job_Params (jobID,param,defaultValue,min,max) " + \
                         "values ('%s','%s','%s','%s','%s');" % (jobID,paramName,defaultValue,minValue,maxValue)

                try:
                    self.conn.execute(sqlCmd)
                    self.db.commit()
                except:
                    jobData.errMsg = "ERROR: Unable to enter calibration parameter information for parameter: " + paramName
                    raise
                    
    def populateParmTable(self,jobData,calibTbl):
        """
        Generic function to create an empty table that will store calibrated parameter
        values (or adjustments) for each basin, for each calibration iteration,
        for each parameter.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
            
        jobID = int(jobData.jobID)
        numIter = int(jobData.nIter)
        nBas = int(len(jobData.gages))
        
        # Read in CSV file containing parameters being calibrated.
        baseParms = pd.read_csv(calibTbl)
        baseParms = baseParms[baseParms['calib_flag'] == 1]
        baseParms = baseParms.reset_index()
        nParms = len(baseParms)
        
        for iteration in range(1,numIter+1):
            for basin in range(0,nBas):
                for parm in range(0,nParms):
                    domainID = int(jobData.gageIDs[basin])
                    parmName = str(baseParms.parameter[parm])
                    itStr = str(iteration)
                    gageStr = str(jobData.gages[basin])
                    # First determine if table row has already been created.
                    sqlCmd = "select * from Calib_Params where jobID='" + str(jobID) + "'" + \
                             " and domainID='" + str(domainID) + "'" + " and iteration='" + \
                             itStr + "'" + " and paramName='" + parmName + "';"
                    try:
                        self.conn.execute(sqlCmd)
                        results = self.conn.fetchone()
                    except:
                        jobData.errMsg = "ERROR: Unable to extract calibration parameter information for " + \
                                         "job ID: " + str(jobID) + " gage: " + gageStr + \
                                         " iteration: " + itStr + " parameter: " + parmName
                        raise
                        
                    if not results:
                        # Create "empty" entry into table.
                        sqlCmd = "insert into Calib_Params (jobID,domainID,iteration,paramName,paramValue) " + \
                                 "values (" + str(jobID) + "," + str(domainID) + "," + \
                                 str(iteration) + ",'" + parmName + "',-9999);"
                        try:
                            self.conn.execute(sqlCmd)
                            self.db.commit()
                        except:
                            jobData.errMsg = "ERROR: Unable to create empty calibration parameter information for " + \
                                             "job ID: " + str(jobID) + " gage: " + gageStr + \
                                             " iteration: " + itStr + " parameter: " + parmName
                            raise
                    
    def populateCalibTable(self,jobData,domainID,gageName):
        """
        Generic function to create empty table rows that will store calibration 
        information for each iteration, for each basin, for each job. This information
        will be updated as the calibration workflow progresses.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
            
        jobID = int(jobData.jobID)
        numIter = int(jobData.nIter)
        
        for iteration in range(1,numIter+1):
            # First determine if table row has already been created.
            sqlCmd = "select * from Calib_Stats where jobID='" + str(jobID) + "'" + \
                     " and domainID='" + str(domainID) + "'" + " and iteration='" + \
                     str(iteration) + "';"
            try:
                self.conn.execute(sqlCmd)
                results = self.conn.fetchone()
            except:
                jobData.errMsg = "ERROR: Unable to extract calib stats for job ID: " + str(jobID) + \
                                 " domainID: " + str(domainID) + " Iteration: " + str(iteration)
                raise
            
            if not results:
                # Create "empty" entry into table.
                sqlCmd = "insert into Calib_Stats (jobID,domainID,iteration,objfnVal,bias,rmse," + \
                         "cor,nse,nselog,kge,fdcerr,best,complete) values (" + str(jobID) + \
                         "," + str(domainID) + "," + str(iteration) + ",-9999,-9999,-9999," + \
                         "-9999,-9999,-9999,-9999,-9999,0,0);"
                try:
                    self.conn.execute(sqlCmd)
                    self.db.commit()
                except:
                    jobData.errMsg = "ERROR: Unable to create empty table entry into Calib_Stats for " + \
                                     "job ID: " + str(jobID) + " domainID: " + str(domainID) + \
                                     " iteration: " + str(iteration)
                    raise
                    
    def iterationStatus(self,jobData,domainID,iteration,gageName):
        """
        Generic function to extract the complete status for a given iteration of 
        a calibration in a given basin for a given job.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
            
        jobID = int(jobData.jobID)
        iterCheck = iteration + 1
        
        sqlCmd = "select complete from Calib_Stats where jobID='" + str(jobID) + "'" + \
                 " and domainID='" + str(domainID) + "'" + " and iteration='" + \
                 str(iterCheck) + "';"
        try:
            self.conn.execute(sqlCmd)
            results = self.conn.fetchone()
        except:
            jobData.errMsg = "ERROR: Unable to extract calibration status for job ID: " + str(jobID) + \
                             " domainID: " + str(domainID) + " Iteration: " + str(iteration)
            raise
            
        return float(results[0])
        
    def logCalibParams(self,jobData,jobID,domainID,calibTbl,iteration):
        """
        Generic function for logging newly created parameter values created
        by R into the database Calib_Params table.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
            
        if not os.path.isfile(calibTbl):
            jobData.errMsg = "ERROR: Expected calibration table: " + calibTbl + " not found."
            raise
            
        # Read in parameter table.
        try:
            tblData = pd.read_csv(calibTbl,sep=' ')
        except:
            jobData.errMsg = "ERROR: Failure to read in table: " + calibTbl
            raise
            
        print tblData
        paramNames = list(tblData.columns.values)
        
        # Update parameter values in Calib_Params
        for paramName in paramNames:
            print paramNames
            sqlCmd = "update Calib_Params set Calib_Params.paramValue='" + str(tblData[paramName][0]) + \
                     "' where jobID='" + str(jobID) + "' and domainID='" + str(domainID) + \
                     "' and iteration='" + str(iteration) + "' and paramName='" + \
                     str(paramName) + "';"
            print sqlCmd
            try:
                self.conn.execute(sqlCmd)
                self.db.commit()
            except:
                jobData.errMsg = "ERROR: Failure to enter value for parameter: " + str(paramName) + \
                                 " jobID: " + str(jobID) + " domainID: " + str(domainID) + \
                                 " iteration: " + str(iteration)
                print jobData.errMsg
                raise
                
    def logCalibStats(self,jobData,jobID,domainID,iteration,statsTbl):
        """
        Generic function for entering calibration statistics into Calib_Stats to
        keep track of performance statistics for each calibration iteration.
        """
        if not self.connected:
            jobData.errMsg = "ERROR: No Connection to Database: " + self.dbName
            raise Exception()
            
        if not os.path.isfile(statsTbl):
            jobData.errMsg = "ERROR: Expected calibration table: " + statsTbl + " not found."
            raise
            
        # Read in table.
        try:
            tblData = pd.read_csv(statsTbl,sep=' ')
        except:
            jobData.errMsg = "ERROR: Failure to read in table: " + statsTbl
            raise
            
        # Update Calib_Stats table.
        # PLACEHOLDER TO UPDATE AS CALIB STATS GETS FINALIZED
        sqlCmd = "update Calib_Stats set Calib_Stats.objfnVal='" + str(9) + "', " + \
                 "Calib_Stats.bias='" + str(9) + "', Calib_Stats.rmse='" + \
                 str(9) + "', Calib_Stats.cor='" + str(9) + "', Calib.Stats.nse='" + \
                 str(9) + "', Calib_Stats.nselog='" + str(9) + "', Calib.Stats.kge='" + \
                 str(9) + "', Calib_Stats.fdcerr='" + str(9) + \
                 "', Calib_Stats.complete='1' where jobID='" + str(jobID) + "' and " + \
                 "domainID='" + str(domainID) + "' and iteration='" + str(iteration) + \
                 "';"
                 
        try:
            self.conn.execute(sqlCmd)
            self.db.commit()
        except:
            jobData.errMsg = "ERROR: Failure to enter calibration statistics for jobID: " + \
                             str(jobID) + " domainID: " + str(domainID) + " iteration: " + \
                             str(iteration)
            raise
        
        if int(tblData.best[0]) == 1:
            # First reset iteration where best currently is to 0
            sqlCmd = "update Calib_Stats set Calib_Stats.best='0' where best='1';"
            
            try:
                self.conn.execute(sqlCmd)
                self.db.commit()
            except:
                jobData.errMsg = "ERROR: Failure to downgrade 'best' status of previous " + \
                                 "calibration iteration for jobID: " + str(jobID) + \
                                 " domainID: " + str(domainID) + " iteration: " + \
                                 str(iteration)
                raise
                
            # Now update this iteration to be the "best"
            sqlCmd = "update Calib_Stats set Calib_Stats.best='1' where jobID='" + \
                     str(jobID) + "' and domainID='" + str(domainID) + "' and " + \
                     "iteration='" + str(iteration) + "';"
            try:
                self.conn.execute(sqlCmd)
                self.db.commit()
            except:
                jobData.errMsg = "ERROR: Failure to upgrade 'best' status for jobID: " + \
                                 str(jobID) + " domainID: " + str(domainID) + \
                                 " iteration: " + str(iteration)
                raise
        