# Module file containing objects and functions for handling interactions
# with the database on workflow monitoring and status updates. 

# Logan Karsten
# National Center for Atmospheric Research

from glob import glob
import os
import pwd
import subprocess
import pandas as pd
import datetime

class statusMeta:
    def __init__(self):
        # Initialize empty object containing variables.
        self.jobID = []
        self.jobDir = []
        self.nGages = []
        self.gages = []
        self.gageIDs = []
        self.bSpinDate = []
        self.eSpinDate = []
        self.spinComplete = []
        self.bCalibDate = []
        self.eCalibDate = []
        self.bCalibEvalDate = []
        self.calibIter = []
        self.calibComplete = []
        self.bValidDate = []
        self.eValidDate = []
        self.bValidEvalDate = []
        self.validComplete = []
        self.nIter = []
        self.nCoresMod = []
        self.nCoresR = []
        self.acctKey = []
        self.exe = []
        self.errMsg = []
        self.genMsg = []
        self.dbUName = []
        self.dbPwd = []
        self.owner = []
        self.email = None
        self.slChan = None
        self.slToken = None
        self.slUser = None
        self.slackObj = None
    def checkGages(self,db):
        # Function to check number of gages in output directory. Function
        # also calls the database module to extract unique ID values for each
        # gage.
        subDirs = glob(self.jobDir + '/*/')
        
        if len(subDirs) != int(self.nGages):
            self.errMsg = "ERROR: Number of gages sub-directories does not match entry in database."
            raise Exception()
            
        gagesTmp = []
        gageIDsTmp = []
            
        # Walk job directory and extract gages.
        for subDir in range(0,len(subDirs)):
            pthTmp = subDirs[subDir]
            strArr = pthTmp.split('/')
            lenTmp = len(strArr)
            gagesTmp.append(strArr[lenTmp-2])
            try:
                gageID = db.getDomainID(self,strArr[lenTmp-2])
            except:
                raise
            gageIDsTmp.append(gageID)
            
        self.gages = gagesTmp[:]
        self.gageIDs = gageIDsTmp[:]
        
def checkYsJobs(jobData):
    # Function to obtain a data frame containing Yellowstone
    # jobs being ran under the owner of the JobID.

    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
        raise
    
    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    try:
        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    except:
        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
        raise
        
    lenJobs = len(jobs.JOBID)
    
    # Loop through data frame. For jobs across multiple cores, the data frame
    # needs to be filled in as the duplicate cores have NaN values, except for the
    # first core.
    for job in range(0,lenJobs):
        # Assume a NaN value with the "USER" field means this is a duplicate.
        jobIdTmp = jobs.JOBID[job]
        userTmp = jobs.USER[job]
        statTmp = jobs.STAT[job]
        queTmp = jobs.QUEUE[job]
        hostTmp = jobs.FROM_HOST[job]
        jobNameTmp = jobs.JOB_NAME[job]
        monthTmp = jobs.SUBMIT_MONTH[job]
        dayTmp = jobs.SUBMIT_DAY[job]
        hourTmp = jobs.SUBMIT_HHMM[job]
        
        if str(userTmp) != 'nan' and str(userTmp) != 'NaN':
            jobIdHold = jobIdTmp
            userHold = userTmp
            statHold = statTmp
            queHold = queTmp
            hostHold = hostTmp
            jobNameHold = jobNameTmp
            monthHold = monthTmp
            dayHold = dayTmp
            hourHold = hourTmp
        else:
            jobs.JOBID[job] = jobIdHold
            jobs.USER[job] = userHold
            jobs.STAT[job] = statHold
            jobs.QUEUE[job] = queHold
            jobs.FROM_HOST[job] = hostHold
            jobs.EXEC_HOST[job] = userTmp
            jobs.JOB_NAME[job] = jobNameHold
            jobs.SUBMIT_MONTH[job] = monthHold
            jobs.SUBMIT_DAY[job] = dayHold
            jobs.SUBMIT_HHMM[job] = hourHold
            
    # Delete temporary CSV file
    try:
        os.remove(csvPath)
    except:
        jobData.errMsg = "ERROR: Failure to remove: " + csvPath
        raise

    # Loop through and check to make sure no existing jobs are being ran for any 
    # of the gages.
    if len(jobs) != 0:
        for gageCheck in range(0,len(jobData.gageIDs)):
            jobNameCheck = "NWM_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageCheck])
            jobNameCheck2 = "NWM_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageCheck])
            testDF = jobs.query("JOB_NAME == '" + jobNameCheck + "'")
            if len(testDF) != 0:
                jobData.errMsg = "ERROR: Job ID: " + str(jobData.jobID) + \
                                 " is already being ran under owner: " + \
                                 str(jobData.owner) + ". User: " + \
                                 str(userTmp) + " is attempting to initiate the workflow."
                print "ERROR: You are attempting to initiate a job that is already being " + \
                      "ran by user: " + str(jobData.owner)
                raise Exception()
            testDF = jobs.query("JOB_NAME == '" + jobNameCheck2 + "'")
            if len(testDF) != 0:
                jobData.errMsg = "ERROR: Job ID: " + str(jobData.jobID) + \
                                 " is already being ran under owner: " + \
                                 str(jobData.owner) + ". User: " + \
                                 str(userTmp) + " is attempting to initiate the workflow."
                print "ERROR: You are attempting to initiate a job that is already being " + \
                      "ran by user: " + str(jobData.owner)
                raise Exception()
                
def checkBasJob(jobData,gageNum):
    """
    Generic function to check Yellowstone for job being ran for a particular basin.
    Job name follows a prescribed format:
    NWM_JOBID_DOMAINID where:
    JOBID = Unique job ID pulled from database.
    DOMAINID = Unique domain ID pulled from database.
    There should be a unique number of nodes found to be running a basin:
    integer(number_cores/16.0 cores/node)
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
    
    csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
        raise
    
    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    try:
        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    except:
        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
        raise
        
    # Delete temporary CSV file
    try:
        os.remove(csvPath)
    except:
        jobData.errMsg = "ERROR: Failure to remove: " + csvPath
        raise
        
    # Compile expected job name that the job should occupy.
    expName = "NWM_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    
    lenJobs = len(jobs.JOBID)

    # Assume no jobs for basin are being ran, unless found in the data frame.
    status = False
    
    if lenJobs == 0:
        status = False
    else:
        # Find if any jobs for this basin are being ran.
        testDF = jobs.query("JOB_NAME == '" + expName + "'")
        if len(testDF) != 0:
            status = True
            
    return status
    
def walkMod(bDate,eDate,runDir):
    """
    Generic function to walk a simulation directory, and determine where the model
    last left off. This is for when the model needs to be restarted, or if it crashed
    and the parent program needs to determine where it can try to restart.
    """
    
    dt = eDate - bDate
    nHours = int((dt.days*24*3600 + dt.seconds)/3600.0)
    
    bDateOrig = bDate
    
    # Initialize flag returned to user as True. Assume model needs to ran.
    runFlag = True
    
    output = []
    #print bDate
    #print eDate
    for hourModel in range(0,nHours+1):
        dCurrent = bDateOrig + datetime.timedelta(seconds=3600.0*hourModel)
        #print dCurrent
        lsmRestartPath = runDir + "/RESTART." + dCurrent.strftime('%Y%m%d%H') + "_DOMAIN1"
        hydroRestartPath = runDir + "/HYDRO_RST." + dCurrent.strftime('%Y-%m-%d_%H') + ':00_DOMAIN1'
        
        if os.path.isfile(lsmRestartPath) and os.path.isfile(hydroRestartPath):
            bDate = dCurrent
            #if hourModel == 0:
            #    # This implies the first time step of output is present. Get the expected
            #    # file size. This will be used to check to make sure the files present
            #    # are complete.
            #    rstPth1 = runDir + "/RESTART." + bDateRstChck.strftime('%Y%m%d%H') + "_DOMAIN1"
            #    rstPth2 = runDir + "/HYDRO_RST." + bDateRstChck.strftime('%Y-%m-%d_%H') + ':00_DOMAIN1'
            #    
            #    lsmSize = os.path.getsize(rstPth1)
            #    hydroSize = os.path.getsize(rstPth2)
            #    
            #    countTmp = countTmp + 1
            #if hourModel >= 1:
            #    checkLsm = os.path.getsize(lsmRestartPath)
            #    checkHydro = os.path.getsize(hydroRestartPath)
            #    if checkLsm == lsmSize and checkHydro == hydroSize:
            #        bDate = dCurrent
            
    #print bDate
    #print eDate
    # If the bDate has reached the eDate, this means the model completed as expected.
    if bDate == eDate:
        runFlag = False
        
    output.append(bDate)
    output.append(eDate)
    output.append(runFlag)
    return output
    
def checkCalibJob(jobData,gageNum):
    """
    Generic function to check Yellowstone for calibration R job being ran for a 
    particular basin for a particular job.
    Job name follows a prescribed format:
    NWM_CALIBRATION_JOBID_DOMAINID where:
    JOBID = Unique job ID pulled from database.
    DOMAINID = Unique domain ID pulled from database.
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
    
    csvPath = jobData.jobDir + "/BJOBS_CALIB_LISTING_" + str(pidUnique) + ".csv"
    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
        raise
    
    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    try:
        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    except:
        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
        raise
        
    # Delete temporary CSV file
    try:
        os.remove(csvPath)
    except:
        jobData.errMsg = "ERROR: Failure to remove: " + csvPath
        raise
        
    # Compile expected job name that the job should occupy.
    expName = "NWM_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    
    lenJobs = len(jobs.JOBID)

    # Assume no jobs for basin are being ran, unless found in the data frame.
    status = False
    
    if lenJobs == 0:
        status = False
    else:
        # Find if any jobs for this basin are being ran.
        testDF = jobs.query("JOB_NAME == '" + expName + "'")
        if len(testDF) != 0:
            status = True
            
    return status