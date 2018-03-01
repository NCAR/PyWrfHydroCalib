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
import psutil

import warnings
warnings.filterwarnings("ignore")

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
        self.nSensSample = []
        self.nSensIter = []
        self.nSensBatch = []
        self.bSensDate = []
        self.eSensDate = []
        self.bSensEvalDate = []
        self.sensComplete = []
        self.nIter = []
        self.nCoresMod = []
        self.nNodesMod = []
        self.nCoresR = []
        self.nNodesR = []
        self.sensFlag = []
        self.sensTbl = []
        self.calibFlag = []
        self.calibTbl = []
        self.dailyAnalysis = []
        self.jobRunType = []
        self.analysisRunType = []
        self.host = []
        self.port = []
        self.acctKey = []
        self.queName = []
        self.queNameAnalysis = []
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
        
def checkBasJob(jobData,gageNum):
    """
    Generic function to check the status of a model run. If we are running BSUB/QSUB/Slurm,
    we will check the que for a specific job name following the format: WH_JOBID_DOMAINID
    where JOBID = Unique job ID pulled from the database and DOMAINID is
    a unique domain ID pulled from the database. If we are running mpiexec/mpirun,
    we will be looking for instances of the model to be running in the format of
    wrf_hydro_JOBID_DOMAINID.exe. The number of instances should match the number
    of model cores specified in the config file. For QSUB/BSUB, the number of nodes
    being uses should also match the number of cores being used. 
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
    
    if jobData.jobRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    
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
                
    if jobData.jobRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
        
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
    
        if lenJobs == 0:
            print "NO MODEL SIMULATIONS FOUND"
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    print "MODEL SIMULATIONS FOUND"
                    status = True
                    
    if jobData.jobRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
        
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    print "MODEL SIMULATIONS FOUND"
                    status = True
        else:
            status = False
        
        if not status:
            print "NO MODEL SIMULATIONS FOUND"
            
    if jobData.jobRunType == 4 or jobData.jobRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are using mpiexec.
        pidActive = []
        exeName = "W" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) 
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
            print "NO MODEL SIMULATIONS FOUND"
        else:
            print "MODEL SIMULATIONS FOUND"
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
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
    for hourModel in range(0,nHours+1):
        dCurrent = bDateOrig + datetime.timedelta(seconds=3600.0*hourModel)
        lsmRestartPath = runDir + "/RESTART." + dCurrent.strftime('%Y%m%d%H') + "_DOMAIN1"
        hydroRestartPath = runDir + "/HYDRO_RST." + dCurrent.strftime('%Y-%m-%d_%H') + ':00_DOMAIN1'
        
        if os.path.isfile(lsmRestartPath) and os.path.isfile(hydroRestartPath):
            bDate = dCurrent
            
    # If the bDate has reached the eDate, this means the model completed as expected.
    if bDate == eDate:
        runFlag = False
        
    output.append(bDate)
    output.append(eDate)
    output.append(runFlag)
    return output
    
def checkCalibJob(jobData,gageNum):
    """
    Generic function to check for a calibration R job being ran for a 
    particular basin for a particular job.
    Job name follows a prescribed format:
    WH_CALIB_JOBID_DOMAINID where:
    JOBID = Unique job ID pulled from database.
    DOMAINID = Unique domain ID pulled from database.
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
    
    if jobData.analysisRunType == 1:
        csvPath = "./BJOBS_CALIB_LISTING_" + str(pidUnique) + ".csv"
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
        expName = "WH_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
        
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
    
        lenJobs = len(jobs.JOBID)

        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            testDF = jobs.query("JOB_NAME == '" + expName + "'")
            if len(testDF) != 0:
                status = True
                
    if jobData.analysisRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
               
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
        
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    status = True
                    
    if jobData.analysisRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
        
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    print "CALIB JOBS FOUND"
                    status = True
        else:
            status = False
        
        if not status:
            print "NO CALIB JOBS FOUND"
    
    if jobData.analysisRunType == 4 or jobData.analysisRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are running via mpiexec
        pidActive = []
        exeName = "C" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) 
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
            print "NO CALIB JOBS FOUND"
        else:
            print "CALIB JOBS FOUND"
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by : " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
                
    return status
    
def checkBasJobValid(jobData,gageNum,modRun):
    """
    Generic function to check for validation job being ran for a particular basin.
    Job name follows a prescribed format:
    WH_SIM_JOBID_DOMAINID where:
    SIM = Can either CTRL or BEST.
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
    
    if jobData.jobRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_" + str(modRun) + '_' + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
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
                
    if jobData.jobRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_" + str(modRun) + '_' + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    status = True
                    
    if jobData.jobRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_" + str(modRun) + '_' + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    print "BASIN VALID JOBS FOUND"
                    status = True
        else:
            status = False
        
        if not status:
            print "NO VALID MODEL JOBS FOUND"
                    
    if jobData.jobRunType == 4 or jobData.jobRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are running via mpiexec
        pidActive = []
        if modRun == "BEST":
            exeName = "WB" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) 
        if modRun == "CTRL":
            exeName = "WC" + str(jobData.jobID) + str(jobData.gageIDs[gageNum])
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
            print "NO VALID MODEL JOBS FOUND"
        else:
            print "BASIN VALID JOBS FOUND"
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
                
    
            
    return status

def checkParmGenJob(jobData,gageNum):
    """
    Generic Function to check for parameter generation jobs running. This applies
    mainly to the validation workflow. 
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
    
    if jobData.analysisRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_PARM_GEN_" + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
    
        lenJobs = len(jobs.JOBID)

        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            testDF = jobs.query("JOB_NAME == '" + expName + "'")
            if len(testDF) != 0:
                status = True
                
    if jobData.analysisRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_PARM_GEN_" + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    print "EVAL JOBS FOUND"
                    status = True
        else:
            status = False
        
        if not status:
            print "NO EVAL JOBS FOUND"
        
    if jobData.analysisRunType == 4 or jobData.analysisRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are running via mpiexec/mpirun
        pidActive = []
        exeName = "P" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) 
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
            print "NO EVAL JOBS FOUND"
        else:
            print "EVAL JOBS FOUND"
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
                
    if jobData.analysisRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        expName = "WH_PARM_GEN_" + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    status = True
            
    return status
    
def checkEvalJob(jobData,gageNum):
    """ 
    Generic function to check for jobs running that are evaluating both 
    a control and best simulation during the validation workflow.
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
    
    if jobData.analysisRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_EVAL_" + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        lenJobs = len(jobs.JOBID)

        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            testDF = jobs.query("JOB_NAME == '" + expName + "'")
            if len(testDF) != 0:
                status = True
                
    if jobData.analysisRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that job should occupy
        expName = "WH_EVAL_" + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    status = True
                    
    if jobData.analysisRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_EVAL_" + str(jobData.jobID) + "_" + \
                  str(jobData.gageIDs[gageNum])
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    print "EVAL JOBS FOUND"
                    status = True
        else:
            status = False
        
        if not status:
            print "NO EVAL JOBS FOUND"
                
    if jobData.analysisRunType == 4 or jobData.analysisRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are running via mpiexec
        pidActive = []
        exeName = "E" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) 
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
            print "NO EVAL JOBS FOUND"
        else:
            print "EVAL JOBS FOUND"
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
            
    return status

def checkSensPreProcJob(jobData,gageID):
    """ 
    Generic function to check for jobs running that are preparing the input
    parameter datasets for sensitivity analysis.
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
        
    if jobData.analysisRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + \
                  str(gageID)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        lenJobs = len(jobs.JOBID)

        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            testDF = jobs.query("JOB_NAME == '" + expName + "'")
            if len(testDF) != 0:
                status = True
                
    if jobData.analysisRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that job should occupy
        expName = "WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + \
                  str(gageID)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    status = True
                    
    if jobData.analysisRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + \
                  str(gageID)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    status = True
        else:
            status = False
        
    if jobData.analysisRunType == 4 or jobData.analysisRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are running via mpiexec
        pidActive = []
        exeName = "SPRE" + str(jobData.jobID) + str(gageID) 
        print exeName
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
        else:
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
            
    return status

def checkSensPostProcJob(jobData,gageID):
    """ 
    Generic function to check for jobs running that are post-processing sensitivity
    model output for analysis. 
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
        
    if jobData.analysisRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + \
                  str(gageID)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        lenJobs = len(jobs.JOBID)

        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            testDF = jobs.query("JOB_NAME == '" + expName + "'")
            if len(testDF) != 0:
                status = True
                
    if jobData.analysisRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])

        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that job should occupy
        expName = "WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + \
                  str(gageID)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    status = True
                    
    if jobData.analysisRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + \
                  str(gageID)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    status = True
        else:
            status = False
        
    if jobData.analysisRunType == 4 or jobData.analysisRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are running via mpiexec
        pidActive = []
        exeName = "SPOS" + str(jobData.jobID) + str(gageID) 
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
        else:
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
    
    return status

def checkBasSensJob(jobData,gageNum,iteration,runDir):
    """
    Generic function to check the status of a sensitivity model run. If we are running BSUB/QSUB/Slurm,
    we will check the que for a specific job name following the format: WH_JOBID_DOMAINID
    where JOBID = Unique job ID pulled from the database and DOMAINID is
    a unique domain ID pulled from the database. If we are running mpiexec/mpirun,
    we will be looking for instances of the model to be running in the format of
    wrf_hydro_JOBID_DOMAINID.exe. The number of instances should match the number
    of model cores specified in the config file. For QSUB/BSUB, the number of nodes
    being uses should also match the number of cores being used. 
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
    
    if jobData.jobRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WHS" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) + str(iteration)
    
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
                
    if jobData.jobRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WHS" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) + str(iteration)
        
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
    
        if lenJobs == 0:
            print "NO MODEL SIMULATIONS FOUND"
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    print "MODEL SIMULATIONS FOUND"
                    status = True
                    
    if jobData.jobRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WHS" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) + str(iteration)
        
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    print "MODEL SIMULATIONS FOUND"
                    status = True
        else:
            status = False
        
        if not status:
            print "NO MODEL SIMULATIONS FOUND"
            
    if jobData.jobRunType == 4 or jobData.jobRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are using mpiexec.
        pidActive = []
        exeName = "WHS" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) + str(iteration)
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
            print "NO MODEL SIMULATIONS FOUND"
        else:
            print "MODEL SIMULATIONS FOUND"
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
                
    return status

def checkSensCollectJob(jobData,gageID,iteration):
    """ 
    Generic function to check for jobs running that are collecting model output
    into an R dataset
    """
    
    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()
        
    if jobData.analysisRunType == 1:
        #csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
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
        
        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
                  str(gageID) + "_" + str(iteration)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        lenJobs = len(jobs.JOBID)

        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            testDF = jobs.query("JOB_NAME == '" + expName + "'")
            if len(testDF) != 0:
                status = True
                
    if jobData.analysisRunType == 2:
        # We are running via qsub
        csvPath = "./QSTAT_" + str(pidUnique) + ".csv"
        cmd = "qstat -f | grep 'Job_Name' > " + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe QSTAT output to: " + csvPath
            raise
                
        try:
            jobs = pd.read_csv(csvPath,header=None,sep='=')
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
        lenJobs = len(jobs[1])
            
        # Delete temporary CSV fies
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp,shell=True)
        
        # Compile expected job name that job should occupy
        expName = "WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
                  str(gageID) + "_" + str(iteration)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
                  
        if lenJobs == 0:
            status = False
        else:
            # Find if any jobs for this basin are being ran.
            for jobNum in range(0,lenJobs):
                if jobs[1][jobNum].strip() == expName:
                    status = True
                    
    if jobData.analysisRunType == 3:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise
            
        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise
            
        try:
            jobs = pd.read_csv(csvPath,delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise
            
        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd,shell=True)
        
        # Compile expected job name that the job should occupy.
        expName = "WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
                  str(gageID) + "_" + str(iteration)
                  
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        if len(jobs.NAME) > 0:
            for jobNum in range(0,len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    status = True
        else:
            status = False
        
    if jobData.analysisRunType == 4 or jobData.analysisRunType == 5:
        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False
        
        # We are running via mpiexec
        pidActive = []
        exeName = "SCOL" + str(jobData.jobID) + str(gageID) + str(iteration)
        for proc in psutil.process_iter():
            try:
                if proc.name() == exeName:
                    pidActive.append(proc.pid)
            except:
                print exeName + " Found, but ended before Python could get the PID."
        if len(pidActive) == 0:
            status = False
        else:
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
                userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise
            else:
                status = True
    
    return status