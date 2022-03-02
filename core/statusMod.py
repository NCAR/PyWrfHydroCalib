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
import math
import time
import shutil

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
        self.validCompleteCTRL = []
        self.validCompleteBEST = []
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
        self.nCoresPerNode = []
        self.mpiCmd = []
        self.cpuPinCmd = []
        self.sensFlag = []
        self.sensTbl = []
        self.calibFlag = []
        self.trouteFlag = []
        self.trouteConfig = []
        self.moduleLoadStr = []
        self.moduleLoadTrouteStr = []
        self.calibTbl = []
        self.dailyAnalysis = []
        self.coldStart = []
        self.jobRunType = []
        self.acctKey = []
        self.queName = []
        self.exe = []
        self.errMsg = []
        self.genMsg = []
        self.owner = []
        self.email = None
        self.slChan = None
        self.slToken = None
        self.slUser = None
        self.slackObj = None
        self.gSQL = []
        self.dbPath = []
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
        
    def checkGages2(self,db):
        #Function to extract domain ID values based on the SQL command placed into the
        #configuration file.
        gagesTmp = []
        gageIDsTmp = []

        try:
            gageInfo = db.getDomainID2(self)
        except:
            raise

        for gTmp in range(0,len(gageInfo)):
            gagesTmp.append(gageInfo[gTmp][1])
            gageIDsTmp.append(gageInfo[gTmp][0])

        self.gages = gagesTmp[:]
        self.gageIDs = gageIDsTmp[:]

    def calcGroupNum(self):
        """
        Function to calculate the number of basin groups based on the CPU layout provided
        by the user. This function also assigns a group number to each basin, along with
        a pre-determined beginning/ending CPU number based on the user-provided CPU layout
        informaiton in the configuration file.
        :return:
        """
        nCoresAvail = self.nCoresPerNode * self.nNodesMod
        self.numBasPerGroup = math.floor(nCoresAvail/self.nCoresMod)
        self.nGroups = math.ceil(len(self.gages)/self.numBasPerGroup)

        print(self.gages)
        print('NUM CORES PER NODE = ' + str(self.nCoresPerNode))
        print('NUM CORES AVAIL = ' + str(nCoresAvail))
        print('NUM BASINS PER GROUP = ' + str(self.numBasPerGroup))
        print('NUM BASINS = ' + str(len(self.gages)))
        print('NUM GROUPS = ' + str(self.nGroups))
        # Temporary arrays to calculate groups, CPU layout, etc
        gGroupTmp = []
        gBcpuTmp = []
        gEcpuTmp = []
        gCompleteTmp = []
        countTmp = 0

        for groupTmp in range(0,self.nGroups):
            begCpuTmpVal = 0
            endCpuTmpVal = 0
            # Initialize the complete flag for this group of basins to be 0. The
            # orchestrator program will set things to 1 if they are already complete.
            gCompleteTmp.append(0)
            for basinTmp in range(0,self.numBasPerGroup):
                if basinTmp == 0:
                    endCpuTmpVal = endCpuTmpVal + self.nCoresMod - 1
                else:
                    endCpuTmpVal = endCpuTmpVal + self.nCoresMod
                # Create CPU strides for each basin in this group.
                if basinTmp == 0:
                    begCpuTmpVal = begCpuTmpVal
                else:
                    begCpuTmpVal = begCpuTmpVal + self.nCoresMod
                gGroupTmp.append(groupTmp)
                gBcpuTmp.append(begCpuTmpVal)
                gEcpuTmp.append(endCpuTmpVal)

        self.gageGroup = gGroupTmp
        self.gageEndModelCpu = gEcpuTmp
        self.gageBegModelCpu = gBcpuTmp
        self.groupComplete = gCompleteTmp

    def backupDatabase(self, configMod, dbMod):
        """
        Generic function to backup the local sql lite database file to an hourly directory
        for a given hour. We first check to see if the proper backup directory for this hour
        exists. If not, we create it. If a LOCK file already exists, we assume another
        process is backing things up. If not, we backup our file, create a LOCK file to alert
        other processes of our actions. Once the file has successfully been backed up,
        we remove the LOCK file. If the LOCK file is older than 2 hours old, we assume the program
        failed during a backup, or something went wrong, and error out to the user.
        :param dbMod:
        :return:
        """
        if configMod.dbBackup == 0:
            return

        dCurrent = datetime.datetime.utcnow()
        # First check to see if the backup directory for this particular hour exists.
        backupDir = configMod.outDir + "/" + configMod.jobName + "/DB_BACKUP_" + dCurrent.strftime('%Y%m%d%H')
        if not os.path.isdir(backupDir):
            try:
                os.mkdir(backupDir)
            except:
                self.errMsg = "Unable to create database backup directory: " + backupDir
                raise Exception()

        backupFlag = False

        # Next, check to see if the database file is locked. If it is, check the lat modified
        # date on the file. If the last modified time on the LOCK file is greater than
        # two hours, we will throw an error to the user as there is assumes the LOCKING/backup
        # process is stale or orphaned since we are doing hourly backups.
        if os.path.isfile(dbMod.lockPath):
            modTime = datetime.datetime.fromtimestamp(os.path.getmtime(dbMod.lockPath))
            dtTmp = dCurrent - modTime
            if dtTmp.seconds > 7200.0:
                self.errMsg = "Database backup LOCK file: " + dbMod.lockPath + \
                              " is has not been modified in over two hours."
                raise Exception()
            else:
                # We are going to assume another process is backing the database file up.
                # Simply return to the main calling program.
                return
        else:
            # See if the final backup file is in place, along with the COMPLETE flag.
            finalPath = backupDir + "/wrfHydro_Calib_Backup.db"
            completeFlag = backupDir + "/wrfHydro_Calib_Backup.COMPLETE"

            if os.path.isfile(finalPath) and os.path.isfile(completeFlag):
                # Database has been backed up for this hour. Return to the main calling program.
                return

            if os.path.isfile(finalPath) and not os.path.isfile(completeFlag):
                # A previous backup did not complete. We will backup the file here. But first,
                # remove the old db file. This scenario would be unusual and rare.....
                try:
                    os.remove(finalPath)
                except:
                    self.errMsg = "Unable to remove stale database file: " + finalPath + \
                                  " during backup process."
                    raise Exception()

            if not os.path.isfile(finalPath) and os.path.isfile(completeFlag):
                # This is also another rare, unusual situation. We will remove the
                # COMPLETE flag and proceed to backup.
                try:
                    os.remove(completeFlag)
                except:
                    self.errMsg = "Unable to remove file: " + completeFlag
                    raise Exception()
                backupFlag = True

            if not os.path.isfile(finalPath) and not os.path.isfile(completeFlag):
                # We need to backup the database file....
                backupFlag = True

            if backupFlag:
                # create a LOCK file. This will prevent any other programs from touching the database
                # until successful backup. Once the backup is complete, remove the LOCK file and
                # create a COMPLETE flag for the backup directory.
                try:
                    open(dbMod.lockPath, 'a').close()
                except:
                    self.errMsg = "Unable to create LOCK file: " + dbMod.lockPath
                    raise Exception()

                # Wait 30 seconds to allow other processes to finish up writing to the database
                # file
                time.sleep(30)

                # Copy the database file to the final backup location.
                try:
                    shutil.copy(self.dbPath, finalPath)
                except:
                    self.errMsg = "Unable to copy: " + self.dbPath + " to: " + finalPath
                    raise Exception()

                # Create a complete flag
                try:
                    open(completeFlag, 'a').close()
                except:
                    self.errMsg = "Unable to create complete flag: " + completeFlag
                    raise Exception()

                # Remove the LOCK file
                try:
                    os.remove(dbMod.lockPath)
                except:
                    self.errMsg = "Unable to remove LOCK file: " + dbMod.lockPath
                    raise Exception()
        
def checkBasJob(jobData,gageNum,pbsJobId):
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
    
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
                
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
    
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                print("MODEL SIMULATIONS FOUND")
    #                status = True
    #    else:
    #        status = False
    #
    #    if not status:
    #        print("NO MODEL SIMULATIONS FOUND")
            
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
    # Assume no jobs for basin are being ran, unless found in the data frame.
    status = False
        
    pidActive = []
    exeName = "W" + str(jobData.jobID) + str(jobData.gageIDs[gageNum])
    for proc in psutil.process_iter():
        try:
            if proc.name() == exeName:
                pidActive.append(proc.pid)
        except:
            print(exeName + " Found, but ended before Python could get the PID.")
    if len(pidActive) == 0:
        status = False
        print("NO MODEL SIMULATIONS FOUND")
    else:
        print("MODEL SIMULATIONS FOUND")
        # Ensure these are being ran by the proper user.
        proc_stat_file = os.stat('/proc/%d' % pidActive[0])
        uid = proc_stat_file.st_uid
        userCheck = pwd.getpwuid(uid)[0]
        if userCheck != str(jobData.owner):
            jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
            userCheck + " When it should be ran by: " + jobData.owner
            status = False
            raise Exception()
        else:
            status = True
            
    return status
   

def walkModTroute(bDate,eDate,runDir,yamlDict): 
    """
    Generic function to walk a simulation directory, and determine where the model
    last left off. This is for when the TROUTE model needs to be restarted, or if it crashed
    and the parent program needs to determine where it can try to restart.
    """
    dt = eDate - bDate
    nLoops = int((dt.days*24)/yamlDict['compute_parameters']['forcing_parameters']['max_loop_size'])
    rem = (dt.days*24)%yamlDict['compute_parameters']['forcing_parameters']['max_loop_size']
    if(nLoops < 0):
        nLoops = 0
    
    bDateOrig = bDate

    # Initialize flag returned to user as True. Assume model needs to ran.
    runFlag = True

    output = []
    for loopModel in range(0,nLoops+1):
        dCurrent = bDateOrig + datetime.timedelta(hours=loopModel*yamlDict['compute_parameters']['forcing_parameters']['max_loop_size'])
        trouteRestartPath = runDir + "/channel_restart_" + dCurrent.strftime('%Y%m%d%H%M')

        if os.path.isfile(trouteRestartPath):
            bDate = dCurrent

    # If the bDate has reached the eDate, this means the model completed as expected.
    if rem > 0:
        if(bDate + datetime.timedelta(hours=rem) == eDate):
            dCurrent = bDate + datetime.timedelta(hours=rem)
            trouteRestartPath = runDir + "/channel_restart_" + dCurrent.strftime('%Y%m%d%H%M')
            if os.path.isfile(trouteRestartPath):
                bDate = dCurrent

    if bDate == eDate:
        runFlag = False

    output.append(bDate)
    output.append(eDate)
    output.append(runFlag)
    return output
    
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
    
def checkCalibJob(jobData,gageNum,pbsJobId):
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
    
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_CALIB_LISTING_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV file
    #    try:
    #        os.remove(csvPath)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to remove: " + csvPath
    #        raise
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
    #
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
    #
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_CALIB_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                print("CALIB JOBS FOUND")
    #                status = True
    #    else:
    #        status = False
    #
    #    if not status:
    #        print("NO CALIB JOBS FOUND")
    
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
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
            print(exeName + " Found, but ended before Python could get the PID.")
    if len(pidActive) == 0:
        status = False
        print("NO CALIB JOBS FOUND")
    else:
        print("CALIB JOBS FOUND")
        # Ensure these are being ran by the proper user.
        proc_stat_file = os.stat('/proc/%d' % pidActive[0])
        uid = proc_stat_file.st_uid
        userCheck = pwd.getpwuid(uid)[0]
        if userCheck != str(jobData.owner):
            jobData.errMsg = "ERROR: " + exeName + " is being ran by : " + \
            userCheck + " When it should be ran by: " + jobData.owner
            status = False
            raise Exception()
        else:
            status = True
                
    return status
    
def checkBasJobValid(jobData,gageNum,modRun,pbsJobId):
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
    
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_" + str(modRun) + '_' + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
    #
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_" + str(modRun) + '_' + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
                  
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_" + str(modRun) + '_' + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                print("BASIN VALID JOBS FOUND")
    #                status = True
    #    else:
    #        status = False
    #
    #    if not status:
    #        print("NO VALID MODEL JOBS FOUND")
                    
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
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
            print(exeName + " Found, but ended before Python could get the PID.")
    if len(pidActive) == 0:
        status = False
        print("NO VALID MODEL JOBS FOUND")
    else:
        print("BASIN VALID JOBS FOUND")
        # Ensure these are being ran by the proper user.
        proc_stat_file = os.stat('/proc/%d' % pidActive[0])
        uid = proc_stat_file.st_uid
        userCheck = pwd.getpwuid(uid)[0]
        if userCheck != str(jobData.owner):
            jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
            userCheck + " When it should be ran by: " + jobData.owner
            status = False
            raise Exception()
        else:
            status = True
                
    
            
    return status

def checkParmGenJob(jobData,gageNum,pbsJobId):
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
    
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_PARM_GEN_" + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
                
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_PARM_GEN_" + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                print("EVAL JOBS FOUND")
    #                status = True
    #    else:
    #        status = False
    #
    #    if not status:
    #        print("NO EVAL JOBS FOUND")
        
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
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
            print(exeName + " Found, but ended before Python could get the PID.")
    if len(pidActive) == 0:
        status = False
        print("NO EVAL JOBS FOUND")
    else:
        print("EVAL JOBS FOUND")
        # Ensure these are being ran by the proper user.
        proc_stat_file = os.stat('/proc/%d' % pidActive[0])
        uid = proc_stat_file.st_uid
        userCheck = pwd.getpwuid(uid)[0]
        if userCheck != str(jobData.owner):
            jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
            userCheck + " When it should be ran by: " + jobData.owner
            status = False
            raise Exception()
        else:
            status = True
                
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    expName = "WH_PARM_GEN_" + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status

    return status
    
def checkEvalJob(jobData,gageNum,pbsJobId):
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
    
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_EVAL_" + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
    #
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that job should occupy
    #    expName = "WH_EVAL_" + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
    #
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_EVAL_" + str(jobData.jobID) + "_" + \
    #              str(jobData.gageIDs[gageNum])
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                print("EVAL JOBS FOUND")
    #                status = True
    #    else:
    #        status = False
    #
    #    if not status:
    #        print("NO EVAL JOBS FOUND")

    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
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
            print(exeName + " Found, but ended before Python could get the PID.")
    if len(pidActive) == 0:
        status = False
        print("NO EVAL JOBS FOUND")
    else:
        print("EVAL JOBS FOUND")
        # Ensure these are being ran by the proper user.
        proc_stat_file = os.stat('/proc/%d' % pidActive[0])
        uid = proc_stat_file.st_uid
        userCheck = pwd.getpwuid(uid)[0]
        if userCheck != str(jobData.owner):
            jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
            userCheck + " When it should be ran by: " + jobData.owner
            status = False
            raise Exception()
        else:
            status = True
            
    return status

def checkSensPreProcJob(jobData,gageID,gageNum,pbsJobId):
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
        
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + \
    #              str(gageID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    lenJobs = len(jobs.JOBID)

    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
                
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that job should occupy
    #    expName = "WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + \
    #              str(gageID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
                  
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + \
    #              str(gageID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                status = True
    #    else:
    #        status = False
    #
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
    # Assume no jobs for basin are being ran, unless found in the data frame.
    status = False
        
    # We are running via mpiexec
    pidActive = []
    exeName = "SPRE" + str(jobData.jobID) + str(gageID)
    print(exeName)
    for proc in psutil.process_iter():
        try:
            if proc.name() == exeName:
                pidActive.append(proc.pid)
        except:
            print(exeName + " Found, but ended before Python could get the PID.")
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
            raise Exception()
        else:
            status = True
            
    return status

def checkSensPostProcJob(jobData,gageID,gageNum,pbsJobId):
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
        
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + \
    #              str(gageID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
    #
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that job should occupy
    #    expName = "WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + \
    #              str(gageID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
            
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + \
    #              str(gageID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                status = True
    #    else:
    #        status = False
        
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
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
            print(exeName + " Found, but ended before Python could get the PID.")
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
            raise Exception()
        else:
            status = True
    
    return status

def checkBasSensJob(jobData,gageNum,iteration,runDir,pbsJobId):
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
    
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WHS" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) + str(iteration)
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
    #
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that the job should occupy.
    #    expName = "WHS" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) + str(iteration)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum,iteration] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum,iteration] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum,iteration])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
    
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
        
    #    # Compile expected job name that the job should occupy.
    #    expName = "WHS" + str(jobData.jobID) + str(jobData.gageIDs[gageNum]) + str(iteration)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                print("MODEL SIMULATIONS FOUND")
    #                status = True
    #    else:
    #        status = False
    #
    #    if not status:
    #        print("NO MODEL SIMULATIONS FOUND")
    #
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
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
            print(exeName + " Found, but ended before Python could get the PID.")
    if len(pidActive) == 0:
        status = False
        print("NO MODEL SIMULATIONS FOUND")
    else:
        print("MODEL SIMULATIONS FOUND")
        # Ensure these are being ran by the proper user.
        proc_stat_file = os.stat('/proc/%d' % pidActive[0])
        uid = proc_stat_file.st_uid
        userCheck = pwd.getpwuid(uid)[0]
        if userCheck != str(jobData.owner):
            jobData.errMsg = "ERROR: " + exeName + " is being ran by: " + \
            userCheck + " When it should be ran by: " + jobData.owner
            status = False
            raise Exception()
        else:
            status = True
                
    return status

def checkSensCollectJob(jobData,gageID,iteration,gageNum,pbsJobId):
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
        
    #if jobData.jobRunType == 1:
    #    csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
    #    cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
    #        raise
    #
    #    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
    #               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files
    #    cmdTmp = 'rm -rf ' + csvPath
    #    subprocess.call(cmdTmp,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
    #              str(gageID) + "_" + str(iteration)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    lenJobs = len(jobs.JOBID)
    #
    #    if lenJobs == 0:
    #        status = False
    #    else:
    #        # Find if any jobs for this basin are being ran.
    #        testDF = jobs.query("JOB_NAME == '" + expName + "'")
    #        if len(testDF) != 0:
    #            status = True
    #
    #if jobData.jobRunType == 2:
    #    # We are running via qsub
    #    # Compile expected job name that job should occupy
    #    expName = "WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
    #              str(gageID) + "_" + str(iteration)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    # Run the qstat command on the username IF we are running this command
    #    # for the first time (restarting workflow, or new workflow instance).
    #    # Once jobs are fired off, we will populate the jobId array with the values
    #    # returned by qsub.
    #    if pbsJobId[gageNum,iteration] == -9999:
    #        # Run qstat for the user.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat','-u',jobData.owner])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #        except:
    #            jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
    #            raise
    #        if len(jobsTmp) == 0:
    #            # This means no jobs are running for the user. We can safely
    #            # assume the status is false.
    #            status = False
    #        else:
    #            numLinesTmp = len(jobsTmp.split('\n'))
    #            # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
    #            if numLinesTmp < 7:
    #                jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
    #                raise Exception()
    #            for lineNum in range(5,numLinesTmp):
    #                # This is a CRUDE assumption based on the behavior of qstat
    #                # on Cheyenne.
    #                lineTmp = jobsTmp.split('\n')[lineNum]
    #                if len(lineTmp) == 0:
    #                    continue
    #                else:
    #                    expCheck = lineTmp.split()[3]
    #                    if expCheck == expName:
    #                        # We have a match. This means a job running from a
    #                        # previous instance of the workflow is still running.
    #                        # Get the job id and set it into the jobIds array.
    #                        pbsJobId[gageNum,iteration] = int((lineTmp.split()[0]).split('.')[0])
    #                        status = True
    #    else:
    #        # We are checking for a job ID that has already been submitted by
    #        # this instance of the workflow.
    #        # Try running qstat for the job ID. If it's unsucessfull, then we
    #        # can make a good assumption that the job is no longer running.
    #        try:
    #            jobsTmp = subprocess.check_output(['qstat',str(pbsJobId[gageNum,iteration])])
    #            jobsTmp = str(jobsTmp,'utf-8')
    #            status = True
    #        except:
    #            # This means the job is no longer running.
    #            status = False
    #            return status
                  
    #if jobData.jobRunType == 3 or jobData.jobRunType == 6:
    #    # We are running via slurm
    #    csvPath = "./SLURM_" + str(pidUnique) + ".csv"
    #    cmd = "squeue -u " + str(jobData.owner) + \
    #          ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
    #          ' > ' + csvPath
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
    #        raise
    #
    #    if not os.path.isfile(csvPath):
    #        jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
    #        raise Exception()
    #
    #    try:
    #        jobs = pd.read_csv(csvPath,delim_whitespace=True)
    #    except:
    #        jobData.errMsg = "ERROR: Failure to read in: " + csvPath
    #        raise
    #
    #    # Delete temporary CSV files.
    #    cmdTmp = "rm -rf " + csvPath
    #    subprocess.call(cmd,shell=True)
    #
    #    # Compile expected job name that the job should occupy.
    #    expName = "WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
    #              str(gageID) + "_" + str(iteration)
    #
    #    # Assume no jobs for basin are being ran, unless found in the data frame.
    #    status = False
    #
    #    if len(jobs.NAME) > 0:
    #        for jobNum in range(0,len(jobs.NAME)):
    #            if jobs.NAME[jobNum].strip() == expName:
    #                status = True
    #    else:
    #        status = False
    #
    #if jobData.jobRunType == 4 or jobData.jobRunType == 5:
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
            print(exeName + " Found, but ended before Python could get the PID.")
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
            raise Exception()
        else:
            status = True
    
    return status


def checkBasGroupJob(jobData, groupNum, pbsJobId, programType):
    """
    Generic function to check the status of a basin group job.
    """

    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name

    if userTmp != str(jobData.owner):
        jobData.errMsg = "ERROR: you are not the owner of this job."
        raise Exception()

    if jobData.jobRunType == 1:
        csvPath = "./BJOBS_" + str(pidUnique) + ".csv"
        cmd = 'bjobs -u ' + str(jobData.owner) + ' -w -noheader > ' + csvPath
        try:
            subprocess.call(cmd, shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe BJOBS output to" + csvPath
            raise

        colNames = ['JOBID', 'USER', 'STAT', 'QUEUE', 'FROM_HOST', 'EXEC_HOST', 'JOB_NAME', \
                    'SUBMIT_MONTH', 'SUBMIT_DAY', 'SUBMIT_HHMM']
        try:
            jobs = pd.read_csv(csvPath, delim_whitespace=True, header=None, names=colNames)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise

        # Delete temporary CSV files
        cmdTmp = 'rm -rf ' + csvPath
        subprocess.call(cmdTmp, shell=True)

        # Compile expected job name that the job should occupy.
        expName = programType + "_" + str(jobData.jobID) + "_" + str(groupNum)

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
        # Compile expected job name that the job should occupy.
        expName = programType + "_" + str(jobData.jobID) + "_" + str(groupNum)

        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False

        # Run the qstat command on the username IF we are running this command
        # for the first time (restarting workflow, or new workflow instance).
        # Once jobs are fired off, we will populate the jobId array with the values
        # returned by qsub.
        if pbsJobId[groupNum] == -9999:
            # Run qstat for the user.
            try:
                jobsTmp = subprocess.check_output(['qstat', '-u', jobData.owner])
                jobsTmp = str(jobsTmp, 'utf-8')
            except:
                jobData.errMsg = "ERROR: Unable to run qstat for user: " + jobData.owner
                raise
            if len(jobsTmp) == 0:
                # This means no jobs are running for the user. We can safely
                # assume the status is false.
                status = False
            else:
                numLinesTmp = len(jobsTmp.split('\n'))
                # The exptected return from qstat on Cheyenne gives us at least 7 lines to parse.
                if numLinesTmp < 7:
                    jobData.errMsg = "ERROR: Expected qstat return should be greater than 6 lines."
                    raise Exception()
                for lineNum in range(5, numLinesTmp):
                    # This is a CRUDE assumption based on the behavior of qstat
                    # on Cheyenne.
                    lineTmp = jobsTmp.split('\n')[lineNum]
                    if len(lineTmp) == 0:
                        continue
                    else:
                        expCheck = lineTmp.split()[3]
                        if expCheck == expName:
                            # We have a match. This means a job running from a
                            # previous instance of the workflow is still running.
                            # Get the job id and set it into the jobIds array.
                            pbsJobId[groupNum] = int((lineTmp.split()[0]).split('.')[0])
                            status = True
        else:
            # We are checking for a job ID that has already been submitted by
            # this instance of the workflow.
            # Try running qstat for the job ID. If it's unsucessfull, then we
            # can make a good assumption that the job is no longer running.
            try:
                jobsTmp = subprocess.check_output(['qstat', str(pbsJobId[groupNum])])
                jobsTmp = str(jobsTmp, 'utf-8')
                status = True
            except:
                # This means the job is no longer running.
                status = False
                return status

    if jobData.jobRunType == 3 or jobData.jobRunType == 6:
        # We are running via slurm
        csvPath = "./SLURM_" + str(pidUnique) + ".csv"
        cmd = "squeue -u " + str(jobData.owner) + \
              ' --format=\"%.18i %.9P %.32j %.8u %.2t %.10M %.6D %R\"' + \
              ' > ' + csvPath
        try:
            subprocess.call(cmd, shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to pipe SLURM output to: " + csvPath
            raise

        if not os.path.isfile(csvPath):
            jobData.errMsg = "ERROR: squeue did not create necessary CSV file with job names."
            raise Exception()

        try:
            jobs = pd.read_csv(csvPath, delim_whitespace=True)
        except:
            jobData.errMsg = "ERROR: Failure to read in: " + csvPath
            raise

        # Delete temporary CSV files.
        cmdTmp = "rm -rf " + csvPath
        subprocess.call(cmd, shell=True)

        # Compile expected job name that the job should occupy.
        expName = programType + "_" + str(jobData.jobID) + "_" + str(groupNum)

        # Assume no jobs for basin are being ran, unless found in the data frame.
        status = False

        if len(jobs.NAME) > 0:
            for jobNum in range(0, len(jobs.NAME)):
                if jobs.NAME[jobNum].strip() == expName:
                    print("MODEL GROUP JOBS FOUND")
                    status = True
        else:
            status = False

        if not status:
            print("NO GROUP JOBS FOUND")

    if jobData.jobRunType == 4:
        # We are using mpiexec.
        pidActive = []
        # Compile expected job name that the job should occupy.
        expName = programType + "_" + str(jobData.jobID) + "_" + str(groupNum)
        for proc in psutil.process_iter():
            try:
                if proc.name() == expName:
                    pidActive.append(proc.pid)
            except:
                print(expName + " Found, but ended before Python could get the PID.")
        if len(pidActive) == 0:
            status = False
            print("NO GROUP JOBS FOUND")
        else:
            print("GROUP JOBS FOUND")
            # Ensure these are being ran by the proper user.
            proc_stat_file = os.stat('/proc/%d' % pidActive[0])
            uid = proc_stat_file.st_uid
            userCheck = pwd.getpwuid(uid)[0]
            if userCheck != str(jobData.owner):
                jobData.errMsg = "ERROR: " + expName + " is being ran by: " + \
                                 userCheck + " When it should be ran by: " + jobData.owner
                status = False
                raise Exception()
            else:
                status = True

    return status

def submitGroupCalibration(jobData,groupScript,pbsJobId,groupNum):
    """
    Generic function to submit a calibration group execution for a group of basins
    from the calibration orchestrator program.
    :param jobData:
    :param groupScript:
    :param pbsJobId:
    :return:
    """
    if jobData.jobRunType == 1:
        try:
            jobTmp = subprocess.check_output(['bsub','<',groupScript])
            pbsJobId[groupNum] = int(jobTmp.decode("UTF-8").split('.')[0])
        except:
            jobData.errMsg = "ERROR: Unable to launch: " + groupScript
            raise

    if jobData.jobRunType == 2:
        try:
            jobTmp = subprocess.check_output(['qsub',groupScript])
            pbsJobId[groupNum] = int(jobTmp.decode("UTF-8").split('.')[0])
        except:
            jobData.errMsg = "ERROR: Unable to launch: " + groupScript
            raise

    if jobData.jobRunType == 3 or jobData.jobRunType == 6:
        try:
            jobTmp = subprocess.check_output(['sbatch',groupScript])
            pbsJobId[groupNum] = int(jobTmp.decode("UTF-8").split('.')[0])
        except:
            jobData.errMsg = "ERROR: Unable to launch: " + groupScript
            raise

    if jobData.jobRunType == 4:
        # Compose stdout and stderr files to pipe output too.
        stdOut = jobData.jobDir + "/GROUP_JOB_OUT_" + str(jobData.jobID) + "_" + \
            str(groupNum) + ".out"
        stdErr = jobData.jobDir + "/GROUP_JOB_OUT_" + str(jobData.jobID) + "_" + \
                 str(groupNum) + ".err"
        cmd = groupScript + " 1>" + stdOut + " 2>" + stdErr
        try:
            p = subprocess.Popen([cmd], shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to launch: " + groupScript
            raise
