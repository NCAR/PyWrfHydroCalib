# Generate IO library for handling high level IO.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import pandas as pd
from errMod import wipeJobDir
import namelistMod
import subprocess
import pwd
import shutil

class gageMeta:
    def __init__(self):
        # Initialize object to hold metadata about a particular basin.
        # This will be used to populate namelist files, DB tables,
        # etc. These files will be listed in the DB metadata table and
        # should exist on the system.
        self.gage = []
        self.geoFile = []
        self.fullDom = []
        self.rtLnk = []
        self.lkFile = []
        self.gwFile = []
        self.udMap = []
        self.wrfInput = []
        self.soilFile = []
    def pullGageMeta(self,jobData,db,gageName):
        # Function to extract locations of gage-specific spatial files.
        
        tmpMeta = {'gageName':gageName,'geoFile':'','fullDomFile':'',\
                   'rtLnk':'','lkFile':'','gwFile':'','udMap':'',\
                   'wrfInput':'','soilFile':''}
        try:
            db.queryGageMeta(jobData,tmpMeta)
        except:
            raise
            
        self.gage = tmpMeta['gageName']
        self.geoFile = tmpMeta['geoFile']
        self.fullDom = tmpMeta['fullDomFile']
        self.rtLnk = tmpMeta['rtLnk']
        self.lkFile = tmpMeta['lkFile']
        self.gwFile = tmpMeta['gwFile']
        self.udMap = tmpMeta['udMap']
        self.wrfInput = tmpMeta['wrfInput']
        self.soilFile = tmpMeta['soilFile']
        
def getGageList(jobData,db):
    # Function for extracting list of gages 
    # based on either the CSV file, or an SQL
    # command to extract gages based on a user 
    # specified command.

    if len(jobData.gList) > 0:
        # User provided CSV file with list of gages.
        gListTmp = pd.read_csv(jobData.gList,dtype={0: str})
        
        jobData.gages = gListTmp.Gage[:]

        if len(jobData.gages) == 0:
            errMsg = "ERROR: List of gages for calibration is zero."
            jobData.errMsg = errMsg
            raise Exception()
            
        for tmpGage in range(0,len(gListTmp.Gage)):
            try:
                db.lookupGage(jobData,gListTmp.Gage[tmpGage])
            except:
                raise
    elif len(jobData.gSQL) > 0:
        # User provided SQL command to extract list of gages.
        try:
            gageList = db.queryGageList(jobData)
            jobData.gages = gageList[:]
        except:
            raise
        
def checkYsJobs(jobData):
    # Function to obtain a data frame containing Yellowstone
    # jobs being ran under the owner of the JobID.

    # Get unique PID.
    pidUnique = os.getpid()
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    jobDir = jobData.outDir + "/" + jobData.jobName
    
    csvPath = jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
    cmd = 'bjobs -u ' + str(jobData.owner) + ' -noheader > ' + csvPath
    subprocess.call(cmd,shell=True)
    
    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    lenJobs = len(jobs.JOBID)
    
    print jobs
    ## Loop through data frame. For jobs across multiple cores, the data frame
    ## needs to be filled in as the duplicate cores have NaN values, except for the
    ## first core.
    #for job in range(0,lenJobs):
    #    # Assume a NaN value with the "USER" field means this is a duplicate.
    #    jobIdTmp = jobs.JOBID[job]
    #    userTmp = jobs.USER[job]
    #    statTmp = jobs.STAT[job]
    #    queTmp = jobs.QUEUE[job]
    #    hostTmp = jobs.FROM_HOST[job]
    #    jobNameTmp = jobs.JOB_NAME[job]
    #    monthTmp = jobs.SUBMIT_MONTH[job]
    #    dayTmp = jobs.SUBMIT_DAY[job]
    #    hourTmp = jobs.SUBMIT_HHMM[job]
    #    
    #    if str(userTmp) != 'nan' and str(userTmp) != 'NaN':
    #        jobIdHold = jobIdTmp
    #        userHold = userTmp
    #        statHold = statTmp
    #        queHold = queTmp
    #        hostHold = hostTmp
    #        jobNameHold = jobNameTmp
    #        monthHold = monthTmp
    #        dayHold = dayTmp
    #        hourHold = hourTmp
    #    else:
    #        jobs.JOBID[job] = jobIdHold
    #        jobs.USER[job] = userHold
    #        jobs.STAT[job] = statHold
    #        jobs.QUEUE[job] = queHold
    #        jobs.FROM_HOST[job] = hostHold
    #        jobs.EXEC_HOST[job] = userTmp
    #        jobs.JOB_NAME[job] = jobNameHold
    #        jobs.SUBMIT_MONTH[job] = monthHold
    #        jobs.SUBMIT_DAY[job] = dayHold
    #        jobs.SUBMIT_HHMM[job] = hourHold
            
    ## Delete temporary CSV file
    #os.remove(csvPath)
    
    ## Loop through and check to make sure no existing jobs are being ran for any 
    ## of the gages.
    #if len(jobs) != 0:
    #    for gageCheck in range(0,jobData.gageIDs):
    #        jobNameCheck = "NWM_" + str(jobData.jobID) + "_" + str(jobData.gageIDs[gageCheck])
    #        testDF = jobs.query("JOB_NAME == '" + jobNameCheck + "'")
    #        if len(testDF) != 0:
    #            jobData.errMsg = "ERROR: Job ID: " + str(jobData.jobId) + \
    #                             " is already being ran under owner: " + \
    #                             str(jobData.owner) + ". User: " + \
    #                             str(userTmp) + " is attempting to initiate a spinup."
    #            print "ERROR: You are attempting to intiate a job that is already being " + \
    #                  "ran by user: " + str(jobData.owner)
    #            raise Exception()
                
    #print jobs
            
def setupModels(jobData,db,args):
    # Function for setting up all model directories,
    # links to forcings, namelist files, etc. 
    # Function will loop through each basin to calibrate,
    # extract information about geospatial files, nested
    # index values, etc. This information will be used in 
    # the setup. 
    
    # First create top level directory based on the job name.
    parentDir = jobData.outDir + "/" + jobData.jobName
    
    if os.path.isdir(parentDir):
        jobData.errMsg = "ERROR: Top level directory: " + parentDir + " already exists"
        raise Exception()
        
    try:
        os.mkdir(parentDir)
    except:
        wipeJobDir(jobData)
        jobData.errMsg = "ERROR: Failure to create directory: " + parentDir
        raise
        
    # Create gage-specific object that will contain gage-specific information.
    gageData = gageMeta()
    
    # Copy config file to the top level directory. This will be used during
    # restarts to extract information about the job. It was decided to do
    # this opposed to attempting to enter the plethura of information 
    # specific to the job into the metadata table. 
    configPath = str(args.configFile[0])
    copyPath = parentDir + '/setup.config'
    try:
        shutil.copy(configPath,copyPath)
    except:
        wipeJobDir(jobData)
        jobData.errMsg = "ERROR: Failure to copy configuration setup file."
        raise
        
    # Loop through each basin and setup appropriate directories.
    for gage in range(0,len(jobData.gages)):
        gageDir = parentDir + "/" + str(jobData.gages[gage])
        
        try:
            os.mkdir(gageDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + gageDir
            raise
            
        # Create symbolic link to forcing directory.
        fLink = gageDir + "/FORCING"
        try:
            os.symlink(jobData.fDir,fLink)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create FORCING link to: " + jobData.fDir
            raise
            
        # Create observations directory to hold obs for calibration/eval, etc
        obsDir = gageDir + "/OBS"
        try:
            os.mkdir(obsDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + obsDir
            raise
        
        # Create sub-directories for spinup/calibration runs.
        spinupDir = gageDir + "/RUN.SPINUP"
        try:
            os.mkdir(spinupDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + spinupDir
            raise
            
        calibDir = gageDir + "/RUN.CALIB"
        try:
            os.mkdir(calibDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + calibDir
            raise
            
        validDir = gageDir + "/RUN.VALID"
        try:
            os.mkdir(validDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + validDir
            raise
        
        # Create symbolic links necessary for model runs.
        link1 = gageDir + "/RUN.SPINUP/wrf_hydro.exe"
        link2 = gageDir + "/RUN.CALIB/wrf_hydro.exe"
        link3 = gageDir + "/RUN.VALID/wrf_hydro.exe"
        try:
            os.symlink(str(jobData.exe),link1)
            os.symlink(str(jobData.exe),link2)
            os.symlink(str(jobData.exe),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to WRF-Hydro executable."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/CHANPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/CHANPARM.TBL"
        link3 = gageDir + "/RUN.VALID/CHANPARM.TBL"
        try:
            os.symlink(str(jobData.chanParmTbl),link1)
            os.symlink(str(jobData.chanParmTbl),link2)
            os.symlink(str(jobData.chanParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to channel parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/GENPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/GENPARM.TBL"
        link3 = gageDir + "/RUN.VALID/GENPARM.TBL"
        try:
            os.symlink(str(jobData.genParmTbl),link1)
            os.symlink(str(jobData.genParmTbl),link2)
            os.symlink(str(jobData.genParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to general parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/HYDRO.TBL"
        link2 = gageDir + "/RUN.CALIB/HYDRO.TBL"
        link3 = gageDir + "/RUN.VALID/HYDRO.TBL"
        try:
            os.symlink(str(jobData.hydroTbl),link1)
            os.symlink(str(jobData.hydroTbl),link2)
            os.symlink(str(jobData.hydroTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to hydro table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/LAKEPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/LAKEPARM.TBL"
        link3 = gageDir + "/RUN.VALID/LAKEPARM.TBL"
        try:
            os.symlink(str(jobData.lakeParmTbl),link1)
            os.symlink(str(jobData.lakeParmTbl),link2)
            os.symlink(str(jobData.lakeParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to lake parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/MPTABLE.TBL"
        link2 = gageDir + "/RUN.CALIB/MPTABLE.TBL"
        link3 = gageDir + "/RUN.VALID/MPTABLE.TBL"
        try:
            os.symlink(str(jobData.mpParmTbl),link1)
            os.symlink(str(jobData.mpParmTbl),link2)
            os.symlink(str(jobData.mpParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to MP parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/SOILPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/SOILPARM.TBL"
        link3 = gageDir + "/RUN.VALID/SOILPARM.TBL"
        try:
            os.symlink(str(jobData.soilParmTbl),link1)
            os.symlink(str(jobData.soilParmTbl),link2)
            os.symlink(str(jobData.soilParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to soil parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/URBPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/URBPARM.TBL"
        link3 = gageDir + "/RUN.VALID/URBPARM.TBL"
        try:
            os.symlink(str(jobData.urbParmTbl),link1)
            os.symlink(str(jobData.urbParmTbl),link2)
            os.symlink(str(jobData.urbParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to urban parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/VEGPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/VEGPARM.TBL"
        link3 = gageDir + "/RUN.VALID/VEGPARM.TBL"
        try:
            os.symlink(str(jobData.vegParmTbl),link1)
            os.symlink(str(jobData.vegParmTbl),link2)
            os.symlink(str(jobData.vegParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to vegetation parameter table."
            raise
            
        # Extract gage-specific information (geogrid file, fulldom file, etc)
        # from metadata DB.
        try:
            gageData.pullGageMeta(jobData,db,str(jobData.gages[gage]))
        except:
            wipeJobDir(jobData)
            raise
            
        # Create namelist.hrldas, hydro.namelist files for spinup/calibration runs.
        try:
            namelistMod.createHrldasNL(gageData,jobData,spinupDir,1)
        except:
            wipeJobDir(jobData)
            raise
        try:
            namelistMod.createHrldasNL(gageData,jobData,calibDir,2)
        except:
            wipeJobDir(jobData)
            raise
        try:
            namelistMod.createHrldasNL(gageData,jobData,validDir,3)
        except:
            wipeJobDir(jobData)
            raise
        try:
            namelistMod.createHydroNL(gageData,jobData,spinupDir,1)
        except:
            wipeJobDir(jobData)
            raise
        try:
            namelistMod.createHydroNL(gageData,jobData,calibDir,2)
        except:
            wipeJobDir(jobData)
            raise
        try:
            namelistMod.createHydroNL(gageData,jobData,validDir,3)
        except:
            wipeJobDir(jobData)
            raise