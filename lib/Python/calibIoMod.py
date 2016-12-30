# Generate IO library for handling high level IO.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import pandas as pd
from errMod import wipeJobDir

def getGageList(jobData):
    # Function for extracting list of gages 
    # based on either the CSV file, or an SQL
    # command to extract gages based on a user 
    # specified command.

    if len(jobData.gList) > 0:
        # User provided CSV file with list of gages.
        gListTmp = pd.read_csv(jobData.gList,dtype={0: str})
        
        # PLACEHOLDER FOR CHECKING DB TO ENSURE
        # ALL GAGE NAMES EXIST IN META TABLE
        jobData.gages = gListTmp.Gage[:]

        if len(jobData.gages) == 0:
            errMsg = "ERROR: List of gages for calibration is zero."
            jobData.errMsg = errMsg
            raise
            
def setupModels(jobData):
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
        raise
        
    try:
        os.mkdir(parentDir)
    except:
        wipeJobDir(jobData,parentDir)
        jobData.errMsg = "ERROR: Failure to create directory: " + parentDir
        raise
        
    # Loop through each basin and setup appropriate directories.
    for gage in range(0,len(jobData.gages)):
        print jobData.gages[gage]
        gageDir = parentDir + "/" + str(jobData.gages[gage])
        print gageDir
        
        try:
            os.mkdir(gageDir)
        except:
            wipeJobDir(jobData,parentDir)
            jobData.errMsg = "ERROR: Failure to create directory: " + gageDir
            raise
            
        # Create symbolic link to forcing directory.
        fLink = gageDir + "/FORCING"
        try:
            os.symlink(jobData.fDir,fLink)
        except:
            wipeJobDir(jobData,parentDir)
            jobData.errMsg = "ERROR: Failure to create FORCING link to: " + jobData.fDir
            raise
            
        # Create observations directory to hold obs for calibration/eval, etc
        obsDir = gageDir + "/OBS"
        try:
            os.mkdir(obsDir)
        except:
            wipeJobDir(jobData,parentDir)
            jobData.errMsg = "ERROR: Failure to create directory: " + obsDir
            raise
        
        # Create sub-directories for spinup/calibration runs.
        spinupDir = gageDir + "/RUN.SPINUP"
        try:
            os.mkdir(spinupDir)
        except:
            wipeJobDir(jobData,parentDir)
            jobData.errMsg = "ERROR: Failure to create directory: " + spinupDir
            raise
            
        calibDir = gageDir + "/RUN.CALIB"
        try:
            os.mkdir(calibDir)
        except:
            wipeJobDir(jobData,parentDir)
            jobData.errMsg = "ERROR: Failure to create directory: " + calibDir
            raise
        
            