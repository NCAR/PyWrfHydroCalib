# Generate IO library for handling high level IO.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import pandas as pd

def getGageList(jobData):
    # Function for extracting list of gages 
    # based on either the CSV file, or an SQL
    # command to extract gages based on a user 
    # specified command.

    if len(jobData.gList) > 0:
        # User provided CSV file with list of gages.
        gListTmp = pd.read_csv(jobData.gList)
        
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
    parentDir = jobData.outDir[0] + "/" + jobData.jobName[0]
    print jobData.outDir
    print jobData.jobName
    
    if os.path.isdir(parentDir):
        jobData.errMsg = "ERROR: Top level directory: " + parentDir + " already exists"
        raise
        
    #try:
    #    os.mkdir(parentDir)
    #except:
    #    jobData.errMsg = "ERROR: Failure to create directory: " + parentDir
    #    raise