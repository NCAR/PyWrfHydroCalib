# General function library for creating namelist.hrldas and hydro.namelist 
# files using information the user provided, and basin-specific information.

# Logan Karsten
# National Center for Atmospheric Research 
# Research Applications Laboratory

import os

def createHrldasNL(gageData,jobData,outDir,typeFlag):
    # General function for creation of a namelist.hrldas file.
    
    # NOTE: typeFlag = 1 indicates spinup, 2 indicates calibration.
    
    # Create path for the namelist file
    pathOut = outDir + "/namelist.hrldas"
    if os.path.isfile(pathOut):
        jobData.errMsg = "ERROR: " + pathOut + " already exists."
        raise
    
    # Write each line of the expected hrldas.namelist file.
    try:
        fileObj = open(pathOut,'w')
        fileObj.write('&NOAHLSM_OFFLINE')
        fileObj.write('\n')
        inStr = ' HRLDAS_SETUP_FILE = "' + gageData.wrfInput + '"'
        fileObj.write(inStr)
        inStr = ' INDIR = "' + jobData.fDir + '"'
        fileObj.write(inStr)
        inStr = ' SPATIAL_FILENAME = "' + gageData.soilFile + '"'
        fileObj.write(inStr)
        inStr = ' OUTDIR = "' + outDir + '"'
        fileObj.write(inStr)
        fileObj.write('\n')
        if typeFlag == 1:
            inStr = ' START_YEAR = ' + jobData.bSpinDate.strftime('%Y')
            fileObj.write(inStr)
            inStr = ' START_MONTH = ' + jobData.bSpinDate.strftime('%m')
            fileObj.write(inStr)
            inStr = ' START_DAY = ' + jobData.bSpinDate.strftime('%d')
            fileObj.write(inStr)
        else:
            inStr = ' START_YEAR = ' + jobData.bCalibDate.strftime('%Y')
            fileObj.write(inStr)
            inStr = ' START_MONTH = ' + jobData.bCalibDate.strftime('%m')
            fileObj.write(inStr)
            inStr = ' START_DAY = ' + jobData.bCalibDate.strftime('%d')
            fileObj.write(inStr)
    except:
        jobData.errMsg = "ERROR: Failure to create: " + pathOut
        raise
    
def createHydroNL(gageData,jobData,outDir,typeFlag):
    # General function for creation of a hydro.namelist file.

    # Create path for the namelist file.
    pathOut = outDir + "/hydro.namelist"
    if os.path.isfile(pathOut):
        jobData.errMsg = "ERROR: " + pathOut + " already exists."
        raise