# Module file for containing functions for executing WRF-Hydro model runs.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import datetime
import os

def runModel(jobData,runDir,typeFlag):
    """
    Generic function for running the model. Some basic information about
    the run directory, beginning date, ending dates, account keys,
    number of cores to use, etc will be used to compose a BSUB
    submision script. This function will walk the run directory 
    to determine where the model left off. If no restart files exist,
    then the function will assume the model has not ran at all. Both
    the LSM and hydro restart files must be present in order for the
    model to restart. 
    """
    # typeFlag = 1: Spinup
    # typeFlag = 2: Calibration
    # typeFlag = 3: Validation
    
    # Calculate datetime objects
    if typeFlag == 1:
        dt = jobData.eSpinDate - jobData.bSpinDate
        begDate = jobData.bSpinDate
        endDate = jobData.eSpinDate
    elif typeFlag == 2:
        dt = jobData.eCalibDate - jobData.bCalibDate
        begDate = jobData.bCalibDate
        endDate = jobData.eCalibDate
    elif typeFlag == 3:
        dt = jobData.eValidDate - jobData.bValidDate
        begDate = jobData.bValidDate
        endDate = jobData.eValidDate
        
    nHours = int((dt.days*24*3600 + dt.seconds)/3600.0)
    
    rstFlag = 0 # Cold start assumed, unless restart files are found.
    
    # Loop through each hour of the period being modeled. Check for both 
    # the LSM and hydro restart file.
    for hourModel in range(0,nHours+1):
        dCurrent = begDate + datetime.timedelta(seconds=3600.0*hourModel)
        
        lsmRestartPath = runDir + "/RESTART." + dCurrent.strftime('%Y%m%d%H')
        hydroRestartPath = runDir + "/HYDRO_RST." + dCurrent.strftime('%Y-%m-%d_%H') + \
                           ':00_DOMAIN1'
                           
        if os.path.isfile(lsmRestartPath) and os.path.isfile(hydroRestartPath):
            begDate = dCurrent
            rstFlag = 1
            
    print begDate
            
    #if begDate == endDate:
    #    return # Model has completed

    