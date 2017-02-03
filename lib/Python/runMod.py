# Module file for containing functions for executing WRF-Hydro model runs.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

#import datetime
import os
import calibIoMod
import namelistMod
import statusMod
import errMod
import subprocess

def runModel(statusData,staticData,db,gageID,gage,typeFlag,keySlot,basinNum):
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
    
    if typeFlag == 1:
        runDir = statusData.jobDir + "/" + gage + "/RUN.SPINUP"
    elif typeFlag == 2:
        runDir = statusData.jobDir + "/" + gage + "/RUN.CALIB"
    elif typeFlag == 3:
        runDir = statusData.jobDir + "/" + gage + "/RUN.VALID"
        
    if not os.path.isdir(runDir):
        statusData.errMsg = "ERROR: " + runDir + " not found."
        raise Exception()
        
    # If BSUB run script doesn't exist, create it here.
    bsubFile = runDir + "/run_NWM.sh"
    if not os.path.isfile(bsubFile):
        try:
            generateRunScript(statusData,int(gageID),runDir)
        except:
            raise
    
    # Calculate datetime objects
    if typeFlag == 1:
        begDate = statusData.bSpinDate
        endDate = statusData.eSpinDate
    elif typeFlag == 2:
        begDate = statusData.bCalibDate
        endDate = statusData.eCalibDate
    elif typeFlag == 3:
        begDate = statusData.bValidDate
        endDate = statusData.eValidDate
        
    runFlag = 0
    
    # Pull gage metadata
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
        
    # Initialize status
    keyStatus = keySlot[basinNum]
    
    try:
        basinStatus = statusMod.checkBasJob(statusData,basinNum)
    except:
        raise
        
    print "BASIN STATUS = " + basinStatus
    ## Initialize empty restart paths
    #hydroRst = ""
    #lsmRst = ""
    
    # Create path to LOCK file if neeced
    lockPath = runDir + "/RUN.LOCK"
    
    if keyStatus == 1.0:
        # Model has already completed
        runFlag = False
        
    # For uncompleted simulations that are still listed as running.
    #if keyStatus == 0.5:
    #    # If a model is running for this basin, continue and set keyStatus to 0.5
    #    if basinStatus:
    #        keySlot[basinNum] = 0.5
    #        keyStatus = 0.5
    #        runFlag = False
    #    else:
    #        # Either simulation has completed, or potentially crashed.
    #        runStatus = statusMod.walkMod(begDate,endDate,runDir,hydroRst,lsmRst)
    #        begDate = runStatus[0]
    #        endDate = runStatus[1]
    #        runFlag = runStatus[2]
    #        if runFlag:
    #            # Model crashed as simulation is not complete but no processes are running.
    #            statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
    #                                " Failed. Attempting to restart."
    #            errMod.sendMsg(statusData)
    #            keySlot[basinNum] = -0.25
    #            keyStatus = -0.25
    #        else:
    #            # Model has completed!
    #            keySlot[basinNum] = 1.0
    #            keyStatus = 1.0
    #            runFlag = False
                
    # For simulations that are fresh
    if keyStatus == 0.0:
        if basinStatus:
            # Model is still running from previous instance of workflow. Allow it to continue.
            keySlot[basinNum] = 0.5
            keyStatus = 0.5
            runFlag = False
        else:
            print begDate
            print endDate
            print "XXXXXX"
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            print runFlag
            print begDate
            print endDate
            if not runFlag:
                # Model simulation completed before workflow was restarted
                keySlot[basinNum] = 1.0
                keyStatus = 1.0
                runFlag = False
                
    ## For when the model failed TWICE and is locked.
    #if keyStatus == -1.0:
    #    # If LOCK file exists, no simulation will take place. File must be removed
    #    # manually by user.
    #    if os.path.isfile(lockPath):
    #        runFlag = False
    #    else:
    #        # LOCK file was removed, upgrade status to 0.0 temporarily
    #        runStatus = statusMod.walkMod(begDate,endDate,runDir)
    #        begDate = runStatus[0]
    #        endDate = runStatus[1]
    #        runFlag = runStatus[2]
    #        if runFlag:
    #            keySlot[basinNum] = 0.0
    #            keyStatus = 0.0
    #        else:
    #            # Model sucessfully completed.
    #            keySlot[basinNum] = 1.0
    #            keyStatus = 1.0
    #            runFlag = False
                
    ## For when the model crashed ONCE
    #if keyStatus == -0.5:
    #    if basinStatus:
    #        # Model is running again, upgrade status
    #        # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
    #        keySlot[basinNum] = 0.5
    #        keyStatus = 0.5
    #        runFlag = False
    #    else:
    #        runStatus = statusMod.walkMod(begDate,endDate,runDir)
    #        begDate = runStatus[0]
    #        endDate = runStatus[1]
    #        runFlag = runStatus[2]
    #        if runFlag:
    #            # Model has crashed again, time to lock it up and send a message out.
    #            statusData.genMsg = "ERROR: SIMULATION FOR GAGE: " + statusData.gages[basinNum] + \
    #                                " HAS FAILED A SECOND TIME. PLEASE FIX ISSUE AND " + \
    #                                "MANUALLY REMOVE LOCK FILE: " + lockPath
    #            open(lockPath,'a').close()
    #           keySlot[basinNum] = -1.0
    #            keyStatus = -1.0
    #            runFlag = False
    #        else:
    #            # Model sucessfully completed from first failed attempt.
    #            keySlot[basinNum] = 1.0
    #            keyStatus = 1.0
                
    #if keyStatus == -0.25 and runFlag:
    #    # Restarting model from one crash
    #    # First delete namelist files if they exist.
    #    check = runDir + "/namelist.hrldas"
    #    check2 = runDir + "/hydro.namelist"
    #    if os.path.isfile(check):
    #        os.remove(check)
    #    if os.path.isfile(check2):
    #        os.remove(check2)
    #    
    #    if begDate == staticData.bSpinDate:
    #        startType = 1
    #    else:
    #        startType = 2
    #    
    #    try:
    #        namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate)
    #        namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate)
    #    except:
    #        raise
    #        
    #    if startType == 2:
    #        # Clean run directory of any old diagnostics files
    #        try:
    #            errMod.cleanRunDir(statusData,runDir)
    #        except:
    #            raise
    #            
    #    # Fire off model.
    #    cmd = "bsub < " + runDir + "/run_NWM.sh"
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
    #        raise
    #        
    #    # Compile 
    #    keyStatus = -0.5
    #    keySlot[basinNum] = -0.5
        
    if keyStatus == 0.0 and runFlag:
        # Model needs to be either ran, or restarted
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        if begDate == staticData.bSpinDate:
            startType = 1
        else:
            startType = 2
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate)
        except:
            raise
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        # Fire off model.
        cmd = "bsub < " + runDir + "/run_NWM.sh"
        print cmd
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.5
        keySlot[basinNum] = 0.5
                
def generateRunScript(jobData,gageID,runDir):
    """
    Generic function to create a run script that will be called by bsub
    to execute the model.
    """
    
    outFile = runDir + "/run_NWM.sh"
    
    if os.path.isfile(outFile):
        jobData.errMsg = "ERROR: Run script: " + outFile + " already exists."
        raise Exception()
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        fileObj.write('# LSF Batch Script to Run NWM Calibration Simulations\n')
        fileObj.write('#\n')
        inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -x\n')
        inStr = "#BSUB -n " + str(jobData.nCores) + '\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -R "span[ptile=16]"\n')
        #inStr = "#BSUB -J NWM_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
        inStr = "#BSUB -J NWM_3425565236_" + str(gageID) + '\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -o wrf%J.out\n')
        fileObj.write('#BSUB -e wrf%J.err\n')
        fileObj.write('#BSUB -W 3:00\n')
        fileObj.write('#BSUB -q premium\n')
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        fileObj.write('mpirun.lsf ./wrf_hydro.exe\n')
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
        
        
        