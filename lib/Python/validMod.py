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

import warnings
warnings.filterwarnings("ignore")

def runModelCtrl(statusData,staticData,db,gageID,gage,keySlot,basinNum,run,libPathTop):
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
    # Establish the "control" and "best" status values. These are important for 
    # the workflow.
    ctrlStatus = keySlot[basinNum,0]
    bestStatus = keySlot[basinNum,1]
    
    print "CALIB STATUS = " + ctrlStatus
    print "BEST STATUS = " + bestStatus
    # If the control status is 1.0, this means the model is complete and we can 
    # return to the main workflow calling program.
    if ctrlStatus == 1.0:
        return
        
    # Pull gage metadata
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
    
    # Establish directory paths.
    runDir = statusData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/CTRL"
    bestDir = statusData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/BEST"
    parmInDir = statusData.jobDir + "/" + gage + "/RUN.CALIB/BASELINE_PARAMETERS"
    
    print runDir
    print bestDir
    print parmInDir
    
    if not os.path.isdir(runDir):
        statusData.errMsg = "ERROR: " + runDir + " not found."
        raise Exception()
    if not os.path.isdir(parmInDir):
        statusData.errMsg = "ERROR: " + parmInDir + " not found."
        raise Exception()
    if not os.path.isdir(bestDir):
        statusData.errMsg = "ERROR: " + bestDir + " not found."
        raise Exception()
        
    # Determine which iteration was the best. If no best iteration was found,
    # this means something happened during the calibrations and all values were reset
    # to allow remaining basins to finish. If the out status from the DB command
    # below is -99, then simply set all status values to 1.0
    try:
        iterStatus = db.genValidParmTbl(statusData,statusData.jobID,gageID,gage)
    except:
        raise
    if iterStatus == -99:
        keySlot[basinNum,0] = 1.0
        keySlot[basinNum,1] = 1.0
        return
        
    ## Make symbolic links as necssary.
    #try:
    #    linkToRst(statusData,gage,runDir)
    #except:
    #    raise
        
    ## Create symbolic links to Python/R code to generate parameters and evaluate
    ## model output if it hasn't already been created.
    #parmGenProgram = libPathTop + "/Python/generate_parameters.py"
    #evalProgram = libPathTop + "/R/eval_output.R"
    #try:
    #    link = bestDir + "/generate_parameters.py"
    #    os.symlink(parmGenProgram,link)
    #except:
    #    statusData.errMsg = "ERROR: Failure to link: " + parmGenProgram
    #    raise
    #try:
    #    link = bestDir + "/eval_output.R"
    #    os.symlink(evalProgram,link)
    #except:
    #    statusData.errMsg = "ERROR: Failure to link: " + evalProgram
    #    raise
        
    ## Create two run scripts:
    ## 1.) Job script to prepare parameter files for both the control and best
    ##     model simulations.
    ## 2.) Job script to run the model with control parameters.
    #parmRunScript = runDir + "/gen_parms.sh"
    #bsub1Script = runDir + "/bsub_parms.sh"
    #bsub2Script = runDir + "/run_NWM.sh"
    
    ## If the files exist, remove them and re-create.
    #if os.path.isfile(parmRunScript):
    #    os.remove(parmRunScript)
    #if os.path.isfile(bsub1Script):
    #    os.remove(bsub1Script)
    #if os.path.isfile(bsub2Script):
    #    os.remove(bsub2Script)
        
    #try:
    #    generateParmScript()
    #except:
    #    raise
    #try:
    #    generateParmRunScript()
    #except:
    #    raise
    #try:
    #    generateRunScript()
    #except:
    #    raise

    ## Calculate datetime objects
    #begDate = statusData.bValidDate
    #endDate = statusData.eValidDate
        
    ## Initialize status
    #keyStatus = keySlot[basinNum,run]
    
    #try:
    #    basinStatus = statusMod.checkBasJob(statusData,basinNum)
    #except:
    #    raise
        
    #print "BASIN STATUS = " + str(basinStatus)
    ## Create path to LOCK file if neeced
    #lockPath = runDir + "/RUN.LOCK"
    
    #if keyStatus == 1.0:
    #    # Model has already completed
    #    runFlag = False
    #    return
        
    ## For uncompleted simulations that are still listed as running.
    #if keyStatus == 0.5:
    #    # If a model is running for this basin, continue and set keyStatus to 0.5
    #    if basinStatus:
    #        keySlot[basinNum,run] = 0.5
    #        keyStatus = 0.5
    #        runFlag = False
    #    else:
    #        # Either simulation has completed, or potentially crashed.
    #        runStatus = statusMod.walkMod(begDate,endDate,runDir)
    #        begDate = runStatus[0]
    #        endDate = runStatus[1]
    #        runFlag = runStatus[2]
    #        if runFlag:
    #            # Model crashed as simulation is not complete but no processes are running.
    #            statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
    #                                " Failed. Attempting to restart."
    #            print statusData.genMsg
    #            errMod.sendMsg(statusData)
    #            keySlot[basinNum,run] = -0.25
    #            keyStatus = -0.25
    #        else:
    #            # Model has completed!
    #            keySlot[basinNum,run] = 1.0
    #            keyStatus = 1.0
    #            runFlag = False
           
    #print keyStatus
    ## For simulations that are fresh
    #if keyStatus == 0.0:
    #    if basinStatus:
    #        # Model is still running from previous instance of workflow. Allow it to continue.
    #        keySlot[basinNum,run] = 0.5
    #        keyStatus = 0.5
    #        runFlag = False
    #    else:
    #        print begDate
    #        print endDate
    #        print "XXXXXX"
    #        runStatus = statusMod.walkMod(begDate,endDate,runDir)
    #        begDate = runStatus[0]
    #        endDate = runStatus[1]
    #        runFlag = runStatus[2]
    #        print runFlag
    #        print begDate
    #        print endDate
    #        if not runFlag:
    #            # Model simulation completed before workflow was restarted
    #            keySlot[basinNum,run] = 1.0
    #            keyStatus = 1.0
    #            runFlag = False
                
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
    #            keySlot[basinNum,run] = 0.0
    #            keyStatus = 0.0
    #        else:
    #            # Model sucessfully completed.
    #            keySlot[basinNum,run] = 1.0
    #            keyStatus = 1.0
    #            runFlag = False
                
    ## For when the model crashed ONCE
    #if keyStatus == -0.5:
    #    if basinStatus:
    #        # Model is running again, upgrade status
    #        # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
    #        keySlot[basinNum,run] = 0.5
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
    #            errMod.sendMsg(statusData)
    #            print statusData.genMsg
    #            open(lockPath,'a').close()
    #            keySlot[basinNum,run] = -1.0
    #            keyStatus = -1.0
    #            runFlag = False
    #        else:
    #            # Model sucessfully completed from first failed attempt.
    #            keySlot[basinNum,run] = 1.0
    #            keyStatus = 1.0
                
    #if keyStatus == -0.25 and runFlag:
    #    print keyStatus
    #    # Restarting model from one crash
    #    # First delete namelist files if they exist.
    #    check = runDir + "/namelist.hrldas"
    #    check2 = runDir + "/hydro.namelist"
    #    if os.path.isfile(check):
    #        os.remove(check)
    #    if os.path.isfile(check2):
    #        os.remove(check2)
    #    
    #    if begDate == staticData.bValidDate:
    #        startType = 1
    #    else:
    #        startType = 2
        
    #    try:
    #        namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,run+2)
    #        namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,run+2)
    #    except:
    #        raise
            
    #    if startType == 2:
    #        # Clean run directory of any old diagnostics files
    #        try:
    #            errMod.cleanRunDir(statusData,runDir)
    #        except:
    #            raise
                
    #    # Fire off model.
    #    cmd = "bsub < " + runDir + "/run_NWM.sh"
    #    print begDate
    #    print endDate
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
    #        raise
            
    #    # Revert statuses to -0.5 for next loop to convey the model crashed once. 
    #    keyStatus = -0.5
    #    keySlot[basinNum,run] = -0.5
        
    #if keyStatus == 0.0 and runFlag:
    #    # Model needs to be either ran, or restarted
    #    # First delete namelist files if they exist.
    #    check = runDir + "/namelist.hrldas"
    #    check2 = runDir + "/hydro.namelist"
    #    if os.path.isfile(check):
    #        os.remove(check)
    #    if os.path.isfile(check2):
    #        os.remove(check2)
    #    
    #    if begDate == staticData.bValidDate:
    #        startType = 1
    #    else:
    #        startType = 2
    #    
    #    try:
    #        namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,run+2)
    #        namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,run+2)
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
    #    print cmd
    #    try:
    #        subprocess.call(cmd,shell=True)
    #    except:
    #        statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
    #        raise
            
    #    keyStatus = 0.5
    #    keySlot[basinNum,run] = 0.5
        
def runModelBest(statusData,staticData,db,gageID,gage,keySlot,basinNum,run):
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
    # Establish the "control" and "best" status values. These are important for 
    # the workflow.
    ctrlStatus = keySlot[basinNum,0]
    bestStatus = keySlot[basinNum,1]
    
    # If the control status is not at least 0.25, this means the code to generate
    # parameters is still running, hasn't begun yet, or there's an issue with
    # the model. Simply return to the main workflow calling program.
    if ctrlStatus < 0.25:
        return
        
    # Create two run scripts:
    # 1.) Job script to run the model with best parameters.
    # 2.) Job script to run the R code for evaluation/plotting. 
                
def generateRunScript(jobData,gageID,runDir,gageMeta,modName):
    """
    Generic function to create a run script that will be called by bsub
    to execute the model.
    """
    
    outFile = runDir + "/run_NWM.sh"
    
    if os.path.isfile(outFile):
        os.remove(outFile)
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        fileObj.write('# LSF Batch Script to Run NWM Calibration Simulations\n')
        fileObj.write('#\n')
        inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -x\n')
        inStr = "#BSUB -n " + str(jobData.nCoresMod) + '\n'
        fileObj.write(inStr)
        inStr = "#BSUB -J NWM_" + str(modName) + "_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
        fileObj.write(inStr)
        inStr = '#BSUB -o ' + runDir + '/%J.out\n'
        fileObj.write(inStr)
        inStr = '#BSUB -e ' + runDir + '/%J.err\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -W 6:00\n')
        fileObj.write('#BSUB -q premium\n')
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        fileObj.write('mpirun.lsf ./wrf_hydro.exe\n')
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generateParmScript(jobData,runDir,gage,parmInDir):
    """
    Generic function to generate the shell script to call Python to
    generate the new parameter datasets.
    """
    
    outFile = runDir + "/gen_parms.sh"
    pyProgram = runDir + "/generate_parameters.py"
    ctrlRunDir = jobData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/CTRL"
    defaultDir = jobData.jobDir + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS"
    
    if os.path.isfile(outFile):
        os.remove(outFile)
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('python ' + pyProgram + ' ' + runDir + ' ' + parmInDir + ' ' + \
                      ctrlRunDir + ' ' + defaultDir + ' \n')
        fileObj.write('exit\n')
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def linkToRst(statusData,gage,runDir):
    """
    Generic function to link to necessary restart files from the spinup.
    """
    # Check to make sure symbolic link to spinup state exists.
    check1 = statusData.jobDir + "/" + gage + "/RUN.SPINUP/OUTPUT/RESTART." + statusData.eSpinDate.strftime('%Y%m%d') + "00_DOMAIN1"
    check2 = statusData.jobDir + "/" + gage + "/RUN.SPINUP/OUTPUT/HYDRO_RST." + statusData.eSpinDate.strftime('%Y-%m-%d') + "_00:00_DOMAIN1"
    if not os.path.isfile(check1):
        statusData.errMsg = "ERROR: Spinup state: " + check1 + " not found."
        raise Exception()
    if not os.path.isfile(check2):
        statusData.errMsg = "ERROR: Spinup state: " + check2 + " not found."
        raise Exception()
    # Create links if they don't exist
    link1 = runDir + "/RESTART." + statusData.bCalibDate.strftime('%Y%m%d') + "00_DOMAIN1"
    link2 = runDir + "/HYDRO_RST." + statusData.bCalibDate.strftime('%Y-%m-%d') + "_00:00_DOMAIN1"
    if not os.path.islink(link1):
        os.symlink(check1,link1)
    if not os.path.islink(link2):
        os.symlink(check2,link2)
