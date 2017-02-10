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

def runModel(statusData,staticData,db,gageID,gage,keySlot,basinNum,iteration):
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
    # First check to make sure previous iteration's status is 1.0 (unless iteration 0).
    if iteration > 0.0:
        if keySlot[basinNum,iteration-1] != 1.0:
            return
            
    # Determine which run sub-directory based on COMPLETE flag presence.
    runDir = statusData.jobDir + "/" + gage + "/RUN.CALIB/OUTPUT"
    workDir = statusData.jobDir + "/" + gage + "/RUN.CALIB"
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
        raise Exception()
    if not os.path.isdir(runDir):
        statusData.errMsg = "ERROR: " + runDir + " not found."
        raise Exception()
        
    # Generate BSUB file necessary for running R calibration/analysis
    # code.
    try:
        generateCalibScript(statusData,int(gageID),runDir,workDir)
    except:
        raise
        
    # If BSUB run script doesn't exist, create it here.
    bsubFile = runDir + "/run_NWM.sh"
    if not os.path.isfile(bsubFile):
        try:
            generateRunScript(statusData,int(gageID),runDir)
        except:
            raise
    
    # Calculate datetime objects
    begDate = statusData.bCalibDate
    endDate = statusData.eCalibDate
        
    # Pull gage metadata
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
     
    # Initialize status
    keyStatus = keySlot[basinNum,iteration]
    
    # Check to see if a model simulation is occurring.
    try:
        basinStatus = statusMod.checkBasJob(statusData,basinNum)
    except:
        raise
        
    # Check to see if an R script calibration job is occurring.
    try:
        calibStatus = statusMod.checkCalibJob(statusData,basinNum)
    except:
        raise
     
    print "BASIN STATUS = " + str(basinStatus)
    print "CALIB STATUS = " + str(calibStatus)
    # Create path to LOCK file if neeced
    lockPath = workDir + "/RUN.LOCK"
    calibLockPath = workDir + "/CALIB.LOCK"
    calibCompleteFlag = workDir + "/CALIB_ITER.COMPLETE"
    calibTbl = workDir + "/params_new.txt"
    statsTbl = workDir + "/params_stats.txt"
    
    # Initialize flags to False
    runFlag = False
    runCalib = False
    if keyStatus == 1.0:
        # Calibration and simulation for this iteration has completed
        runFlag = False
        runCalib = False
        return
    # For uncompleted simulations that are still listed as running.
    if keyStatus == 0.5:
        # If a model is running for this basin, continue and set keyStatus to 0.5
        if basinStatus:
            keySlot[basinNum,iteration] = 0.5
            keyStatus = 0.5
            runFlag = False
            runCalib = False
        else:
            # Either simulation has completed, or potentially crashed.
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                # Model crashed as simulation is not complete but no processes are running.
                statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
                                    " Failed. Attempting to restart."
                print statusData.genMsg
                errMod.sendMsg(statusData)
                keySlot[basinNum,iteration] = -0.25
                keyStatus = -0.25
                runFlag = True
                runCalib = False
            else:
                # Model has completed. Set to 0.75, which indicates calibration code
                # needs to be ran.
                # Clean up any previous iteration calib files if they are around.
                print "MODEL COMPLETED READY FOR MAIN CALIBRATION"
                try:
                    errMod.cleanCalib(statusData,workDir,runDir)
                    errMod.scrubParams(statusData,workDir)
                except:
                    raise
                keySlot[basinNum,iteration] = 0.75
                keyStatus = 0.75
                runFlag = False
                runCalib = True
                print keyStatus
                
    # For when the model simulation has completed, but the calibration is still 
    # listed as running.
    if keyStatus == 0.90:
        # If the calibration is still running, keep status as 0.90.
        if calibStatus:
            keySlot[basinNum,iteration] = 0.90
            keyStatus = 0.90
            runFlag = False
            runCalib = False
        else:
            # If calibration COMPLETE flag listed, upgrade status to 1.0, and make entry into
            # database as this iteration being completed. Also remove model output in preparation
            # for model simualation that will take place beginning with the next iteration.
            # Also scrub calib-related files (minus new parameters).
            if os.path.isfile(calibCompleteFlag):
                print "MAIN CALIBRATION COMPLETE! ENTERING DB INFO."
                try:
                    errMod.removeOutput(statusData,runDir)
                    errMod.cleanCalib(statusData,workDir,runDir)
                    db.logCalibParams(statusData,int(statusData.jobID),int(gageID),calibTbl,int(iteration))
                except:
                    raise
                keySlot[basinNum,iteration] = 1.0
                keyStatus = 1.0
                runFlag = False
                runCalib = False
                raise Exception()
            else:
                # This means the calibration failed. Demote status and send message to user.
                statusData.genMsg = "ERROR: Calibration Scripts failed for gage: " + statusData.gages[basinNum] + \
                                    " Iteration: " + str(iteration) + " Failed. Please remove LOCKFILE: " + calibLockPath
                print statusData.genMsg
                # Scrub calib-related files that were created as everything will need to be re-ran.
                try:
                    errMod.cleanCalib(statusData,workDir,runDir)
                    errMod.scrubParams(statusData,runDir)
                except:
                    raise
                open(calibLockPath,'a').close()
                errMod.sendMsg(statusData)
                keySlot[basinNum,iteration] = -0.75
                keyStatus = -0.75
                runFlag = False
                runCalib = False
                
    # For when the first calibration is running for the first iteration.
    if keyStatus == 0.25:
        # If calibration is still running, keep status as 0.10.
        if calibStatus:
            print "WE ARE DOING THE FIRST CALIBRATION FOR ITERATION 0."
            keySlot[basinNum,iteration] = 0.25
            keyStatus = 0.25
            runFlag = False
            runCalib = False
        else:
            # If calibration COMPLETE flag listed, upgrade status to 0.0 with runFlag on, signalling 
            # to proceed with model simulation. 
            print "CHECK FOR 1ST CALIB COMPLETENESS"
            print calibCompleteFlag
            if os.path.isfile(calibCompleteFlag):
                print "1ST CALIBRATION FOR ITERATION 0 COMPLETE"
                # Enter in parameters for iteration update.
                try:
                    db.logCalibParams(statusData,int(statusData.jobID),int(gageID),calibTbl,int(iteration))
                except:
                    raise
                keySlot[basinNum,iteration] = 0.0
                keyStatus = 0.0
                runFlag = True
                runCalib = False
            else:
                # This means the calibration failed. Demote status and send message to user.
                statusData.genMsg = "ERROR: 1st Calibration Scripts failed for gage: " + statusData.gages[basinNum] + \
                                    " Iteration: " + str(iteration) + " Failed. Please remove LOCK file: " + calibLockPath
                print statusData.genMsg
                # Scrub calib-related files that were created as everything will need to be re-ran.
                try:
                    errMod.scrubParams(statusData,runDir)
                except:
                    raise
                open(calibLockPath,'a').close()
                errMod.sendMsg(statusData)
                keySlot[basinNum,iteration] = -0.1
                keyStatus = -0.1
                runFlag = False
                runCalib = False
           
    # For iterations that are ready for run. The first step is to clean out old model output
    # and fire off simulation with newly updated model parameter values. 
    if keyStatus == 0.0:
        if iteration > 0:
            print "ON ITERATION GREATER THAN 0"
            # Iteration 0 needs to have an inital calibration procedure to generate
            # an initial parameter dataset.
            if os.path.isfile(lockPath):
                # Simulation was locked up. Set status to -1.0.
                keySlot[basinNum,iteration] = -1.0
                keyStatus = -1.0
                runFlag = False
                runCalib = False
            elif os.path.isfile(calibLockPath):
                # Calibration failed and locked directory up.
                keySlot[basinNum,iteration] = -0.75
                keyStatus = -0.75
                runFlag = False
                runCalib = False
            else:
                if basinStatus:
                    # Model is still running from previous instance of workflow. Allow it to continue.
                    keySlot[basinNum,iteration] = 0.5
                    keyStatus = 0.5
                    runFlag = False
                    runCalib = False
                else:
                    # Model is not running.
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
                        # Model simulation completed before workflow was restarted. Ready to 
                        # move onto calibration R code.
                        # Cleanup any previous calib-related files that may be sitting around.
                        print "MODEL COMPLETE, READY FOR MAIN CALIBRATION"
                        try:
                            errMod.cleanCalib(statusData,workDir,runDir)
                            errMod.scrubParams(statusData,workDir)
                        except:
                            raise
                        keySlot[basinNum,iteration] = 0.75
                        keyStatus = 0.75
                        runFlag = False
                        runCalib = True
                        if calibStatus:
                            # Model has completed, and calibration routines are currently being ran.
                            keySlot[basinNum,iteration] = 0.90
                            keyStatus = 0.90
                            runFlag = False
                            runCalib = False
                    else:
                        # Model is not running, but it's not complete. This means a simulation needs
                        # to be started or re-started.
                        # To be safe, cleanup any calib-related files (minus parameters) that
                        # may be lying around.
                        print "STARTING OR RESTARTING MODEL"
                        try:
                            errMod.cleanCalib(statusData,workDir,runDir)
                        except:
                            raise
                        runFlag = True
                        runCalib = False
        else:
            print "ON ITERATION 0"
            # Run calibration procedure to generate initial table of values to adjust
            # parameters with.
            if os.path.isfile(lockPath):
                print "MODEL LOCKED"
                # Simulation was locked up. Set status to -1.0.
                keySlot[basinNum,iteration] = -1.0
                keyStatus = -1.0
                runFlag = False
                runCalib = False
                print keyStatus
            else:
                if basinStatus:
                    print "MODEL IS RUNNING"
                    # This means that we are rebooting the program. Parameters were 
                    # adjusted already and the simulation for iteration 0 is underway
                    # still from a previous crash.
                    keySlot[basinNum,iteration] = 0.5
                    keyStatus = 0.5
                    runFlag = False
                    runCalib = False
                    print keyStatus
                else:
                    print "MODEL HAS NOT COMPLETED"
                    runStatus = statusMod.walkMod(begDate,endDate,runDir)
                    begDate = runStatus[0]
                    endDate = runStatus[1]
                    runFlag = runStatus[2]
                    if begDate == statusData.bCalibDate and runFlag:
                        # Model has not ran at all yet.
                        if calibStatus:
                            print "CALIBRATION PROGRAMS ARE RUNNING"
                            # First calibration taking place to prepare parameters for
                            # first iteration.
                            keySlot[basinNum,iteration] = 0.25
                            keyStatus = 0.25
                            runFlag = False
                            runCalib = False
                            print keyStatus
                        else:
                            if os.path.isfile(calibCompleteFlag):
                                print "FIRST CALIBRATION FOR ITERATION 0 HAS OCCURRED. READY TO RUN MODEL"
                                # First calibration completed. Ready to run the model.
                                # Cleanup any previou calib-related parameters (minus new parameter files)
                                keySlot[basinNum,iteration] = 0.0
                                keyStatus = 0.0
                                runFlag = True
                                runCalib = False
                            else:
                                print "FIRST CALIBRATION FOR ITERATION 0 HAS NOT OCCURRED YET."
                                # Check for LOCK file, meaning the first calibration failed.
                                if os.path.isfile(calibLockPath):
                                    print "FIRST CALIBRATION LOCKED"
                                    keySlot[basinNum,iteration] = -0.10
                                    keyStatus = -0.10
                                    runFlag = False
                                    runCalib = False
                                    print keyStatus
                                else:
                                    print "READY TO RUN FIRST CALIBRATION FOR ITERATION 0."
                                    # Run first calibration and parameter adjustment.
                                    # Clean all previous calib files that may be laying around.
                                    try:
                                        errMod.cleanCalib(statusData,workDir,runDir)
                                        errMod.scrubParams(statusMod,runDir)
                                    except:
                                        raise
                                    keySlot[basinNum,iteration] = 0.0
                                    keyStatus = 0.0
                                    runFlag = False
                                    runCalib = True
                    if begDate != statusData.bCalibDate and runFlag:
                        # This implies a general crash that did not result in the creation of
                        # a model LOCK file. In this case, set the status to 0.0 with the runFlag
                        # on to tell the workflow to skip the first calibration. This implies it has
                        # already occurred.
                        print "FIRST CALIBRATION FOR ITERATION 0 OCCURRED AND MODEL BEGAN, BUT CRASHED WITHOUT LOCK FILE"
                        keySlot[basinNum,iteration] = 0.0
                        keyStatus = 0.0
                        runFlag = True
                        runCalib = False
                        if not runFlag:
                            # Model has completed.
                            if calibStatus:
                                print "SECOND CALIBRATION FOR ITERATION 0 UNDERWAY"
                                # First calibration completed, model simulation completed, and second calibration
                                # underway.
                                keySlot[basinNum,iteration] = 0.90
                                keyStatus = 0.90
                                runFlag = False
                                runCalib = False
                                print keyStatus
                            else:
                                print "SECOND CALIBRATION FOR ITERATION 0 READY"
                                # First calibration completed, model simulation completed, second calibration 
                                # needs to be ran.
                                # Clean any previou calib files that may be laying around.
                                try:
                                    errMod.cleanCalib(statusData,workDir,runDir)
                                    errMod.scrubParams(statusMod,runDir)
                                except:
                                    raise
                                keySlot[basinNum,iteration] = 0.75
                                keyStatus = 0.75
                                runFlag = False
                                runCalib = True
                                print keyStatus
                
    # For when the model failed TWICE and is locked.
    if keyStatus == -1.0:
        # If LOCK file exists, no simulation will take place. File must be removed
        # manually by user.
        print "MODEL LOCKED"
        if os.path.isfile(lockPath):
            runFlag = False
            runCalib = False
            keySlot[basinNum,iteration] = -1.0
            keyStatus = -1.0
            print keyStatus
        else:
            # LOCK file was removed, upgrade status to 0.0 temporarily
            print "MODEL LOCK FILE REMOVED, READY TO RESTART MODEL"
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                keySlot[basinNum,iteration] = 0.0
                keyStatus = 0.0
                runFlag = True
                runCalib = False
            else:
                print "MODEL FINISHED AFTER LOCK FILE CREATED. READY FOR CALIBRATION"
                # Model sucessfully completed. Ready to move onto calibration R code
                # Remove any previous calibration files.
                try:
                    errMod.cleanCalib(statusData,workDir,runDir)
                    errMod.scrubParams(statusMod,runDir)
                except:
                    raise
                keySlot[basinNum,iteration] = 0.75
                keyStatus = 0.75
                runFlag = False
                runCalib = True
                print keyStatus
                
    # For when calibration R code and parameter adjustment failed.
    if keyStatus == -0.75:
        print "REGULAR CALIBRATION HAS LOCKED."
        # If LOCK file exists, no calibrations can take place. File must
        # be removed manually by user.
        if os.path.isfile(calibLockPath):
            # Cleanup any calibration related files.
            try:
                errMod.cleanCalib(statusData,workDir,runDir)
                errMod.scrubParams(statusMod,runDir)
            except:
                raise
            runFlag = False
            runCalib = False
            keySlot[basinNum,iteration] = -0.75
            keyStatus = -0.75
            print keyStatus
        else:
            print "CALIB LOCK FILE REMOVED, READY TO RESTART REGULAR CALIBRATION."
            # Cleanup any previous calib files that may be laying around.
            try:
                errMod.cleanCalib(statusData,workDir,runDir)
                errMod.scrubParams(statusMod,runDir)
            except:
                raise
            # LOCK file was removed, upgrade status.
            keySlot[basinNum,iteration] = 0.75
            keyStatus = 0.75
            runFlag = False
            runCalib = True
            print keyStatus
                    
    # For when the first calibration during the first iteration failed.
    if keyStatus == -0.1:
        print "FIRST CALIB FOR ITERATION 0 HAS BEEN LOCKED."
        # If LOCK file exists, no calibrations can take place. File must
        # be removed manually by user.
        if os.path.isfile(calibLockPath):
            keySlot[basinNum,iteration] = -0.10
            keyStatus = -0.10
            runFlag = False
            runCalib = False
            print keyStatus
        else:
            print "FIRST CALIB LOCK FILE REMOVED, READY TO RESTART 1ST CALIB FOR ITERATION 0"
            # Cleanup any previous calibration files
            try:
                errMod.cleanCalib(statusData,workDir,runDir)
                errMod.scrubParams(statusMod,runDir)
            except:
                raise
            # LOCK file was removed, upgrade status.
            keySlot[basinNum,iteration] = 0.0
            runFlag = False
            runCalib = True
                
    # For when the model crashed ONCE
    if keyStatus == -0.5:
        print "MODEL HAS CRASHED ONCE"
        if basinStatus:
            print "MODEL IS RUNNING AFTER FIRST CRASH"
            # Model is running again, upgrade status
            # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
            keySlot[basinNum,iteration] = 0.5
            keyStatus = 0.5
            runFlag = False
            runCalib = False
            print keyStatus
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                # Model has crashed again, time to lock it up and send a message out.
                statusData.genMsg = "ERROR: SIMULATION FOR GAGE: " + statusData.gages[basinNum] + \
                                    " HAS FAILED A SECOND TIME. PLEASE FIX ISSUE AND " + \
                                    "MANUALLY REMOVE LOCK FILE: " + lockPath
                errMod.sendMsg(statusData)
                print statusData.genMsg
                open(lockPath,'a').close()
                keySlot[basinNum,iteration] = -1.0
                keyStatus = -1.0
                runFlag = False
                runCalib = False
                print keyStatus
            else:
                print "MODEL COMPLETED. READY FOR REGULAR CALIBRATION"
                # Model sucessfully completed from first failed attempt. Ready for
                # calibration R code.
                # Cleanup any previous calib files.
                try:
                    errMod.cleanCalib(statusData,workDir,runDir)
                    errMod.scrubParams(statusMod,runDir)
                except:
                    raise
                keySlot[basinNum,iteration] = 0.75
                keyStatus = 0.75
                runFlag = False
                runCalib = True
                print keyStatus
                
    if keyStatus == -0.25:
        print "MODEL CRASHED ONCE. RESTARTING..."
        # Restarting model from one crash
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
            
        if begDate == staticData.bCalibDate:
            startType = 1
        else:
            startType = 2
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,1)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,1)
        except:
            raise
            
        # Make symbolic links as necssary.
        try:
            linkToRst(statusData,gage,runDir)
        except:
            raise
        # Fire off model.
        cmd = "bsub < " + runDir + "/run_NWM.sh"
        print begDate
        print endDate
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        # Revert statuses to -0.5 for next loop to convey the model crashed once. 
        keyStatus = -0.5
        keySlot[basinNum,iteration] = -0.5
        runFlag = False
        runCalib = False
        print keyStatus
        
    if keyStatus == 0.0 and runFlag:
        print "WE ARE READY TO RUN MODEL."
        # Model needs to be either ran, or restarted
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        if begDate == staticData.bCalibDate:
            startType = 1
        else:
            startType = 2
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,1)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,1)
        except:
            raise
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        # Make symbolic links as necssary.
        try:
            linkToRst(statusData,gage,runDir)
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
        keySlot[basinNum,iteration] = 0.5
        print keyStatus 
        
    if keyStatus == 0.0 and runCalib:
        print "WE ARE ON ITERATION 0. NEED TO RUN FIRST SET OF CALIB PROGRAMS."
        # Unique situation where we are on iteration 1, and we need to run
        # a calibration script and adjust parameters once before beginning
        # the model.
        
        # First cleanup any old model output or calibration output that 
        # is from previous iterations.
        try:
            errMod.removeOutput(statusData,runDir)
            errMod.cleanCalib(statusData,workDir,runDir)
            errMod.scrubParams(statusData,runDir)
        except:
            raise
            
        # Fire off calibration programs.
        cmd = "bsub < " + workDir + "/run_NWM_CALIB.sh"
        print cmd
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM Calib job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.25
        keySlot[basinNum] = 0.25
        print keyStatus
    
                
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
        inStr = "#BSUB -n " + str(jobData.nCoresMod) + '\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -R "span[ptile=16]"\n')
        inStr = "#BSUB -J NWM_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
        fileObj.write(inStr)
        inStr = '#BSUB -o ' + runDir + '/%J.out\n'
        fileObj.write(inStr)
        inStr = '#BSUB -e ' + runDir + '/%J.err\n'
        fileObj.write(inStr)
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
        
def generateRScript(jobData,gageMeta,gageNum):
    """
    Generic function to create R script that will be sourced by R during
    calibration.
    """
    outPath = jobData.outDir + "/" + jobData.jobName + "/" + str(jobData.gages[gageNum]) + \
              "/RUN.CALIB/calibScript.R"
    
    if os.path.isfile(outPath):
        jobData.errMsg = "ERROR: Calibration R script: " + outPath + " aleady exists."
        raise Exception()

    try:
        fileObj = open(outPath,'w')
        fileObj.write('#### Model Parameters ####\n')
        inStr = "objFn <- \"" + str(jobData.objFunc) + "\"\n"
        fileObj.write(inStr)
        fileObj.write('# Specify number of calibration iterations.\n')
        inStr = "m <- " + str(jobData.nIter) + '\n'
        fileObj.write(inStr)
        fileObj.write('# Specify DDS parameter (if used).\n')
        inStr = "r <- " + str(jobData.ddsR) + "\n"
        fileObj.write(inStr)
        fileObj.write("# Specify run directory containing calibration simulations.\n")
        inStr = "runDir <- '" + jobData.outDir + "/" + jobData.jobName + "/" + \
                str(jobData.gages[gageNum]) + "/RUN.CALIB'\n"
        fileObj.write(inStr)
        #fileObj.write('# Parameter bounds\n')
        #fileObj.write('# Must create a data table called paramBnds with one row per parameter and columns labeled: \n')
        #fileObj.write('# "param" for parameter name, "ini" for initial value, "minValue" for minimum value, "maxValue" for maximum value\n')
        #inStr = "paramBnds <- read.table(paste0(runDir, '/calib_parms.tbl'), header=TRUE, sep=" ", stringsAsFactors=FALSE)\n"
        fileObj.write('# Basin-Specific Metadata\n')
        inStr = "siteId <- '" + str(jobData.gages[gageNum]) + "'\n"
        fileObj.write(inStr)
        inStr = "linkId <- " + str(gageMeta.comID) + "\n"
        fileObj.write(inStr)
        fileObj.write('# Start date for evaluation period (e.g., after spinup period)\n')
        inStr = "startDate <- as.POSIXct(\"" + jobData.bCalibEvalDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        fileObj.write('# Specify number of cores to use\n')
        inStr = "ncores <- " + str(jobData.nCoresR) + "\n"
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outPath
        raise        
        
def generateCalibScript(jobData,gageID,runDir,workDir):
    """
    Generic Function function to create BSUB script for running R
    calibration routines. These jobs will be shorter than 
    the model runs, but still need to be ran through Yellowstone
    compute nodes. This function also creates the shell script that
    will execute R and Python to modify parameters.
    """
    
    outFile1 = workDir + "/run_NWM_CALIB.sh"
    
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            fileObj.write('# LSF Batch Script to Run NWM Calibration R Code\n')
            fileObj.write('#\n')
            inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
            fileObj.write(inStr)
            #fileObj.write('#BSUB -x\n')
            #inStr = "#BSUB -n " + str(jobData.nCoresR) + '\n'
            #fileObj.write(inStr)
            fileObj.write("#BSUB -n 1\n")
            fileObj.write('#BSUB -R "span[ptile=1]"\n')
            inStr = "#BSUB -J NWM_CALIB_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#BSUB -o ' + workDir + '/%J.out\n'
            fileObj.write(inStr)
            inStr = '#BSUB -e ' + workDir + '/%J.err\n'
            fileObj.write(inStr)
            fileObj.write('#BSUB -W 3:00\n')
            fileObj.write('#BSUB -q premium\n')
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('mpirun.lsf ./calibCmd.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/calibCmd.sh"
    
    runRProgram = workDir + "/calib_workflow.R"
    srcScript = workDir + "/calibScript.R"
    if not os.path.isfile(srcScript):
        jobData.errMsg = "ERROR: Necessary R script file: " + srcScript + " not found."
        raise
        
    if not os.path.isfile(outFile2):
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + " " + srcScript + '\n')
            fileObj.write('python ' + workDir + '/adjust_parameters.py ' + workDir + ' ' + runDir + ' \n')
            fileObj.write('exit\n')
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile2
            raise
            
    # Make shell script an executable.
    cmd = 'chmod +x ' + outFile2
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + outFile2 + " to an executable."
        raise
        
def linkToRst(statusData,gage,runDir):
    """
    Generic function to link to necessary restart files from the spinup.
    This was broken out as a function as sometimes the output directory
    is scrubbed, and links need to be re-made in preparation for a new 
    iteration simulation.
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