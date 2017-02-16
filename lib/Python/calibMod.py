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
# TEMPORARY
import shutil

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
    if iteration > 0:
        if keySlot[basinNum,iteration-1] < 1.0:
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
     
    # Create path to LOCK file if neeced
    lockPath = workDir + "/RUN.LOCK"
    calibLockPath = workDir + "/CALIB.LOCK"
    calibCompleteFlag = workDir + "/CALIB_ITER.COMPLETE"
    missingFlag = workDir + "/CALC_STATS_MISSING"
    calibTbl = workDir + "/params_new.txt"
    statsTbl = workDir + "/params_stats.txt"
    rDataFile = workDir + "/proj_data.Rdata"
    
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
                try:
                    errMod.cleanCalib(statusData,workDir,runDir)
                    errMod.scrubParams(statusData,workDir)
                except:
                    raise
                keySlot[basinNum,iteration] = 0.75
                keyStatus = 0.75
                runFlag = False
                runCalib = True
                
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
                try:
                    # If we are on the last iteration, no new parameters are created.
                    if int(iteration+1) < int(statusData.nIter):
                        try:
                            db.logCalibParams(statusData,int(statusData.jobID),int(gageID),calibTbl,int(iteration)+1)
                        except:
                            raise
                    db.logCalibStats(statusData,int(statusData.jobID),int(gageID),str(gage),int(iteration),statsTbl)
                    errMod.removeOutput(statusData,runDir)
                    errMod.cleanCalib(statusData,workDir,runDir)
                except:
                    raise
                # TEMPORARY TO MAKE ARCHIVE OF PARAMETER FILES
                # Make a copy of files, named based on iteration.
                inPath = runDir + "/Fulldom.nc"
                outPath = runDir + "/Fulldom_" + str(iteration+2) + ".nc"
                if os.path.isfile:
                    shutil.copy(inPath,outPath)
                inPath = runDir + "/HYDRO.TBL"
                outPath = runDir + "/HYDRO_" + str(iteration+2) + ".TBL"
                if os.path.isfile:
                    shutil.copy(inPath,outPath)
                inPath = runDir + "/soil_properties.nc"
                outPath = runDir + "/soil_properties_" + str(iteration+2) + ".nc"
                if os.path.isfile:
                    shutil.copy(inPath,outPath)
                inPath = runDir + "/GWBUCKPARM.nc"
                outPath = runDir + "/GWBUCKPARM_" + str(iteration+2) + ".nc"
                if os.path.isfile:
                    shutil.copy(inPath,outPath)
                # END TEMPORARY
                keySlot[basinNum,iteration] = 1.0
                keyStatus = 1.0
                runFlag = False
                runCalib = False
            elif os.path.isfile(missingFlag):
                # This is a unique situation where either an improper COMID (linkID) was passed to 
                # the R program, pulling NA from the model. Or, the observations file contains 
                # all missing values. For this, convey this to the user through a message, set the
                # status for all iterations to 1.
                statusData.genMsg = "WARNING: Either a bad COMID exists for this gage, or there are no " + \
                                    "observations for the evaluation period."
                print statusData.genMsg
                errMod.sendMsg(statusData)
                # set the status for all iterations to 1.
                try:
                    db.fillMisingBasin(statusData,int(statusData.jobID),int(gageID))
                except:
                    raise
                # Clean everything up.
                try:
                    errMod.removeOutput(statusData,runDir)
                    errMod.cleanCalib(statusData,workDir,runDir)
                    errMod.scrubParams(statusData,runDir)
                except:
                    raise
                keySlot[basinNum,:] = 1.0
                keyStatus = 1.0
                runFlag = False
                runCalib = False
            else:
                # This means the calibration failed. Demote status and send message to user.
                statusData.genMsg = "ERROR: Calibration Scripts failed for gage: " + statusData.gages[basinNum] + \
                                    " Iteration: " + str(iteration) + " Failed. Please remove LOCKFILE: " + calibLockPath
                print statusData.genMsg
                errMod.sendMsg(statusData)
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
            keySlot[basinNum,iteration] = 0.25
            keyStatus = 0.25
            runFlag = False
            runCalib = False
        else:
            # If calibration COMPLETE flag listed, upgrade status to 0.0 with runFlag on, signalling 
            # to proceed with model simulation. 
            if os.path.isfile(calibCompleteFlag):
                # Copy parameter files to the DEFAULT directory
                try:
                    calibIoMod.copyDefaultParms(statusData,runDir,gage)
                except:
                    raise
                # Enter in parameters for iteration update.
                try:
                    db.logCalibParams(statusData,int(statusData.jobID),int(gageID),calibTbl,int(iteration))
                except:
                    raise
                keySlot[basinNum,iteration] = 0.0
                keyStatus = 0.0
                runFlag = True
                runCalib = False
            elif os.path.isfile(missingFlag):
                # This is a unique situation where either an improper COMID (linkID) was passed to 
                # the R program, pulling NA from the model. Or, the observations file contains 
                # all missing values. For this, convey this to the user through a message, set the
                # status for all iterations to 1.
                statusData.genMsg = "WARNING: Either a bad COMID exists for this gage, or there are no " + \
                                    "observations for the evaluation period."
                print statusData.genMsg
                # Copy parameter files to the DEFAULT directory
                try:
                    calibIoMod.copyDefaultParms(statusData,runDir,gage)
                except:
                    raise
                # set the status for all iterations to 1.
                try:
                    db.fillMisingBasin(statusData,int(statusData.jobID),int(gageID))
                except:
                    raise
                # Clean everything up.
                try:
                    errMod.removeOutput(statusData,runDir)
                    errMod.cleanCalib(statusData,workDir,runDir)
                    errMod.scrubParams(statusData,runDir)
                except:
                    raise
                keySlot[basinNum,:] = 1.0
                keyStatus = 1.0
                runFlag = False
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
                    runStatus = statusMod.walkMod(begDate,endDate,runDir)
                    begDate = runStatus[0]
                    endDate = runStatus[1]
                    runFlag = runStatus[2]
                    if not runFlag:
                        # Model simulation completed before workflow was restarted. Ready to 
                        # move onto calibration R code. However.... to be safe, we are going
                        # to re-run the model. There are a couple cracks that occur when we assume
                        # this is the current model. If a previous model completed, but wasn't 
                        # completely wiped clean, this will screw up calibration statistics. 
                        # Cleanup any previous calib-related files that may be sitting around.
                        try:
                            errMod.cleanCalib(statusData,workDir,runDir)
                            errMod.scrubParams(statusData,runDir)
                        except:
                            raise
                        keySlot[basinNum,iteration] = 0.0
                        keyStatus = 0.0
                        runFlag = True
                        runCalib = False
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
                        try:
                            errMod.cleanCalib(statusData,workDir,runDir)
                        except:
                            raise
                        keySlot[basinNum,iteration] = 0.0
                        keyStatus = 0.0
                        runFlag = True
                        runCalib = False
        else:
            # Run calibration procedure to generate initial table of values to adjust
            # parameters with.
            if os.path.isfile(lockPath):
                # Simulation was locked up. Set status to -1.0.
                keySlot[basinNum,iteration] = -1.0
                keyStatus = -1.0
                runFlag = False
                runCalib = False
            elif os.path.isfile(calibLockPath):
                # Calibration failed and locked directory up.
                # Double check to make sure this is not the first calib that 
                # failed.
                if not basinStatus:
                    runStatus = statusMod.walkMod(begDate,endDate,runDir)
                    begDate = runStatus[0]
                    endDate = runStatus[1]
                    runFlag = runStatus[2]
                    if begDate == statusData.bCalibDate and runFlag:
                        # The first calib for iteration 1 failed.
                        keySlot[basinNum,iteration] = -0.10
                        keyStatus = -0.10
                        runFlag = False
                        runCalib = False
                    else:
                        # The main calib failed.
                        keySlot[basinNum,iteration] = -0.75
                        keyStatus = -0.75
                        runFlag = False
                        runCalib = False
            else:
                if basinStatus:
                    # This means that we are rebooting the program. Parameters were 
                    # adjusted already and the simulation for iteration 0 is underway
                    # still from a previous crash.
                    keySlot[basinNum,iteration] = 0.5
                    keyStatus = 0.5
                    runFlag = False
                    runCalib = False
                else:
                    runStatus = statusMod.walkMod(begDate,endDate,runDir)
                    begDate = runStatus[0]
                    endDate = runStatus[1]
                    runFlag = runStatus[2]
                    if begDate == statusData.bCalibDate and runFlag:
                        # Model has not ran at all yet.
                        if calibStatus:
                            # First calibration taking place to prepare parameters for
                            # first iteration.
                            keySlot[basinNum,iteration] = 0.25
                            keyStatus = 0.25
                            runFlag = False
                            runCalib = False
                        else:
                            if os.path.isfile(calibCompleteFlag):
                                # First calibration completed. Ready to run the model.
                                # Cleanup any previou calib-related parameters (minus new parameter files)
                                keySlot[basinNum,iteration] = 0.0
                                keyStatus = 0.0
                                runFlag = True
                                runCalib = False
                            else:
                                # Check for LOCK file, meaning the first calibration failed.
                                if os.path.isfile(calibLockPath):
                                    if os.path.isfile(rDataFile):
                                        os.remove(rDataFile)
                                    keySlot[basinNum,iteration] = -0.10
                                    keyStatus = -0.10
                                    runFlag = False
                                    runCalib = False
                                else:
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
                        keySlot[basinNum,iteration] = 0.0
                        keyStatus = 0.0
                        runFlag = True
                        runCalib = False
                        if not runFlag:
                            # Model has completed.
                            if calibStatus:
                                # First calibration completed, model simulation completed, and second calibration
                                # underway.
                                keySlot[basinNum,iteration] = 0.90
                                keyStatus = 0.90
                                runFlag = False
                                runCalib = False
                            else:
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
                    if begDate == statusData.eCalibDate and not runFlag:
                        # Both the first calibration and model simulation completed. Ready for
                        # second (main) calibration.
                        try:
                            errMod.cleanCalib(statusData,workDir,runDir)
                            errMod.scrubParams(statusMod,runDir)
                        except:
                            raise
                        keySlot[basinNum,iteration] = 0.75
                        keyStatus = 0.75
                        runFlag = False
                        runCalib = True
                
    # For when the model failed TWICE and is locked.
    if keyStatus == -1.0:
        # If LOCK file exists, no simulation will take place. File must be removed
        # manually by user.
        if os.path.isfile(lockPath):
            runFlag = False
            runCalib = False
            keySlot[basinNum,iteration] = -1.0
            keyStatus = -1.0
        else:
            # LOCK file was removed, upgrade status to 0.0 temporarily
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
                
    # For when calibration R code and parameter adjustment failed.
    if keyStatus == -0.75:
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
        else:
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
                    
    # For when the first calibration during the first iteration failed.
    if keyStatus == -0.1:
        # If LOCK file exists, no calibrations can take place. File must
        # be removed manually by user.
        if os.path.isfile(calibLockPath):
            if os.path.isfile(rDataFile):
                os.remove(rDataFile)
            keySlot[basinNum,iteration] = -0.10
            keyStatus = -0.10
            runFlag = False
            runCalib = False
        else:
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
        if basinStatus:
            # Model is running again, upgrade status
            # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
            keySlot[basinNum,iteration] = 0.5
            keyStatus = 0.5
            runFlag = False
            runCalib = False
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
            else:
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
                
    if keyStatus == -0.25:
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
            
        # Double check to make sure all old calibration files have been cleaned up, except for newly
        # created parameter files.
        try:
            errMod.cleanCalib(statusData,workDir,runDir)
        except:
            raise
        # Fire off model.
        cmd = "bsub < " + runDir + "/run_NWM.sh"
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
        
    if keyStatus == 0.0 and runFlag:
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
                
        # clean up old calibration related files, except for new parameter files.
        try:
            errMod.cleanCalib(statusData,workDir,runDir)
        except:
            raise
            
        # Fire off model.
        cmd = "bsub < " + runDir + "/run_NWM.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.5
        keySlot[basinNum,iteration] = 0.5
        
    if keyStatus == 0.0 and runCalib:
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
            
        # If any proj_data.Rdata exists, remove it as it might have been from a failed first attempt.
        if os.path.isfile(workDir + '/proj_data.Rdata'):
            try:
                os.remove(workDir + '/proj_data.Rdata')
            except:
                statusData.errMsg = "ERROR: Failure to remove: " + workDir + "/proj_data.Rdata"
                raise
            
        try:
            generateRScript(staticData,gageMeta,gage,int(iteration))
        except:
            statusData.errMsg = "ERROR: Failure to write calibration R script."
            raise
            
        # Fire off calibration programs.
        cmd = "bsub < " + workDir + "/run_NWM_CALIB.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM Calib job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.25
        keySlot[basinNum,iteration] = 0.25
        
    if keyStatus == 0.75 and runCalib:
        # Fire off calibration for simulation.
        
        # First cleanup any old calibration related files. This should have 
        # already been done per workflow, but this is a fail safe.
        try:
            errMod.cleanCalib(statusData,workDir,runDir)
            errMod.scrubParams(statusMod,runDir)
        except:
            raise
            
        try:
            generateRScript(staticData,gageMeta,gage,int(iteration)+1)
        except:
            statusData.errMsg = "ERROR: Failure to write calibration R script."
            raise
            
        # Fire off calibration program.
        cmd = "bsub < " + workDir + "/run_NWM_CALIB.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM Calib job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.90
        keySlot[basinNum,iteration] = 0.90
        
    # Update job status in the Database table.
    try:
        db.updateIterationStatus(statusData,int(gageMeta.gageID),iteration,str(gageMeta.gage),keyStatus)
    except:
        raise
    
                
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
        
def generateRScript(jobData,gageMeta,gageNum,iteration):
    """
    Generic function to create R script that will be sourced by R during
    calibration.
    """
    outPath = jobData.outDir + "/" + jobData.jobName + "/" + str(gageMeta.gage) + \
              "/RUN.CALIB/calibScript.R"
              
    if os.path.isfile(outPath):
        # Over-write to update mCurrent to reflect current iteration
        os.remove(outPath)
    
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
        inStr = "mCurrent <- " + str(iteration) + "\n"
        fileObj.write(inStr)
        fileObj.write('# Specify DDS parameter (if used).\n')
        inStr = "r <- " + str(jobData.ddsR) + "\n"
        fileObj.write(inStr)
        fileObj.write("# Specify run directory containing calibration simulations.\n")
        inStr = "runDir <- '" + jobData.outDir + "/" + jobData.jobName + "/" + \
                str(gageMeta.gage) + "/RUN.CALIB'\n"
        fileObj.write(inStr)
        fileObj.write('# Basin-Specific Metadata\n')
        inStr = "siteId <- '" + str(gageMeta.gage) + "'\n"
        fileObj.write(inStr)
        inStr = "rtlinkFile <- '" + str(gageMeta.rtLnk) + "'\n"
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
            inStr = "#BSUB -n " + str(jobData.nCoresR) + '\n'
            fileObj.write(inStr)
            #fileObj.write("#BSUB -n 1\n")
            #fileObj.write('#BSUB -R "span[ptile=16]"\n')
            inStr = "#BSUB -J NWM_CALIB_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#BSUB -o ' + workDir + '/%J.out\n'
            fileObj.write(inStr)
            inStr = '#BSUB -e ' + workDir + '/%J.err\n'
            fileObj.write(inStr)
            fileObj.write('#BSUB -W 0:20\n')
            fileObj.write('#BSUB -q premium\n')
            #fileObj.write('#BSUB -q geyser\n')
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./calibCmd.sh\n')
            #fileObj.write('mpirun.lsf ./calibCmd.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/calibCmd.sh"
    
    runRProgram = workDir + "/calib_workflow.R"
    srcScript = workDir + "/calibScript.R"
        
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