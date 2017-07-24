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
import time

import warnings
warnings.filterwarnings("ignore")

def runModelCtrl(statusData,staticData,db,gageID,gage,keySlot,basinNum,libPathTop):
    """
    Generic function for running the model. Some basic information about
    the run directory, beginning date, ending dates, account keys,
    number of cores to use, etc will be used to compose a BSUB/QSUB
    submision script or run mpiexec/mpirun. This function will walk the run directory 
    to determine where the model left off. If no restart files exist,
    then the function will assume the model has not ran at all. Both
    the LSM and hydro restart files must be present in order for the
    model to restart. 
    """
    # Establish the "control" and "best" status values. These are important for 
    # the workflow.
    ctrlStatus = keySlot[basinNum,0]
    bestStatus = keySlot[basinNum,1]
    
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
    
    print "JOB ID = " + str(statusData.jobID)
    # Establish directory paths.
    runDir = statusData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/CTRL"
    bestDir = statusData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/BEST"
    parmInDir = statusData.jobDir + "/" + gage + "/RUN.CALIB/BASELINE_PARAMETERS"
    workDir = statusData.jobDir + "/" + gage + "/RUN.VALID"
    
    if not os.path.isdir(runDir):
        statusData.errMsg = "ERROR: " + runDir + " not found."
        raise Exception()
    if not os.path.isdir(parmInDir):
        statusData.errMsg = "ERROR: " + parmInDir + " not found."
        raise Exception()
    if not os.path.isdir(bestDir):
        statusData.errMsg = "ERROR: " + bestDir + " not found."
        raise Exception()
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
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
        
    # Make symbolic links as necssary.
    try:
        linkToRst(statusData,gage,runDir)
    except:
        raise
        
    # Create symbolic links to Python/R code to generate parameters and evaluate
    # model output if it hasn't already been created.
    parmGenProgram = libPathTop + "/Python/generate_parameters.py"
    evalProgram = libPathTop + "/R/valid_workflow.R"
    utilProgram = libPathTop + "/R/calib_utils.R"
    try:
        link = bestDir + "/generate_parameters.py"
        if not os.path.islink(link):
            os.symlink(parmGenProgram,link)
    except:
        statusData.errMsg = "ERROR: Failure to link: " + parmGenProgram
        raise
    try:
        link = workDir + "/valid_workflow.R"
        if not os.path.islink(link):
            os.symlink(evalProgram,link)
    except:
        statusData.errMsg = "ERROR: Failure to link: " + evalProgram
        raise
    try:
        link = workDir + "/calib_utils.R"
        if not os.path.islink(link):
            os.symlink(utilProgram,link)
    except:
        statusData.errMsg = "ERROR: Failure to link: " + utilProgram
        raise
        
    # Create two run scripts:
    # 1.) Job script to prepare parameter files for both the control and best
    #     model simulations.
    # 2.) Job script to run the model with control parameters.
    parmRunScript = runDir + "/gen_parms.sh"
    bsub1Script = runDir + "/run_parms.sh"
    bsub2Script = runDir + "/run_NWM.sh"
    
    # If the files exist, remove them and re-create.
    if os.path.isfile(parmRunScript):
        os.remove(parmRunScript)
    if os.path.isfile(bsub1Script):
        os.remove(bsub1Script)
    if os.path.isfile(bsub2Script):
        os.remove(bsub2Script)
    
    if statusData.jobRunType == 1:
        # Generate the shell script to call Python to generate parameter datasets
        # in preparation for model runs.     
        try:
            generateParmScript(statusData,bestDir,gage,parmInDir)
        except:
            raise
        # Generate the BSUB script to run the parameter generation code. 
        try:
            generateBsubParmRunScript(statusData,bestDir,gageID)
        except:
            raise
        # Generate the BSUB run script to run the model simulations. 
        try:
            generateBsubRunScript(statusData,gageID,runDir,gageMeta,'CTRL')
        except:
            raise
    if statusData.jobRunType == 4:
        # Generate the shell script to call Python to generate parameter datasets
        # in preparation for model runs.
        try:
            generateParmScript(statusData,bestDir,gage,parmInDir)
        except:
            raise
        # Generate the mpiexec script to run the parameter generation 
        try:
            generateMpiexecParmRunScript(statusData,bestDir,gageID)
        except:
            raise
        # Generate the BSUB run script to run the model simulations. 
        try:
            generateMpiexecRunScript(statusData,gageID,runDir,gageMeta,'CTRL')
        except:
            raise

    # Calculate datetime objects
    begDate = statusData.bValidDate
    endDate = statusData.eValidDate
        
    ## Initialize status
    keyStatus = keySlot[basinNum,0]
    
    # Pull status values for parameter generation and model simulations. 
    try:
        basinStatus = statusMod.checkBasJobValid(statusData,basinNum,'CTRL')
    except:
        raise
    try:
        genParmStatus = statusMod.checkParmGenJob(statusData,basinNum)
    except:
        raise
     
    # Create path to LOCK file if neeced
    lockPath = runDir + "/RUN.LOCK"
    parmLockPath = runDir + '/PARM_GEN.LOCK'    
    
    # Path that will define when the parameter generation has completed.
    genParmComplete = bestDir + "/PARAM_GEN.COMPLETE"
    
    if keyStatus == 1.0:
        # Model has already completed
        runFlag = False
        return
        
    if keyStatus == 0.1:
        # Parameter generation code is running. 
        if genParmStatus:
            # Parameter generation code is still running.
            keySlot[basinNum,0] = 0.1
            keyStatus = 0.1
            runFlag = False
        else:
            # Check to make sure complete flag is present.
            if os.path.isfile(genParmComplete):
                # Parameter generation complete. Ready to run model. 
                keySlot[basinNum,0] = 0.25
                keyStatus = 0.25
                runFlag = True
            else:
                # Parameter generation code failed. Lock up basin and 
                # send error message out.
                statusData.genMsg = "ERROR: Parameter generation failed for gage: " + statusData.gages[basinNum] + \
                                    " Failed. Please remove LOCKFILE: " + parmLockPath
                open(parmLockPath,'a').close()
                errMod.sendMsg(statusData)
                keySlot[basinNum,0] = -0.1
                keyStatus = -0.1
                runFlag = False
        
    # For uncompleted simulations that are still listed as running.
    if keyStatus == 0.5:
        # If a model is running for this basin, continue and set keyStatus to 0.5
        if basinStatus:
            keySlot[basinNum,0] = 0.5
            keyStatus = 0.5
            runFlag = False
        else:
            # Either simulation has completed, or potentially crashed.
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                # Model crashed as simulation is not complete but no processes are running.
                #statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
                #                    " Failed. Attempting to restart."
                #print statusData.genMsg
                #errMod.sendMsg(statusData)
                keySlot[basinNum,0] = -0.25
                keyStatus = -0.25
                runFlag = True
            else:
                # Model has completed!
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                runFlag = False
           
    # For simulations that are fresh
    if keyStatus == 0.0:
        if basinStatus:
            # Model is still running from previous instance of workflow. Allow it to continue.
            keySlot[basinNum,0] = 0.5
            keyStatus = 0.5
            runFlag = False
        elif genParmStatus:
            # Parameter generation code is running.
            keySlot[basinNum,0] = 0.1
            keyStatus = 0.1
            runFlag = False
        elif os.path.isfile(lockPath):
            keySlot[basinNum,0] = -1.0
            keyStatus = -1.0
            runFlag = False
        elif os.path.isfile(parmLockPath):
            keySlot[basinNum,0] = -0.1
            keyStatus = -0.1
            runFlag = False
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if not runFlag and os.path.isfile(genParmComplete):
                # Model simulation completed before workflow was restarted.
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                runFlag = False
            if runFlag and not os.path.isfile(genParmComplete):
                # Model hasn't ran, and parameter generation code hasn't ran yet.
                keySlot[basinNum,0] = 0.0
                keyStatus = 0.0
                runFlag = False
            if runFlag and os.path.isfile(genParmComplete):
                # parameter generation code has completed, and the model either
                # needs to be restarted or begun.
                keySlot[basinNum,0] = 0.25
                keyStatus = 0.25
                runFlag = True
                
    # For when parameter estimation code failed and is locked up. 
    if keyStatus == -0.1:
        # If LOCK file exists. File must
        # be removed manually by user.
        if os.path.isfile(parmLockPath):
            keySlot[basinNum,0] = -0.1
            keyStatus = -0.1
            runFlag = False
        else:
            # LOCK file was removed, upgrade status.
            keySlot[basinNum,0] = 0.0
            runFlag = False

    # For when the model failed TWICE and is locked.
    if keyStatus == -1.0:
        # If LOCK file exists, no simulation will take place. File must be removed
        # manually by user.
        if os.path.isfile(lockPath):
            runFlag = False
        else:
            # LOCK file was removed, upgrade status to 0.25 temporarily
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                keySlot[basinNum,0] = 0.25
                keyStatus = 0.25
            else:
                # Model sucessfully completed.
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                runFlag = False
                
    # For when the model crashed ONCE
    if keyStatus == -0.5:
        if basinStatus:
            # Model is running again, upgrade status
            # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
            keySlot[basinNum,0] = 0.5
            keyStatus = 0.5
            runFlag = False
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
                keySlot[basinNum,0] = -1.0
                keyStatus = -1.0
                runFlag = False
            else:
                # Model sucessfully completed from first failed attempt.
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                
    if keyStatus == -0.25 and runFlag:
        # Restarting model from one crash
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        # Since these are validation simulations, we are always going to be 
        # starting the model rom an existing RESTART file. startType = 1 is for
        # when we have cold starts. 
        startType = 2
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,2)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,2)
        except:
            raise
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        if statusData.jobRunType == 1:
            # Fire off model.
            cmd = "bsub < " + runDir + "/run_NWM.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
        if statusData.jobRunType == 4:
            cmd = runDir + "/run_NWM.sh"
            try:
                p = subprocess.Popen([cmd],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
            
        # Revert statuses to -0.5 for next loop to convey the model crashed once. 
        keyStatus = -0.5
        keySlot[basinNum,0] = -0.5
        
    if keyStatus == 0.25 and runFlag:
        # Model needs to be either ran, or restarted
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        # Since these are validation simulations, we are always going to be 
        # starting the model rom an existing RESTART file. startType = 1 is for
        # when we have cold starts. 
        startType = 2
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,2)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,2)
        except:
            raise
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        if statusData.jobRunType == 1:
            # Fire off model.
            cmd = "bsub < " + runDir + "/run_NWM.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
        if statusData.jobRunType == 4:
            cmd = runDir + "/run_NWM.sh"
            try:
                p = subprocess.Popen([cmd],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
            
        keyStatus = 0.5
        keySlot[basinNum,0] = 0.5
        
    if keyStatus == 0.0 and not runFlag:
        # We need to run parameter generation code.
        if statusData.jobRunType == 1:
            cmd = "bsub < " + bestDir + "/run_params.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch parameter generation job for gage: " + str(gageMeta.gage[basinNum])
                raise
        if statusData.jobRunType == 4:
            print gageMeta.gage
            cmd = bestDir + "/P" + str(statusData.jobID) + str(gageID)
            print cmd
            try:
                p2 = subprocess.Popen([str(cmd)],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                time.sleep(5)
            except:
                statusData.errMsg = "ERROR: Unable to launch parameter generation job for gage: " + str(gageMeta.gage[basinNum])
                raise
            
        keyStatus = 0.1
        keySlot[basinNum,0] = 0.1
            
def runModelBest(statusData,staticData,db,gageID,gage,keySlot,basinNum):
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
        
    # If the best status is 1.0, this means the model is complete and we can 
    # return to the main workflow calling program.
    if bestStatus == 1.0:
        return
        
    # Pull gage metadata
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
    
    # Establish directory paths.
    runDir = statusData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/BEST"
    ctrlDir = statusData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/CTRL"
    calibWorkDir = statusData.jobDir + "/" + gage + "/RUN.CALIB"
    validWorkDir = statusData.jobDir + "/" + gage + "/RUN.VALID"
    
    if not os.path.isdir(runDir):
        statusData.errMsg = "ERROR: " + runDir + " not found."
        raise Exception()
    if not os.path.isdir(ctrlDir):
        statusData.errMsg = "ERROR: " + ctrlDir + " not found."
        raise Exception()
    if not os.path.isdir(calibWorkDir):
        statusData.errMsg = "ERROR: " + calibWorkDir + " not found."
        raise Exception()
    if not os.path.isdir(validWorkDir):
        statusData.errMsg = "ERROR: " + validWorkDir + " not found."
        raise Exception()
        
    # Make symbolic links as necssary.
    try:
        linkToRst(statusData,gage,runDir)
    except:
        raise
        
    # Create two run scripts:
    # 1.) Job script to run the model with best parameters.
    # 2.) Job script to run the R code for evaluation/plotting.
    if statusData.jobRunType == 1:
        bsubRunScript = runDir + "/run_NWM.sh"
        bsubEvalScript = validWorkDir + "/run_eval.sh"
    
        # If the files exist, remove them and re-create.
        if os.path.isfile(bsubRunScript):
            os.remove(bsubRunScript)
        if os.path.isfile(bsubEvalScript):
            os.remove(bsubEvalScript)
        
        # Generate scripts to do evaluation on both 
        # the control and best simulations. 
        try:
            generateBsubEvalRunScript(staticData,statusData.jobID,gageID,runDir,gageMeta,calibWorkDir,validWorkDir)
        except:
            raise
        # Generate the BSUB run script to run the model simulations. 
        try:
            generateBsubRunScript(statusData,gageID,runDir,gageMeta,'BEST')
        except:
            raise
    if statusData.jobRunType == 4:
        runScript = runDir + "/run_NWM.sh"
        evalScript = validWorkDir + "/E" + str(statusData.jobID) + str(gageID)
    
        # If the files exist, remove them and re-create.
        if os.path.isfile(runScript):
            os.remove(runScript)
        if os.path.isfile(evalScript):
            os.remove(evalScript)
        
        # Generate scripts to do evaluation on both 
        # the control and best simulations. 
        try:
            generateMpiexecEvalRunScript(staticData,statusData.jobID,gageID,runDir,gageMeta,calibWorkDir,validWorkDir)
        except:
            raise
        # Generate the BSUB run script to run the model simulations. 
        try:
            generateMpiexecRunScript(statusData,gageID,runDir,gageMeta,'BEST')
        except:
            raise
        
    # Calculate datetime objects
    begDate = statusData.bValidDate
    endDate = statusData.eValidDate
        
    ## Initialize status
    keyStatus = keySlot[basinNum,1]
    
    # Pull status values for evaluation jobs and model simulations. 
    try:
        basinStatus = statusMod.checkBasJobValid(statusData,basinNum,'BEST')
    except:
        raise
    try:
        evalStatus = statusMod.checkEvalJob(statusData,basinNum)
    except:
        raise
     
    # Create path to LOCK file if neeced
    lockPath = runDir + "/RUN.LOCK"
    evalLockPath = validWorkDir + '/EVAL.LOCK'
    
    # Path that will define when the parameter generation has completed.
    evalComplete = validWorkDir + "/R_VALID_COMPLETE"
    
    # Initialize the runFlag
    runFlag = False
    
    if keyStatus == 1.0:
        # Model has already completed
        runFlag = False
        return
        
    if keyStatus == 0.9:
        # Evaluation code is running. 
        if evalStatus:
            # Parameter generation code is still running.
            keySlot[basinNum,1] = 0.9
            keyStatus = 0.9
            runFlag = False
        else:
            # Check to make sure complete flag is present.
            if os.path.isfile(evalComplete):
                # Evaluation complete.
                # Log statistics into the DB.
                try:
                    db.logValidStats(statusData,int(statusData.jobID),int(gageID),str(gage))
                except:
                    raise
                keySlot[basinNum,1] = 1.0
                keyStatus = 1.0
                runFlag = False
            else:
                # Evaluation code failed. Lock up basin and 
                # send error message out.
                statusData.genMsg = "ERROR: Evaluation failed for gage: " + statusData.gages[basinNum] + \
                                    " Failed. Please remove LOCKFILE: " + evalLockPath
                open(evalLockPath,'a').close()
                errMod.sendMsg(statusData)
                keySlot[basinNum,1] = -0.9
                keyStatus = -0.9
                runFlag = False
                
    # For uncompleted simulations that are still listed as running.
    if keyStatus == 0.5:
        # If a model is running for this basin, continue and set keyStatus to 0.5
        if basinStatus:
            keySlot[basinNum,1] = 0.5
            keyStatus = 0.5
            runFlag = False
        else:
            # Either simulation has completed, or potentially crashed.
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                # Model crashed as simulation is not complete but no processes are running.
                #statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
                #                    " Failed. Attempting to restart."
                #print statusData.genMsg
                #errMod.sendMsg(statusData)
                keySlot[basinNum,1] = -0.25
                keyStatus = -0.25
                runFlag = True
            else:
                # Model has completed. Ready to run R evaluation code (pending control complete)
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                runFlag = False
                
    # For simulations that are fresh
    if keyStatus == 0.0:
        if basinStatus:
            # Model is still running from previous instance of workflow. Allow it to continue.
            keySlot[basinNum,1] = 0.5
            keyStatus = 0.5
            runFlag = False
        elif evalStatus:
            # Parameter generation code is running.
            keySlot[basinNum,1] = 0.9
            keyStatus = 0.9
            runFlag = False
        elif os.path.isfile(lockPath):
            keySlot[basinNum,1] = -1.0
            keyStatus = -1.0
            runFlag = False
        elif os.path.isfile(evalLockPath):
            keySlot[basinNum,1] = -0.9
            keyStatus = -0.1
            runFlag = False
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if not runFlag and os.path.isfile(evalComplete):
                # Model validation completed before workflow was restarted.
                try:
                    db.logValidStats(statusData,int(statusData.jobID),int(gageID),str(gage))
                except:
                    raise
                keySlot[basinNum,1] = 1.0
                keyStatus = 1.0
                runFlag = False
            if not runFlag and not os.path.isfile(evalComplete):
                # Model has completed, but the eval code hasn't ben ran yet.
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                runFlag = False
            if runFlag and not os.path.isfile(evalComplete):
                # model either hasn't ran yet, or needs to be restarted.
                keySlot[basinNum,1] = 0.0
                keyStatus = 0.0
                runFlag = True
                
    # For when evaluation jobs failed.
    if keyStatus == -0.9:
        # If LOCK file exists. File must
        # be removed manually by user.
        if os.path.isfile(evalLockPath):
            keySlot[basinNum,1] = -0.9
            keyStatus = -0.9
            runFlag = False
        else:
            # LOCK file was removed, upgrade status.
            keySlot[basinNum,1] = 0.75
            runFlag = False
                
    # For when the model failed TWICE and is locked.
    if keyStatus == -1.0:
        # If LOCK file exists, no simulation will take place. File must be removed
        # manually by user.
        if os.path.isfile(lockPath):
            runFlag = False
        else:
            # LOCK file was removed, upgrade status to 0.25 temporarily
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                keySlot[basinNum,1] = 0.0
                keyStatus = 0.0
            else:
                # Model sucessfully completed. Ready to run evaluation code.
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                runFlag = False
                
    # For when the model crashed ONCE
    if keyStatus == -0.5:
        if basinStatus:
            # Model is running again, upgrade status
            # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
            keySlot[basinNum,1] = 0.5
            keyStatus = 0.5
            runFlag = False
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
                open(lockPath,'a').close()
                keySlot[basinNum,1] = -1.0
                keyStatus = -1.0
                runFlag = False
            else:
                # Model sucessfully completed from first failed attempt. Ready to run evaluation code. 
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                
    if keyStatus == -0.25 and runFlag:
        # Restarting model from one crash
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        # Since these are validation simulations, we are always going to be 
        # starting the model rom an existing RESTART file. startType = 1 is for
        # when we have cold starts. 
        startType = 2
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,3)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,3)
        except:
            raise
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        # Fire off model.
        if statusData.jobRunType == 1:   
            cmd = "bsub < " + runDir + "/run_NWM.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
        if statusData.jobRunType == 4:
            cmd = runDir + "/run_NWM.sh"
            try:
                p = subprocess.Popen([cmd],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
                
        # Revert statuses to -0.5 for next loop to convey the model crashed once. 
        keyStatus = -0.5
        keySlot[basinNum,1] = -0.5
        
    if keyStatus == 0.0 and runFlag:
        # Model needs to be either ran, or restarted
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        # Since these are validation simulations, we are always going to be 
        # starting the model rom an existing RESTART file. startType = 1 is for
        # when we have cold starts. 
        startType = 2
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,3)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,3)
        except:
            raise
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        # Fire off model.
        if statusData.jobRunType == 1:
            cmd = "bsub < " + runDir + "/run_NWM.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
        if statusData.jobRunType == 4:
            cmd = runDir + "/run_NWM.sh"
            try:
                p = subprocess.Popen([cmd],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            except:
                statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
                raise
            
        keyStatus = 0.5
        keySlot[basinNum,1] = 0.5
        
    if keyStatus == 0.75 and not runFlag and ctrlStatus == 1.0:
        # Note the control simulation needs to be completed as well in 
        # order for the evaluation code to complete. 
        # We need to run parameter generation code.
        if statusData.jobRunType == 1:
            cmd = "bsub < " + validWorkDir + "/run_eval.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch evaluation job for gage: " + str(gageMeta.gage[basinNum])
                raise
        if statusData.jobRunType == 4:
            cmd = validWorkDir + "/E" + str(statusData.jobID) + str(gageID)
            print cmd
            try:
                p2 = subprocess.Popen([str(cmd)],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                time.sleep(20)
                print 'blah'
                p2.communicate
            except:
                statusData.errMsg = "ERROR: Unable to launch evaluation job for gage: " + str(gageMeta.gage[basinNum])
                raise
            
        keyStatus = 0.9
        keySlot[basinNum,1] = 0.9
                
def generateBsubRunScript(jobData,gageID,runDir,gageMeta,modName):
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
        
def generateMpiexecRunScript(jobData,gageID,runDir,gageMeta,modName):
    """
    Generic function to create a run script that will use mpiexec to run
    the model
    """
    
    outFile = runDir + "/run_NWM.sh"
    
    if os.path.isfile(outFile):
        os.remove(outFile)
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        inStr = 'for FILE in HYDRO_RST.*; do if [ ! -L $FILE ] ; then rm -rf $FILE; fi; done\n'
        fileObj.write(inStr)
        inStr = 'for FILE in RESTART.*; do if [ ! -L $FILE ] ; then rm -rf $FILE; fi; done\n'
        fileObj.write(inStr)
        if modName == "BEST":
            inStr = 'mpiexec -n ' + str(int(jobData.nCoresMod)) + ' ./WB' + \
            str(jobData.jobID) + str(gageID) + '\n'
        if modName == "CTRL":
            inStr = 'mpiexec -n ' + str(int(jobData.nCoresMod)) + ' ./WC' + \
            str(jobData.jobID) + str(gageID) + '\n'
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = 'Failure to create: ' + outFile
        raise
        
    # Make the file an executable.
    cmd = "chmod +x " + outFile
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + outFile + " to an executable."
        raise
        
def generateParmScript(jobData,bestDir,gage,parmInDir):
    """
    Generic function to generate the shell script to call Python to
    generate the new parameter datasets.
    """
    
    outFile = bestDir + "/gen_parms.sh"
    pyProgram = bestDir + "/generate_parameters.py"
    ctrlRunDir = jobData.jobDir + "/" + gage + "/RUN.VALID/OUTPUT/CTRL"
    defaultDir = jobData.jobDir + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS"
    
    if os.path.isfile(outFile):
        os.remove(outFile)
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('python ' + pyProgram + ' ' + bestDir + ' ' + parmInDir + ' ' + \
                      ctrlRunDir + ' ' + defaultDir + ' \n')
        fileObj.write('exit\n')
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
    # Make shell script an executable.
    cmd = 'chmod +x ' + outFile
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + outFile + " to an executable."
        raise
def generateMpiexecEvalRunScript(jobData,jobID,gageID,runDir,gageMeta,calibWorkDir,validWorkDir):
    """
    Generic function to create evaluation mpiexec script in the best simulation directory.
    This function also generates the shell script to call R.
    """
    # First establish paths to files being created.
    rScript = validWorkDir + "/validScript.R"
    fileOut = validWorkDir + "/run_eval.sh"
    
    if os.path.isfile(rScript):
        os.remove(rScript)
    if os.path.isfile(fileOut):
        os.remove(fileOut)
        
    # Create mpiexec shell script for the evaluation job.
    try:
        fileObj = open(fileOut,'w')
        fileObj.write('#!/bin/bash\n')
        inStr = 'cd ' + validWorkDir + '\n'
        fileObj.write(inStr)
        inStr = "Rscript " + validWorkDir + "/valid_workflow.R " + rScript + "\n"
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + fileOut
        raise
        
    # Make shell script an executable.
    cmd = 'chmod +x ' + fileOut
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + fileOut + " to an executable."
        raise
        
    # Create validScript.R
    try:
        fileObj = open(rScript,'w')
        fileObj.write("#### Model Parameters ####\n")
        fileObj.write("# Specify run directory containing validation simulations.\n")
        inStr = "runDir <- '" + calibWorkDir + "'\n"
        fileObj.write(inStr)
        inStr = "validDir <- '" + validWorkDir + "'\n"
        fileObj.write(inStr)
        fileObj.write("# Objective function#\n")
        inStr = "objFn <- '" + str(jobData.objFunc) + "'\n"
        fileObj.write(inStr)
        fileObj.write("# Basin-specific metadata\n")
        inStr = "siteId <- '" + str(gageMeta.gage) + "'\n"
        fileObj.write(inStr)
        inStr = "linkId <- " + str(gageMeta.comID) + "\n"
        fileObj.write(inStr)
        fileObj.write('# Start and dates for evaluation period (e.g., after spinup period)\n')
        inStr = "startCalibDate <- as.POSIXct(\"" + jobData.bCalibEvalDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        inStr = "endCalibDate <- as.POSIXct(\"" + jobData.eCalibDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        inStr = "startValidDate <- as.POSIXct(\"" + jobData.bValidEvalDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        inStr = "endValidDate <- as.POSIXct(\"" + jobData.eValidDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        fileObj.write('# Specify number of cores to use\n')
        inStr = "ncores <- " + str(jobData.nCoresR) + "\n"
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + rScript
        raise
        
    # Create symbolic link with basin/job info in link name for monitoring
    fileLink = validWorkDir + "/E" + str(jobID) + str(gageID)
    if not os.path.islink(fileLink):
        try:
            os.symlink(fileOut,fileLink)
        except:
            jobData.errMsg = "ERROR: Failure to create symbolic link: " + fileLink
            raise
        
def generateBsubEvalRunScript(jobData,jobID,gageID,runDir,gageMeta,calibWorkDir,validWorkDir):
    """
    Generic function to create evaluation BSUB script in the best simulation
    directory. This function also generates the shell script to call R.
    """
    # First establish paths to files being created.
    rScript = validWorkDir + "/validScript.R"
    bsubOut = validWorkDir + "/run_eval.sh"
    
    if os.path.isfile(rScript):
        os.remove(rScript)
    if os.path.isfile(bsubOut):
        os.remove(bsubOut)
        
    # Create BSUB shell script for the evaluation job.
    try:
        fileObj = open(bsubOut,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -x\n')
        inStr = "#BSUB -n 1\n"
        fileObj.write(inStr)
        inStr = "#BSUB -J NWM_EVAL_" + str(jobID) + "_" + str(gageID) + '\n'
        fileObj.write(inStr)
        inStr = '#BSUB -o ' + validWorkDir + '/%J.out\n'
        fileObj.write(inStr)
        inStr = '#BSUB -e ' + validWorkDir + '/%J.err\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -W 1:00\n')
        fileObj.write('#BSUB -q premium\n')
        fileObj.write('\n')
        inStr = 'cd ' + validWorkDir + '\n'
        fileObj.write(inStr)
        inStr = "Rscript " + validWorkDir + "/valid_workflow.R " + rScript + "\n"
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + bsubOut
        raise
        
    # Create validScript.R
    try:
        fileObj = open(rScript,'w')
        fileObj.write("#### Model Parameters ####\n")
        fileObj.write("# Specify run directory containing validation simulations.\n")
        inStr = "runDir <- '" + calibWorkDir + "'\n"
        fileObj.write(inStr)
        inStr = "validDir <- '" + validWorkDir + "'\n"
        fileObj.write(inStr)
        fileObj.write("# Objective function#\n")
        inStr = "objFn <- '" + str(jobData.objFunc) + "'\n"
        fileObj.write(inStr)
        fileObj.write("# Basin-specific metadata\n")
        inStr = "siteId <- '" + str(gageMeta.gage) + "'\n"
        fileObj.write(inStr)
        inStr = "linkId <- " + str(gageMeta.comID) + "\n"
        fileObj.write(inStr)
        fileObj.write('# Start and dates for evaluation period (e.g., after spinup period)\n')
        inStr = "startCalibDate <- as.POSIXct(\"" + jobData.bCalibEvalDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        inStr = "endCalibDate <- as.POSIXct(\"" + jobData.eCalibDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        inStr = "startValidDate <- as.POSIXct(\"" + jobData.bValidEvalDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        inStr = "endValidDate <- as.POSIXct(\"" + jobData.eValidDate.strftime('%Y-%m-%d') + "\", " + \
                 "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        fileObj.write('# Specify number of cores to use\n')
        inStr = "ncores <- " + str(jobData.nCoresR) + "\n"
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + rScript
        raise
def generateBsubParmRunScript(jobData,runDir,gageID):
    """
    Generic function to run BSUB command to run the parameter generation script.
    """
    
    outFile = runDir + "/run_parms.sh"
    
    if os.path.isfile(outFile):
        os.remove(outFile)
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -x\n')
        inStr = "#BSUB -n 1\n"
        fileObj.write(inStr)
        inStr = "#BSUB -J NWM_PARM_GEN_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
        fileObj.write(inStr)
        inStr = '#BSUB -o ' + runDir + '/%J.out\n'
        fileObj.write(inStr)
        inStr = '#BSUB -e ' + runDir + '/%J.err\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -W 0:20\n')
        fileObj.write('#BSUB -q premium\n')
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        fileObj.write('./gen_parms.sh\n')
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generateMpiexecParmRunScript(jobData,runDir,gageID):
    """
    Generic function to run mpiexec to run the parameter generation script.
    """
    
    outFile = runDir + "/run_params.sh"
    
    if os.path.isfile(outFile):
        os.remove(outFile)
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        fileObj.write('./gen_parms.sh\n')
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
    # Make shell script an executable.
    cmd = 'chmod +x ' + outFile
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + outFile + " to an executable."
        raise
        
    # Create symbolic link with basin/job info in link name for monitoring
    fileLink = runDir + "/P" + str(jobData.jobID) + str(gageID)
    if not os.path.islink(fileLink):
        try:
            os.symlink(outFile,fileLink)
        except:
            jobData.errMsg = "ERROR: Failure to create symbolic link: " + fileLink
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
