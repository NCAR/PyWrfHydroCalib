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

def runModelCtrl(statusData,staticData,db,gageID,gage,keySlot,basinNum,libPathTop):
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
    
    print "CTRL STATUS XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    print "CTRL STATUS = " + str(ctrlStatus)
    print "BEST STATUS = " + str(bestStatus)
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
    workDir = statusData.jobDir + "/" + gage + "/RUN.VALID"
    
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
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
        raise Exception()
        
    # Determine which iteration was the best. If no best iteration was found,
    # this means something happened during the calibrations and all values were reset
    # to allow remaining basins to finish. If the out status from the DB command
    # below is -99, then simply set all status values to 1.0
    try:
        iterStatus = db.genValidParmTbl(statusData,statusData.jobID,gageID,gage)
        print "ITER STATUS = " + str(iterStatus)
    except:
        raise
    if iterStatus == -99:
        print "BASIN: " + gage + " WAS BLACKED OUT HERE"
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
    utilProgram = libPathTop + "/calib_workflow.R"
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
        link = workDir + "/calib_workflow.R"
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
    bsub1Script = runDir + "/bsub_parms.sh"
    bsub2Script = runDir + "/run_NWM.sh"
    
    # If the files exist, remove them and re-create.
    if os.path.isfile(parmRunScript):
        os.remove(parmRunScript)
    if os.path.isfile(bsub1Script):
        os.remove(bsub1Script)
    if os.path.isfile(bsub2Script):
        os.remove(bsub2Script)
    
    # Generate the shell script to call Python to generate parameter datasets
    # in preparation for model runs.     
    try:
        generateParmScript(statusData,bestDir,gage,parmInDir)
    except:
        raise
    # Generate the BSUB script to run the parameter generation code. 
    try:
        generateParmRunScript(statusData,bestDir,gageID)
    except:
        raise
    # Generate the BSUB run script to run the model simulations. 
    try:
        generateRunScript(statusData,gageID,runDir,gageMeta,'CTRL')
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
     
    print "BASIN STATUS = " + str(basinStatus)
    print "PARAM GEN STATUS = " + str(genParmStatus)
    # Create path to LOCK file if neeced
    lockPath = runDir + "/RUN.LOCK"
    parmLockPath = runDir + '/PARM_GEN.LOCK'    
    
    print keyStatus
    # Path that will define when the parameter generation has completed.
    genParmComplete = bestDir + "/PARAM_GEN.COMPLETE"
    
    if keyStatus == 1.0:
        # Model has already completed
        runFlag = False
        return
        
    if keyStatus == 0.1:
        # Parameter generation code is running. 
        print "PARAM GEN CODE RUNNING"
        if genParmStatus:
            # Parameter generation code is still running.
            keySlot[basinNum,0] = 0.1
            keyStatus = 0.1
            runFlag = False
        else:
            # Check to make sure complete flag is present.
            if os.path.isfile(genParmComplete):
                print "PARAM GEN CODE COMPLETE"
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
            print "MODEL RUNNING"
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
                print "MODEL CRASHED ONCE"
                # Model crashed as simulation is not complete but no processes are running.
                #statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
                #                    " Failed. Attempting to restart."
                #print statusData.genMsg
                #errMod.sendMsg(statusData)
                keySlot[basinNum,0] = -0.25
                keyStatus = -0.25
                runFlag = True
            else:
                print "MODEL COMPLETED"
                # Model has completed!
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                runFlag = False
           
    # For simulations that are fresh
    if keyStatus == 0.0:
        if basinStatus:
            print "MODEL RUNNING"
            # Model is still running from previous instance of workflow. Allow it to continue.
            keySlot[basinNum,0] = 0.5
            keyStatus = 0.5
            runFlag = False
        elif genParmStatus:
            print "PARAM GEN CODE RUNNING"
            # Parameter generation code is running.
            keySlot[basinNum,0] = 0.1
            keyStatus = 0.1
            runFlag = False
        elif os.path.isfile(lockPath):
            print "MODEL IS LOCKED"
            keySlot[basinNum,0] = -1.0
            keyStatus = -1.0
            runFlag = False
        elif os.path.isfile(parmLockPath):
            print "PARAMETER GEN LOCKED"
            keySlot[basinNum,0] = -0.1
            keyStatus = -0.1
            runFlag = False
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if not runFlag and os.path.isfile(genParmComplete):
                print "MODEL COMPLETED"
                # Model simulation completed before workflow was restarted.
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                runFlag = False
            if runFlag and not os.path.isfile(genParmComplete):
                print "READY TO RUN PARAM GEN CODE"
                # Model hasn't ran, and parameter generation code hasn't ran yet.
                keySlot[basinNum,0] = 0.0
                keyStatus = 0.0
                runFlag = False
            if runFlag and os.path.isfile(genParmComplete):
                print "PARAM GEN CODE COMPLETED. READY TO RUN MODEL"
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
            print "MODEL STILL LOCKED"
            runFlag = False
        else:
            # LOCK file was removed, upgrade status to 0.25 temporarily
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                print "UPGRADING MODEL STATUS TO UNLOCKED"
                keySlot[basinNum,0] = 0.25
                keyStatus = 0.25
            else:
                print "MODEL COMPLETED"
                # Model sucessfully completed.
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                runFlag = False
                
    # For when the model crashed ONCE
    if keyStatus == -0.5:
        if basinStatus:
            print "MODEL RUNNING AGAIN, UPGRADING STATUS"
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
                print "MODEL COMPLETED"
                # Model sucessfully completed from first failed attempt.
                keySlot[basinNum,0] = 1.0
                keyStatus = 1.0
                
    if keyStatus == -0.25 and runFlag:
        print "RESTARTING MODEL"
        # Restarting model from one crash
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        if begDate == staticData.bValidDate:
            startType = 1
        else:
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
                
        # Fire off model.
        cmd = "bsub < " + runDir + "/run_NWM.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        # Revert statuses to -0.5 for next loop to convey the model crashed once. 
        keyStatus = -0.5
        keySlot[basinNum,0] = -0.5
        
    if keyStatus == 0.25 and runFlag:
        print "FIRING OFF MODEL"
        # Model needs to be either ran, or restarted
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        if begDate == staticData.bValidDate:
            startType = 1
        else:
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
                
        # Fire off model.
        cmd = "bsub < " + runDir + "/run_NWM.sh"
        print cmd
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.5
        keySlot[basinNum,0] = 0.5
        
    if keyStatus == 0.0 and not runFlag:
        print "FIRING OFF PARAM GEN CODE"
        # We need to run parameter generation code.
        cmd = "bsub < " + bestDir + "/bsub_parms.sh"
        print cmd
        try:
            subprocess.call(cmd,shell=True)
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
    
    print "BEST STATUS XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    # If the control status is not at least 0.25, this means the code to generate
    # parameters is still running, hasn't begun yet, or there's an issue with
    # the model. Simply return to the main workflow calling program.
    if ctrlStatus < 0.25:
        print "CONTROL RUNS NOT FAR ENOUGH"
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
    
    print runDir
    
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
        generateEvalRunScript(staticData,statusData.jobID,gageID,runDir,gageMeta,calibWorkDir,validWorkDir)
    except:
        raise
    # Generate the BSUB run script to run the model simulations. 
    try:
        generateRunScript(statusData,gageID,runDir,gageMeta,'BEST')
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
     
    print "BEST BASIN STATUS = " + str(basinStatus)
    print "BEST EVAL STATUS = " + str(evalStatus)
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
        print "EVAL CODE RUNNING"
        if evalStatus:
            # Parameter generation code is still running.
            keySlot[basinNum,1] = 0.9
            keyStatus = 0.9
            runFlag = False
        else:
            # Check to make sure complete flag is present.
            if os.path.isfile(evalComplete):
                print "EVAL CODE COMPLETE"
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
            print "MODEL RUNNING"
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
                print "MODEL CRASHED ONCE"
                # Model crashed as simulation is not complete but no processes are running.
                #statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
                #                    " Failed. Attempting to restart."
                #print statusData.genMsg
                #errMod.sendMsg(statusData)
                keySlot[basinNum,1] = -0.25
                keyStatus = -0.25
                runFlag = True
            else:
                print "MODEL COMPLETED"
                # Model has completed. Ready to run R evaluation code (pending control complete)
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                runFlag = False
                
    # For simulations that are fresh
    if keyStatus == 0.0:
        if basinStatus:
            print "MODEL RUNNING"
            # Model is still running from previous instance of workflow. Allow it to continue.
            keySlot[basinNum,1] = 0.5
            keyStatus = 0.5
            runFlag = False
        elif evalStatus:
            print "EVAL CODE RUNNING"
            # Parameter generation code is running.
            keySlot[basinNum,1] = 0.9
            keyStatus = 0.9
            runFlag = False
        elif os.path.isfile(lockPath):
            print "MODEL IS LOCKED"
            keySlot[basinNum,1] = -1.0
            keyStatus = -1.0
            runFlag = False
        elif os.path.isfile(evalLockPath):
            print "EVAL LOCKED"
            keySlot[basinNum,1] = -0.1
            keyStatus = -0.1
            runFlag = False
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if not runFlag and os.path.isfile(evalComplete):
                print "VALIDATION COMPLETED"
                # Model validation completed before workflow was restarted.
                try:
                    db.logValidStats(statusData,int(statusData.jobID),int(gageID),str(gage))
                except:
                    raise
                keySlot[basinNum,1] = 1.0
                keyStatus = 1.0
                runFlag = False
            if not runFlag and not os.path.isfile(evalComplete):
                print "READY TO RUN EVAL CODE"
                # Model has completed, but the eval code hasn't ben ran yet.
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                runFlag = False
            if runFlag and not os.path.isfile(evalComplete):
                print "READY TO RUN MODEL"
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
            print "MODEL STILL LOCKED"
            runFlag = False
        else:
            # LOCK file was removed, upgrade status to 0.25 temporarily
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                print "UPGRADING MODEL STATUS TO UNLOCKED"
                keySlot[basinNum,1] = 0.0
                keyStatus = 0.0
            else:
                print "MODEL COMPLETED"
                # Model sucessfully completed. Ready to run evaluation code.
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                runFlag = False
                
    # For when the model crashed ONCE
    if keyStatus == -0.5:
        if basinStatus:
            print "MODEL RUNNING AGAIN, UPGRADING STATUS"
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
                print statusData.genMsg
                open(lockPath,'a').close()
                keySlot[basinNum,1] = -1.0
                keyStatus = -1.0
                runFlag = False
            else:
                print "MODEL COMPLETED"
                # Model sucessfully completed from first failed attempt. Ready to run evaluation code. 
                keySlot[basinNum,1] = 0.75
                keyStatus = 0.75
                
    if keyStatus == -0.25 and runFlag:
        print "RESTARTING MODEL"
        # Restarting model from one crash
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        if begDate == staticData.bValidDate:
            startType = 1
        else:
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
        cmd = "bsub < " + runDir + "/run_NWM.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        # Revert statuses to -0.5 for next loop to convey the model crashed once. 
        keyStatus = -0.5
        keySlot[basinNum,1] = -0.5
        
    if keyStatus == 0.0 and runFlag:
        print "FIRING OFF MODEL"
        # Model needs to be either ran, or restarted
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        if begDate == staticData.bValidDate:
            startType = 1
        else:
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
        cmd = "bsub < " + runDir + "/run_NWM.sh"
        print cmd
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch NWM job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.5
        keySlot[basinNum,1] = 0.5
        
    if keyStatus == 0.75 and not runFlag and ctrlStatus != 1.0:
        print "CTRL RUN NOT FINISHED YET."
        
    if keyStatus == 0.75 and not runFlag and ctrlStatus == 1.0:
        # Note the control simulation needs to be completed as well in 
        # order for the evaluation code to complete. 
        print "FIRING OFF EVAL CODE"
        # We need to run parameter generation code.
        cmd = "bsub < " + validWorkDir + "/run_eval.sh"
        print cmd
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch evaluation job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.9
        keySlot[basinNum,1] = 0.9
                
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
        
def generateEvalRunScript(jobData,jobID,gageID,runDir,gageMeta,calibWorkDir,validWorkDir):
    """
    Generic function to create evaluation BSUB script in the best simulation
    directory. This function also generates the shell script to call R.
    """
    # First establish paths to files being created.
    rScript = validWorkDir + "/validScript.R"
    bsubOut = runDir + "/run_eval.sh"
    
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
        inStr = '#BSUB -o ' + runDir + '/%J.out\n'
        fileObj.write(inStr)
        inStr = '#BSUB -e ' + runDir + '/%J.err\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -W 1:00\n')
        fileObj.write('#BSUB -q premium\n')
        fileObj.write('\n')
        inStr = 'cd ' + validWorkDir + '\n'
        fileObj.write(inStr)
        inStr = "Rscript " + validWorkDir + "/valid_workflow.R\n"
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
def generateParmRunScript(jobData,runDir,gageID):
    """
    Generic function to run BSUB command to run the parameter generation script.
    """
    
    outFile = runDir + "/bsub_parms.sh"
    
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
