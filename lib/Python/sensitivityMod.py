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
import socket
import time

import warnings
warnings.filterwarnings("ignore")

def preProc(preProcStatus,statusData,staticData,db,gageID,gage):
    """
    Function to run R/Python pre-processing for all the potential permutations
    of the sensivity analysis. The first step is to run R code to generate a 
    table of parameter values. Followed up with that, Python code will be 
    ran to create parameter NetCDF files that will be used by the model 
    simulations. 
    """
    # Establish some constant paths
    workDir = statusData.jobDir + "/" + gage + "/RUN.SENSITIVITY"
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
        
    lockFile = workDir + "/PRE_PROC.LOCK"
    errFile = workDir + "/PRE_PROC.ERR"
    completeFlag = workDir + "/preProc.COMPLETE"
        
    # Pull gage metadata for this particular basin.
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
    
    # First check for the namelist file:
    # 1.) No namelist file is present. This means the code needs to be ran.
    # 2.) The namelist file is present, but not COMPLETE flag is present. 
    #     Also, no job is running. This means the code failed at some point.
    #     Lock the basin up and report an error message. Also create an empty
    #     ERROR file.
    # 3.) The namelist file is present and the COMPLETE flag is present. 
    #     Set the preproc status to True and return to the main calling program.
    # 4.) A LOCK file is in place. The user will need to remove. 
    # 5.) The namelist file is present, the LOCK file is not present, and the 
    #     ERROR file is present. This means the user removed the LOCK file.
    #     resubmit the job. 
    # First step is to create a R namelist file the pre-processing R code will
    # run
    if os.path.isfile(lockFile) and os.path.isfile(errFile):
        # Pre-processing is locked. Return to the main calling program
        #statusData.genMsg = "Pre-Processing for basin: " + gage + " has been locked. " + \
        #                    " Please remove: " + lockFile
        #errMod.sendMsg(statusData)
        preProcStatus = False
        return
    
    rNameList = workDir + "/namelist.sensitivity"
    if not os.path.isfile(rNameList):
        try:
            genRNameList(staticData,workDir,gageMeta,gage)
        except:
            statusData.errMsg = "ERROR: Unable to generate sensitivity pre-processing R namelist"
            raise
    else:
        # Check to see if the COMPLETE flag is present.
        if os.path.isfile(completeFlag):
            # Code successfully completed
            preProcStatus = True
            return
        if not os.path.isfile(lockFile) and os.path.isfile(errFile):
            # User has removed the LOCK file. We will attempt to run the pre-processing code.
            print "LOCK FILE REMOVED, RESTARTING JOB"
            try:
                os.remove(errFile)
            except:
                statusData.errMsg = "ERROR: Unable to remove: " + errFile
                errMod.errOut(statusData)
                preProcStatus = False
        else:
            # Check to see if a job is running.
            preRunStatus = statusMod.checkSensPreProcJob(statusData,gageID)
            if not preRunStatus:
                # This implies the job failed. Report an error to the user and create a LOCK file.
                try:
                    open(lockFile,'a').close()
                except:
                    statusData.errMsg = "ERROR: Unable to create: " + lockFile
                    raise
                try:
                    open(errFile,'a').close()
                except:
                    statusData.errMsg = "ERROR: Unable to create: " + errFile
                    raise
                statusData.genMsg = "ERROR: Basin - " + str(gageID) + " sensitivity pre-processing failed."
                errMod.sendMsg(statusData)
                statusData.genMsg = "       Please remove: " + lockFile + " before this basin can continue."
                errMod.sendMsg(statusData)
                preProcStatus = False
                return
            else:
                # The job is still running.
                print "SENSITIVITY PRE PROC SENS RUNNING FOR BASIN: " + str(gageID)
                time.sleep(1)
                preProcStatus = False
                return
                
    # Generate run script to generate parameters for this basin. Then execute the job.
    if statusData.analysisRunType == 1:
        #BSUB
        generateBsubPreProcScript(statusData,gageID,workDir,workDir,gageMeta)
        cmd = "bsub < " + workDir + "/run_WH_SENS_PREPROC.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch sensitivity pre-processing job for gage: " + str(gage)
            raise
    if statusData.analysisRunType == 2:
        #PBS
        generatePbsPreProcScript(statusData,gageID,workDir,workDir,gageMeta)
        cmd = "qsub " + workDir + "/run_WH_SENS_PREPROC.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch sensitivity pre-processing job for gage: " + str(gage)
            raise
    if statusData.analysisRunType == 3:
        #SLURM
        generateSlurmPreProcScript(statusData,gageID,workDir,workDir,gageMeta)
        cmd = "sbatch " + workDir + "/run_WH_SENS_PREPROC.sh"
        try:
            subprocess.call(cmd,shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch sensitivity pre-processing job for gage: " + str(gage)
            raise
    if statusData.analysisRunType == 4 or statusData.analysisRunType == 5:
        #MPIEXEC/MPIRUN
        generateMpiPreProcScript(statusData,gageID,workDir,workDir,gageMeta)
        cmd = workDir + "/run_WH_SENS_PREPROC.sh 1>" + workDir + "/WH_SENS_PREPROC_" + \
              str(statusData.jobID) + "_" + str(gageID) + ".out" + \
              ' 2>' + workDir + "/WH_SENS_PREPROC_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        try:
            p3 = subprocess.Popen([cmd],shell=True)
            time.sleep(1)
        except:
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro Calib job for gage: " + str(gage)
            raise
    
def postProc(postProcStatus,statusData,staticData,db,gageID,gage):
    """
    Function to run R post-processing for all the potential permutations
    of the sensivity analysis. The first step is to run R code to analyze/read
    model streamflow. From there, we look for a COMPLETE file and table of 
    statistics, which are placed into the DB. 
    """
    # Establish some constant paths
    workDir = statusData.jobDir + "/" + gage + "/RUN.SENSITIVITY"
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
        
    lockFile = workDir + "/POST_PROC.LOCK"
    errFile = workDir + "/POST_PROC.ERR"
    runFlag = workDir + "/POST_PROC.RUN"
    completeFlag = workDir + "/postProc.COMPLETE"
    missingFlag = workDir + "/CALC_STATS_MISSING"
    statsFile = workDir + "/stat_sensitivity.txt"
    runStatus = True
        
    # Pull gage metadata for this particular basin.
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
    
    if os.path.isfile(lockFile) and os.path.isfile(errFile):
        # Remove the run flag if it's present.
        if os.path.isfile(runFlag):
            try:
                os.remove(runFlag)
            except:
                statusData.errMsg = "ERROR: Unable to remove: " + runFlag
                raise
        # Post-processing is locked. Return to the main calling program
        #statusData.genMsg = "Post-Processing for basin: " + gage + " has been locked. " + \
        #                    " Please remove: " + lockFile
        #errMod.sendMsg(statusData)
        postProcStatus = False
        runStatus = False
        return
    
    if not os.path.isfile(errFile) and os.path.isfile(lockFile):
        # This would be an unusual situation where the ERROR file was removed
        # but not the LOCK File. Report an error to the user. 
        # Remove the run flag if it's present.
        if os.path.isfile(runFlag):
            try:
                os.remove(runFlag)
            except:
                statusData.errMsg = "ERROR: Unable to remove: " + runFlag
                raise
        #statusData.genMsg = "Post-Processing for basin: " + gage + " has been locked. " + \
        #                    " Please remove: " + lockFile
        #errMod.sendMsg(statusData)
        postProcStatus = False
        runStatus = False
        return
    
    rNameList = workDir + "/namelist.sensitivity"
    if not os.path.isfile(rNameList):
        try:
            genRNameList(staticData,workDir,gageMeta,gage)
        except:
            statusData.errMsg = "ERROR: Unable to generate sensitivity pre-processing R namelist"
            raise
        
    # Check to see if the COMPLETE flag is present.
    if os.path.isfile(completeFlag):
        # Code successfully completed
        # Ensure the stats file is present.
        if not os.path.isfile(statsFile) and not os.path.isfile(missingFlag):
            statusData.errMsg = "ERROR: Expected to find: " + statsFile + " but was not found."
            raise
        # Remove the run flag if it's present.
        if os.path.isfile(runFlag):
            try:
                os.remove(runFlag)
            except:
                statusData.errMsg = "ERROR: Unable to remove: " + runFlag
                raise
        postProcStatus = True
        runStatus = False
        return
    if not os.path.isfile(lockFile) and os.path.isfile(errFile):
        print "ERRORED BUT LOCK REMOVED"
        # User has removed the LOCK file. We will attempt to run the post-processing code.
        # Remove the run flag if it's present.
        if os.path.isfile(runFlag):
            try:
                os.remove(runFlag)
            except:
                statusData.errMsg = "ERROR: Unable to remove: " + runFlag
                raise
        try:
            os.remove(errFile)
        except:
            statusData.errMsg = "ERROR: Unable to remove: " + errFile
            errMod.errOut(statusData)
            postProcStatus = False
        # Remove the run flag if it's present.
        if os.path.isfile(runFlag):
            try:
                os.remove(runFlag)
            except:
                statusData.errMsg = "ERROR: Unable to remove: " + runFlag
                raise
        runStatus = True
    if os.path.isfile(runFlag):
        # Check to see if a job is running.
        postRunStatus = statusMod.checkSensPostProcJob(statusData,gageID)
        if not postRunStatus:
            if os.path.isfile(missingFlag):
                # This is a unique siutation where either an improper COMID (linkID) was passed to
                # the R program, pulling NA from the model. Or, the observations
                # file contians too many missing values. For this, convey to the
                # user through a message, set the status fro all iterations to 1.
                statusData.genMsg = "WARNING: Either a bad COMID exists for gage: " + \
                                    str(gage) + " or there are no observations " + \
                                    " for the evaluation period."
                errMod.sendMsg(statusData)
                # Set the status to 1.0, remove the run flag, upgrade the post-processing
                # status, and touch a complete flag. 
                try:
                    os.remove(runFlag)
                except:
                    statusData.errMsg = "ERROR: Unable to remove: " + runFlag
                    raise
                try:
                    open(completeFlag,'a').close()
                except:
                    statusData.errMsg = "ERROR: Unable to create: " + completeFlag
                    raise
                postProcStatus = True
                runStatus = False
                return
            else:
                # This implies the job failed. Report an error to the user and create a LOCK file.
                try:
                    open(lockFile,'a').close()
                except:
                    statusData.errMsg = "ERROR: Unable to create: " + lockFile
                    raise
                try:
                    open(errFile,'a').close()
                except:
                    statusData.errMsg = "ERROR: Unable to create: " + errFile
                    raise
                # Remove the run flag if it's present.
                if os.path.isfile(runFlag):
                    try:
                        os.remove(runFlag)
                    except:
                        statusData.errMsg = "ERROR: Unable to remove: " + runFlag
                        raise
                statusData.genMsg = "ERROR: Basin - " + str(gageID) + " sensitivity post-processing failed."
                errMod.sendMsg(statusData)
                statusData.genMsg = "       Please remove: " + lockFile + " before this basin can continue."
                errMod.sendMsg(statusData)
                postProcStatus = False
                runStatus = False
                return
        else:
            # The job is still running.
            print "SENS POST PROC SENS RUNNING FOR BASIN: " + str(gageID)
            postProcStatus = False
            runStatus = False
            return
                
    if runStatus == True:
        # Generate run script to generate parameters for this basin. Then execute the job.
        if statusData.analysisRunType == 1:
            #BSUB
            generateBsubPostProcScript(statusData,gageID,workDir,workDir,gageMeta)
            cmd = "bsub < " + workDir + "/run_WH_SENS_POSTPROC.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch sensitivity post-processing job for gage: " + str(gage)
                raise
        if statusData.analysisRunType == 2:
            #PBS
            generatePbsPostProcScript(statusData,gageID,workDir,workDir,gageMeta)
            cmd = "qsub " + workDir + "/run_WH_SENS_POSTPROC.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch sensitivity post-processing job for gage: " + str(gage)
                raise
        if statusData.analysisRunType == 3:
            #SLURM
            generateSlurmPostProcScript(statusData,gageID,workDir,workDir,gageMeta)
            cmd = "sbatch " + workDir + "/run_WH_SENS_POSTPROC.sh"
            try:
                subprocess.call(cmd,shell=True)
            except:
                statusData.errMsg = "ERROR: Unable to launch sensitivity post-processing job for gage: " + str(gage)
                raise
        if statusData.analysisRunType == 4 or statusData.analysisRunType == 5:
            #MPIEXEC/MPIRUN
            generateMpiPostProcScript(statusData,gageID,workDir,workDir,gageMeta)
            cmd = workDir + "/run_WH_SENS_POSTPROC.sh 1>" + workDir + "/WH_SENS_POSTPROC_" + \
                  str(statusData.jobID) + "_" + str(gageID) + ".out" + \
                  ' 2>' + workDir + "/WH_SENS_POSTPROC_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
            try:
                p3 = subprocess.Popen([cmd],shell=True)
                time.sleep(1)
            except:
                statusData.errMsg = "ERROR: Unable to launch WRF-Hydro Calib job for gage: " + str(gage)
                raise
        try:
            open(runFlag,'a').close()
        except:
            statusData.errMsg = "ERROR: Unable to create: " + lockFile
            raise
            
    time.sleep(1)
            
def runModel(statusData,staticData,db,gageID,gage,keySlot,basinNum,iteration):
    """
    Function for running the sensitivity analysis for a given basin. 
    This function will loop through all model iterations specified for
    sensitivity. This code will only iterate over blocks of model runs at 
    a time, as specified in the setup.parm file. This is to prevent too many
    model simulations being ran all at once. All model iterations must be 
    complete before the sensitivity analysis can be ran. Additionally, 
    an initial program to generate the necessary input parameter files
    will be ran to generate parmaeter files for all model iterations. If
    any model iterations fail, the analysis workflow will lock up and report
    an error back to the user. 
    """
    # Pull gage metadata for this particular basin.
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
        
    runDir = statusData.jobDir + "/" + gage + "/RUN.SENSITIVITY/OUTPUT_" + str(iteration)
    workDir = statusData.jobDir + "/" + gage + "/RUN.SENSITIVITY"
    collectComplete = runDir + "/R_COLLECT.COMPLETE"
    
    rNameList = workDir + "/namelist.sensitivity"
    if not os.path.isfile(rNameList):
        try:
            genRNameList(staticData,workDir,gageMeta,gage)
        except:
            statusData.errMsg = "ERROR: Unable to generate sensitivity pre-processing R namelist"
            raise
    
    if statusData.jobRunType == 1:
        # If BSUB run script doesn't exist, create it here.
        bsubFile = runDir + "/run_WH.sh"
        if not os.path.isfile(bsubFile):
            try:
                generateBsubScript(statusData,int(gageID),runDir,gageMeta,iteration)
            except:
                raise
    if statusData.jobRunType == 2:
        pbsFile = runDir + "/run_WH.sh"
        if not os.path.isfile(pbsFile):
            try:
                generatePbsScript(statusData,int(gageID),runDir,gageMeta,iteration)
            except:
                raise
    if statusData.jobRunType == 3:
        slurmFile = runDir + "/run_WH.sh"
        if not os.path.isfile(slurmFile):
            try:
                generateSlurmScript(statusData,int(gageID),runDir,gageMeta,iteration)
            except:
                raise
    if statusData.jobRunType == 4 or statusData.jobRunType == 5:
        # If run script doesn't exist, create it here.
        runScript = runDir + "/run_WH.sh"
        if not os.path.isfile(runScript):
            try:
                generateMpiScript(statusData,int(gageID),runDir,gageMeta,iteration)
            except:
                raise
                
    if statusData.analysisRunType == 1:
        collectScript = runDir + "/run_collection.sh"
        try:
            generateBsubCollectScript(statusData,int(gageID),runDir,gageMeta,iteration,workDir)
        except:
            raise
    if statusData.analysisRunType == 2:
        collectScript = runDir + "/run_collection.sh"
        try:
            generatePbsCollectScript(statusData,int(gageID),runDir,gageMeta,iteration,workDir)
        except:
            raise
    if statusData.analysisRunType == 3:
        collectScript = runDir + "/run_collection.sh"
        try:
            generateSlurmCollectScript(statusData,int(gageID),runDir,gageMeta,iteration,workDir)
        except:
            raise
    if statusData.analysisRunType == 4 or statusData.analysisRunType == 5:
        collectScript = runDir + "/SCOL" + str(statusData.jobID) + str(gageID) + str(iteration)
        try:
            generateMpiCollectScript(statusData,int(gageID),runDir,gageMeta,iteration,workDir)
        except:
            raise
            
    # Calculate datetime objects
    begDate = statusData.bSensDate
    endDate = statusData.eSensDate
    
    # Initialize status
    keyStatus = keySlot[basinNum,iteration]
    
    try:
        basinStatus = statusMod.checkBasSensJob(statusData,basinNum,iteration,runDir)
    except:
        raise
    try:
        collectStatus = statusMod.checkSensCollectJob(statusData,gageID,iteration)
    except:
        raise
        
    # Create path to LOCK file if neeced
    lockPath = runDir + "/RUN.LOCK"
    collectLock = runDir + "/COLLECT.LOCK"
    
    # If the LOCK file is present, report this and lock things up.
    if os.path.isfile(lockPath):
        keySlot[basinNum,iteration] = -1.0
        keyStatus = -1.0
        runFlag = False
        #statusData.genMsg = "ERROR: Basin ID: " + str(gageID) + " sensitivity is locked. " + \
        #                    " for iteration: " + str(iteration) + " " + \
        #                    "Please remove: " + lockPath + " before continuing."
        #errMod.sendMsg(statusData)
        
    if os.path.isfile(collectLock):
        keySlot[basinNum,iteration] = -0.9
        keyStatus = -0.9
        runFlag = False
        #statusData.genMsg = "ERROR: Basin ID: " + str(gageID) + " collection is locked. " + \
        #                    " for iteration: " + str(iteration) + " " + \
        #                    "Please remove: " + collectLock + " before continuing."
        #errMod.sendMsg(statusData)
        
    if keyStatus >= 1.0:
        # Model has already completed
        runFlag = False
        return
        
    # For uncompleted simulations that are still listed as running.
    if keyStatus == 0.5:
        # If a model is running for this basin, continue and set keyStatus to 0.5
        if basinStatus:
            keySlot[basinNum,iteration] = 0.5
            keyStatus = 0.5
            runFlag = False
            print "MODEL IS RUNNING"
            return
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
                keySlot[basinNum,iteration] = -0.25
                keyStatus = -0.25
            else:
                # Model has completed!
                print "MODEL COMPLETE! - READY TO COLLECT DATA"
                keySlot[basinNum,iteration] = 0.75
                keyStatus = 0.75
                runFlag = False
    # For simulations that are fresh
    if keyStatus == 0.0:
        if basinStatus:
            # Model is still running from previous instance of workflow. Allow it to continue.
            keySlot[basinNum,iteration] = 0.5
            keyStatus = 0.5
            runFlag = False
            print "MODEL IS RUNNING"
        elif collectStatus:
            # Collection code is running. Allow it to continue.
            keySlot[basinNum,iteration] = 0.9
            keyStatus = 0.9
            runFlag = False
            print "COLLECTION RUNNING"
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if not runFlag:
                # Model has completed!
                print "MODEL COMPLETE! - READY TO COLLECT DATA"
                keySlot[basinNum,iteration] = 0.75
                keyStatus = 0.75
                runFlag = False
                # Check to see if the collection complete file and output file
                # are present. 
                if collectComplete:
                    # Collection code also completed.
                    print "COLLECTION COMPLETE!"
                    keySlot[basinNum,iteration] = 1.0
                    keyStatus = 1.0
                    runFlag = False
                
    # For when the model failed TWICE and is locked.
    if keyStatus == -1.0:
        # If LOCK file exists, no simulation will take place. File must be removed
        # manually by user.
        if os.path.isfile(lockPath):
            runFlag = False
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
            else:
                # Check for complete flag
                if os.path.isfile(collectComplete):
                    print "COLLECTION COMPLETE"
                    keySlot[basinNum,iteration] = 1.0
                    keyStatus = 1.0
                    runFlag = False
                else:
                    # Upgrade the status
                    print "UPGRADING COLLECTION STATUS"
                    keySlot[basinNum,iteration] = 0.75
                    keyStatus = 0.75
                    runFlag = False
                
    # For when the collection routines have failed and are locked.
    if keyStatus == -0.9:
        # If LOCK file exists, no collection will take place. File must be removed
        # manually by user.
        if os.path.isfile(collectLock):
            runFlag = False
        else:
            # LOCK file was removed, uprade status to 0.75
            if collectStatus:
                print "COLLECTION RUNNING"
                keySlot[basinNum,iteration] = 0.9
                keyStatus = 0.9
                runFlag = False
            else:
                # Check for complete flag
                if os.path.isfile(collectComplete):
                    print "COLLECTION COMPLETE"
                    keySlot[basinNum,iteration] = 1.0
                    keyStatus = 1.0
                    runFlag = False
                else:
                    # Upgrade the status
                    print "UPGRADING COLLECTION STATUS"
                    keySlot[basinNum,iteration] = 0.75
                    keyStatus = 0.75
                    runFlag = False
                
    # For when the model crashed ONCE
    if keyStatus == -0.5:
        if basinStatus:
            print "MODEL RUNNING"
            # Model is running again, upgrade status
            # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
            keySlot[basinNum,iteration] = 0.5
            keyStatus = 0.5
            runFlag = False
            return
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                # Model has crashed again, time to lock it up and send a message out.
                statusData.genMsg = "ERROR: SENSITIVITY SIMULATION FOR GAGE: " + statusData.gages[basinNum] + \
                                    " ITERATION: " + str(iteration) + \
                                    " HAS FAILED A SECOND TIME. PLEASE FIX ISSUE AND " + \
                                    "MANUALLY REMOVE LOCK FILE: " + lockPath
                errMod.sendMsg(statusData)
                open(lockPath,'a').close()
                keySlot[basinNum,iteration] = -1.0
                keyStatus = -1.0
                runFlag = False
            else:
                # Check for complete flag
                if os.path.isfile(collectComplete):
                    print "COLLECTION COMPLETE"
                    keySlot[basinNum,iteration] = 1.0
                    keyStatus = 1.0
                    runFlag = False
                else:
                    # Upgrade the status
                    print "UPGRADING COLLECTION STATUS"
                    keySlot[basinNum,iteration] = 0.75
                    keyStatus = 0.75
                    runFlag = False
                
    if keyStatus == -0.25 and runFlag:
        # Restarting model from one crash
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        # Make symbolic links as necssary.
        try:
            linkToRst(statusData,gage,runDir)
        except:
            raise
            
        # Since these are sensitivity simulations, we are always going to be 
        # starting the model rom an existing RESTART file. startType = 1 is for
        # when we have cold starts. 
        startType = 2
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,4)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,4)
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
            cmd = "bsub < " + runDir + "/run_WH.sh"
        if statusData.jobRunType == 2:
            cmd = "qsub " + runDir + "/run_WH.sh"
        if statusData.jobRunType == 3:
            cmd = "sbatch " + runDir + "/run_WH.sh"
        if statusData.jobRunType == 4 or statusData.jobRunType == 5:
            cmd = runDir + "/run_WH.sh 1>" + runDir + "/WH_" + \
                  str(statusData.jobID) + "_" + str(gageID) + ".out" + \
                  ' 2>' + runDir + "/WH_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        try:
            if statusData.jobRunType == 1 or statusData.jobRunType == 2 or statusData.jobRunType == 3:
                subprocess.call(cmd,shell=True)
            if statusData.jobRunType == 4 or statusData.jobRunType == 5:
                p = subprocess.Popen([cmd],shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + \
                                str(gageMeta.gage[basinNum]) + " Iteration: " + str(iteration)
            raise
            
        # Revert statuses to -0.5 for next loop to convey the model crashed once. 
        keyStatus = -0.5
        keySlot[basinNum,iteration] = -0.5
        
    if keyStatus == 0.0 and runFlag:
        # Model needs to be either ran, or restarted
        # First delete namelist files if they exist.
        check = runDir + "/namelist.hrldas"
        check2 = runDir + "/hydro.namelist"
        if os.path.isfile(check):
            os.remove(check)
        if os.path.isfile(check2):
            os.remove(check2)
        
        # Make symbolic links as necssary.
        try:
            linkToRst(statusData,gage,runDir)
        except:
            raise
            
        # Since these are sensitivity simulations, we are always going to be 
        # starting the model rom an existing RESTART file. startType = 1 is for
        # when we have cold starts. 
        startType = 2
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,4)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,4)
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
            cmd = "bsub < " + runDir + "/run_WH.sh"
        if statusData.jobRunType == 2:
            cmd = "qsub " + runDir + "/run_WH.sh"
        if statusData.jobRunType == 3:
            cmd = "sbatch " + runDir + "/run_WH.sh"
        if statusData.jobRunType == 4 or statusData.jobRunType == 5:
            cmd = runDir + "/run_WH.sh 1>" + runDir + "/WH_" + \
                  str(statusData.jobID) + "_" + str(gageID) + ".out" + \
                  ' 2>' + runDir + "/WH_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        try:
            if statusData.jobRunType == 1 or statusData.jobRunType == 2 or statusData.jobRunType == 3:
                subprocess.call(cmd,shell=True)
            if statusData.jobRunType == 4 or statusData.jobRunType == 5:
                p = subprocess.Popen([cmd],shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + \
                                str(gageMeta.gage[basinNum]) + " Iteration: " + str(iteration)
            raise
            
        keyStatus = 0.5
        keySlot[basinNum,iteration] = 0.5
        
    # Collection program is running.
    if keyStatus == 0.9:
        # Collection routine is running. Check to see if it's completed. If not, lock this 
        # basin up and require the user to manually remove the LOCK file.
        if collectStatus:
            print "COLLECTING DATA"
            keySlot[basinNum,iteration] = 0.9
            keyStatus = 0.9
            runFlag = False
        else:
            if os.path.isfile(collectComplete):
                print "COLLECTION COMPLETE!"
                keySlot[basinNum,iteration] = 1.0
                keyStatus = 1.0
                runFlag = 1.0
                runFlag = False
            else:
                # Collection routine has failed. 
                statusData.genMsg = "ERROR: SENSITIVITY COLLECTION FOR GAGE: " + statusData.gages[basinNum] + \
                                    " ITERATION: " + str(iteration) + \
                                    " HAS FAILED. PLEASE FIX ISSUE AND " + \
                                    "MANUALLY REMOVE LOCK FILE: " + collectLock
                errMod.sendMsg(statusData)
                open(collectLock,'a').close()
                keySlot[basinNum,iteration] = -0.9
                keyStatus = -0.9
                runFlag = False
                
    if keyStatus == 0.75 and not runFlag:
        # Ready to fire off collection program.
        print "FIRING OFF COLLECTION"
        # Fire off model.
        if statusData.analysisRunType == 1:
            cmd = "bsub < " + collectScript
        if statusData.analysisRunType == 2:
            cmd = "qsub " + collectScript
        if statusData.analysisRunType == 3:
            cmd = "sbatch " + collectScript
        if statusData.analysisRunType == 4 or statusData.analysisRunType == 5:
            cmd = collectScript + " 1>" + runDir + "/SCOL_" + \
                  str(statusData.jobID) + "_" + str(gageID) + "_" + str(iteration) + ".out" + \
                  ' 2>' + runDir + "/SCOL_" + str(statusData.jobID) + "_" + \
                  str(gageID) + "_" + str(iteration) + ".err"
        try:
            if statusData.analysisRunType == 1 or statusData.analysisRunType == 2 or statusData.analysisRunType == 3:
                subprocess.call(cmd,shell=True)
            if statusData.analysisRunType == 4 or statusData.analysisRunType == 5:
                p = subprocess.Popen([cmd],shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch collection job for gage: " + \
                                str(gageMeta.gage[basinNum]) + " Iteration: " + str(iteration)
            raise
            
        keyStatus = 0.9
        keySlot[basinNum,iteration] = 0.9
        
    # Update job status in the Database table.
    try:
        db.updateSensIterationStatus(statusData,int(gageMeta.gageID),iteration,str(gageMeta.gage),keyStatus)
    except:
        raise
    
def genRNameList(jobData,workDir,gageMeta,gage):
    """
    Function to generate an R namelist file the sensitivity analysis R code
    will be expecting. The namelist file will contain some basic information 
    anout the working direectory, the input parameter file, and some 
    specific parameters to the sensitivity analysis.
    """
    rNameList = workDir + "/namelist.sensitivity"
    
    # Create the namelist file.
    try:
        fileObj = open(rNameList,'w')
        fileObj.write("### Model Parameters ###\n")
        fileObj.write("\n")
        inStr = "objFn <- \"" + str(jobData.objFunc) + "\"\n"
        fileObj.write(inStr)
        inStr = "nCores <- " + str(jobData.nCoresR) + "\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        fileObj.write("# Model run directory\n")
        inStr = "runDir <- \"" + workDir + "\"\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        fileObj.write("# Route link file\n")
        inStr = "rtlinkFile <- \"" + str(gageMeta.rtLnk) + "\"\n"
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write("# Parameter bounds\n")
        fileObj.write("# Must create a data table called paramBnds with one row " + \
                      " per parameter and columns labeled: \n")
        fileObj.write("# 'param' for parameter name, 'ini' for initial value, 'min' for " + \
                      "minimum value, 'max' for maximum value\n")
        inStr = "paramBnds <- read.table(\"" + workDir + "/sens_params.tbl\", header=TRUE, " + \
                "sep=\",\", stringsAsFactors=FALSE)\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        fileObj.write(" ### Model Evaluation Parameters ###\n")
        fileObj.write("\n")
        fileObj.write("# Gage ID to extract from the model output and compare against the obs\n")
        inStr = "siteId <- \"" + str(gageMeta.gage) + "\"\n"
        fileObj.write(inStr)
        fileObj.write("# feature_id that corresonds to where the gage is at\n")
        inStr = "linkId <- " + str(gageMeta.comID) + "\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        fileObj.write("# R dataset containing observations\n")
        fileObj.write("# Must contain an object called obsDf containing columns:\n")
        fileObj.write("# 'POSIXct' for POSIXct data, 'obs' for streamflow data\n")
        inStr = "obsFile <- \"" + workDir + "/OBS/obsStrData.Rdata\"\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        fileObj.write("# Start date for evaluation period (e.g. after spinup period)\n")
        inStr = "startDate <- as.POSIXct(\"" + jobData.bSensEvalDate.strftime('%Y-%m-%d') + "\", " + \
                "format=\"%Y-%m-%d\", tz=\"UTC\")\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        fileObj.write("# Sensitivity Analysis\n")
        # Hard coding several parameter search things here for now. May want to add this
        # to the configuration file for the workflow in the future.
        fileObj.write("SA_method <- \"DELSA\"\n")
        fileObj.write("SA_par_gen_method <- \"sobol\"\n")
        fileObj.write("SA_perturb <- 1.01\n")
        inStr = "SA_sample_size <- " + str(jobData.nSensSample) + "\n"
        fileObj.write(inStr)
        fileObj.write("SA_bootstrap_replicates <- 1000 # Number of replicates for the bootstrap resmaplins\n")
        fileObj.write("SA_quantileFrac <- 0.9 # if not provided will use the 0.9 for 90% quantile\n")
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + rNameList
        raise
        
def generateBsubPreProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic Function function to create BSUB script for running R
    sensitivity pre-processing routines.
    """
    
    outFile1 = workDir + "/run_WH_SENS_PREPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just going to manually over-write the file everytime to be safe.
        os.remove(outFile1)
    
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            fileObj.write('# LSF Batch Script to Run WRF-Hydro Calibration R Code\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#BSUB -n 1\n"
            fileObj.write(inStr)
            inStr = "#BSUB -J WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#BSUB -o ' + workDir + '/%J.out\n'
            fileObj.write(inStr)
            inStr = '#BSUB -e ' + workDir + '/%J.err\n'
            fileObj.write(inStr)
            fileObj.write('#BSUB -W 1:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#BSUB -q ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            if socket.gethostname()[0:8] == 'cheyenne':
                inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
                fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./sensPreProc.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/sensPreProc.sh"
    
    runRProgram = workDir + "/sens_workflow_pre.R"
        
    if not os.path.isfile(outFile2):
        # This is the file that will run the R code first to generate params_new.txt and
        # params_stats.txt. Python is called next, which will read in 
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + gageMeta.fullDom + \
                          ' ' + gageMeta.hydroSpatial + ' ' + gageMeta.soilFile + ' ' + \
                          gageMeta.gwFile + ' ' + workDir + ' ' + str(jobData.nSensIter) + ' \n')
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
        
def generatePbsPreProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic Function function to create PBS script for running R
    pre-processing routines.
    """
    
    outFile1 = workDir + "/run_WH_SENS_PREPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just going to manually over-write the file everytime to be safe.
        os.remove(outFile1)
    
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            fileObj.write('# PBS Batch Script to Run WRF-Hydro Calibration R Code\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#PBS -A " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#PBS -N WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#PBS -o ' + workDir + '/WH_SENS_PREPROC_' + str(jobData.jobID) + '_' + str(gageID) + '.out\n'
            fileObj.write(inStr)
            inStr = '#PBS -e ' + workDir + '/WH_SENS_PREPROC_' + str(jobData.jobID) + '_' + str(gageID) + '.err\n'
            fileObj.write(inStr)
            #nCoresPerNode = int(jobData.nCoresR/jobData.nNodesR)
            #inStr = "#PBS -l select=" + str(jobData.nNodesR) + ":ncpus=" + str(nCoresPerNode) + \
            #        ":mpiprocs=" + str(nCoresPerNode) + "\n"
            inStr = "#PBS -l select=1:ncpus=1:mpiprocs=1\n"
            fileObj.write(inStr)
            fileObj.write('#PBS -l walltime=01:00:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#PBS -q ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            #if socket.gethostname()[0:8] == 'cheyenne':
            #    inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
            #    fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./sensPreProc.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/sensPreProc.sh"
    
    runRProgram = workDir + "/sens_workflow_pre.R"
        
    if not os.path.isfile(outFile2):
        # This is the file that will run the R code first to generate params_new.txt and
        # params_stats.txt. Python is called next, which will read in 
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + gageMeta.fullDom + \
                          ' ' + gageMeta.hydroSpatial + ' ' + gageMeta.soilFile + ' ' + \
                          gageMeta.gwFile + ' ' + workDir + ' ' + str(jobData.nSensIter) + ' \n')
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
        
def generateSlurmPreProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic Function function to create Slurm script for running R
    pre-processing routines. 
    """
    
    outFile1 = workDir + "/run_WH_SENS_PREPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just going to manually over-write the file everytime to be safe.
        os.remove(outFile1)
    
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            fileObj.write('# Slurm Batch Script to Run WRF-Hydro Calibration R Code\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#SBATCH -A " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#SBATCH -J WH_SENS_PREPROC_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#SBATCH -o ' + workDir + '/WH_SENS_PREPROC_' + str(jobData.jobID) + '_' + str(gageID) + '.out\n'
            fileObj.write(inStr)
            inStr = '#SBATCH -e ' + workDir + '/WH_SENS_PREPROC_' + str(jobData.jobID) + '_' + str(gageID) + '.err\n'
            fileObj.write(inStr)
            inStr = "#SBATCH -N 1\n"
            fileObj.write(inStr)
            fileObj.write('#SBATCH -t 01:00:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#SBATCH -p ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            if socket.gethostname()[0:8] == 'cheyenne':
                inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
                fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./sensPreProc.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/sensPreProc.sh"
    
    runRProgram = workDir + "/sens_workflow_pre.R"
        
    if not os.path.isfile(outFile2):
        # This is the file that will run the R code first to generate params_new.txt and
        # params_stats.txt. Python is called next, which will read in 
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + gageMeta.fullDom + \
                          ' ' + gageMeta.hydroSpatial + ' ' + gageMeta.soilFile + ' ' + \
                          gageMeta.gwFile + ' ' + workDir + ' ' + str(jobData.nSensIter) + ' \n')
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
        
def generateMpiPreProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic function to create mpiexec/mpirun script for running R pre-processing
    routines.
    """
    
    outFile1 = workDir + "/run_WH_SENS_PREPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just gonig to manually over-write the file everytime to be safe.
        os.remove(outFile1)
        
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            if jobData.analysisRunType == 4:
                inStr = 'mpiexec -n 1 ./SPRE' + \
                str(jobData.jobID) + str(gageID) +'\n'
            if jobData.analysisRunType == 5:
                inStr = 'mpirun -np 1 ./SPRE' + \
                str(jobData.jobID) + str(gageID) +'\n'
            fileObj.write(inStr)
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create " + outFile1
            raise
            
    # Make the file an executable.
    cmd = "chmod +x " + outFile1
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + outFile1 + " to an executable."
        raise
            
    outFile2 = workDir + '/sensPreProc.sh'
    outLink2 = workDir + '/SPRE' + str(jobData.jobID) + str(gageID) 
    
    runRProgram = workDir + '/sens_workflow_pre.R'
    
    if not os.path.isfile(outFile2):
        # This is the file that will run R code. First to generate params_new.txt and
        # params_stats.txt. Python is called next, which will generate new parameters.
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + gageMeta.fullDom + \
                          ' ' + gageMeta.hydroSpatial + ' ' + gageMeta.soilFile + ' ' + \
                          gageMeta.gwFile + ' ' + workDir + ' ' + str(jobData.nSensIter) + ' \n')
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
        
    # Make symbolic link to newly created executable, which will be called by
    # mpiexec/mpirun.
    if not os.path.islink(outLink2):
        try:
            os.symlink(outFile2,outLink2)
        except:
            jobData.errMsg = "ERROR: Failure to create symbolic link: " + outLink2
            raise
            
def generateBsubScript(jobData,gageID,runDir,gageMeta,iteration):
    """
    Generic function to create a run script that will be called by bsub
    to execute the model.
    """
    
    outFile = runDir + "/run_WH.sh"
    
    if os.path.isfile(outFile):
        jobData.errMsg = "ERROR: Run script: " + outFile + " already exists."
        raise Exception()
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        fileObj.write('# LSF Batch Script to Run WRF-Hydro Calibration Simulations\n')
        fileObj.write('#\n')
        if len(jobData.acctKey.strip()) > 0:
            inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
            fileObj.write(inStr)
        fileObj.write('#BSUB -x\n')
        inStr = "#BSUB -n " + str(jobData.nCoresMod) + '\n'
        fileObj.write(inStr)
        #fileObj.write('#BSUB -R "span[ptile=16]"\n')
        inStr = "#BSUB -J WHS" + str(jobData.jobID) + str(gageID) + str(iteration) + '\n'
        fileObj.write(inStr)
        inStr = '#BSUB -o ' + runDir + '/%J.out\n'
        fileObj.write(inStr)
        inStr = '#BSUB -e ' + runDir + '/%J.err\n'
        fileObj.write(inStr)
        fileObj.write('#BSUB -W 8:00\n')
        if len(jobData.queName.strip()) > 0:
            inStr = '#BSUB -q ' + str(jobData.queName) + '\n'
            fileObj.write(inStr)
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        fileObj.write('mpirun.lsf ./wrf_hydro.exe\n')
        fileObj.write(inStr)
        #inStr = 'rm -rf *.LDASOUT_DOMAIN1\n'
        #fileObj.write(inStr)
        #inStr = 'rm -rf *.CHRTOUT_DOMAIN1\n'
        #fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generatePbsScript(jobData,gageID,runDir,gageMeta,iteration):
    """
    Generic function to create a script that will be called by qsub
    to execute the model.
    """
    
    outFile = runDir + "/run_WH.sh"
    
    if os.path.isfile(outFile):
        jobData.errMsg = "ERROR: Run script: " + outFile + " already exists."
        raise Exception()
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        fileObj.write('# PBS Batch Script to Run WH Calibration Simulations\n')
        fileObj.write('#\n')
        inStr = "#PBS -N WHS" + str(jobData.jobID) + str(gageID) + str(iteration) + '\n'
        fileObj.write(inStr)
        if len(jobData.acctKey.strip()) > 0:
            inStr = "#PBS -A " + str(jobData.acctKey) + '\n'
            fileObj.write(inStr)
        inStr = "#PBS -l walltime=08:00:00\n"
        fileObj.write(inStr)
        if len(jobData.queName.strip()) > 0:
            inStr = "#PBS -q " + str(jobData.queName) + "\n"
            fileObj.write(inStr)
        inStr = "#PBS -o " + runDir + "/WHS" + str(jobData.jobID) + str(gageID) + str(iteration) + ".out\n"
        fileObj.write(inStr)
        inStr = "#PBS -e " + runDir + "/WHS" + str(jobData.jobID) + str(gageID) + str(iteration) + ".err\n"
        fileObj.write(inStr)
        nCoresPerNode = int(jobData.nCoresMod/jobData.nNodesMod)
        inStr = "#PBS -l select=" + str(jobData.nNodesMod) + ":ncpus=" + str(nCoresPerNode) + \
                ":mpiprocs=" + str(nCoresPerNode) + "\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        fileObj.write('mpiexec_mpt ./wrf_hydro.exe\n')
        fileObj.write(inStr)
        #inStr = 'rm -rf *.LDASOUT_DOMAIN1\n'
        #fileObj.write(inStr)
        #inStr = 'rm -rf *.CHRTOUT_DOMAIN1\n'
        #fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generateSlurmScript(jobData,gageID,runDir,gageMeta,iteration):
    """
    Generic function to create a script that will be called by Slurm
    to execute the model.
    """

    outFile = runDir + "/run_WH.sh"

    if os.path.isfile(outFile):
        jobData.errMsg = "ERROR: Run script: " + outFile + " already exists."
        raise Exception()
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        fileObj.write('# Slurm Batch Script to Run WH Calibration Simulations\n')
        fileObj.write('#\n')
        inStr = '#SBATCH -J WHS_' + str(jobData.jobID) + str(gageID) + str(iteration) + '\n'
        fileObj.write(inStr)
        if len(jobData.acctKey.strip()) > 0:
            inStr = '#SBATCH -A ' + str(jobData.acctKey) + '\n'
            fileObj.write(inStr)
        inStr = '#SBATCH -t 08:00:00\n'
        fileObj.write(inStr)
        if len(jobData.queName.strip()) > 0:
            inStr = '#SBATCH -p ' + str(jobData.queName) + '\n'
            fileObj.write(inStr)
        inStr = "#SBATCH -o " + runDir + "/WHS_" + str(jobData.jobID) + str(gageID) + str(iteration) + ".out\n"
        fileObj.write(inStr)
        inStr = "#SBATCH -e " + runDir + "/WHS_" + str(jobData.jobID) + str(gageID) + str(iteration) + ".err\n"
        fileObj.write(inStr)
        inStr = '#SBATCH -N ' + str(jobData.nNodesMod) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        inStr = 'srun -n ' + str(jobData.nCoresMod) + ' ./wrf_hydro.exe\n'
        fileObj.write(inStr)
        #inStr = 'rm -rf *.LDASOUT_DOMAIN1\n'
        #fileObj.write(inStr)
        #inStr = 'rm -rf *.CHRTOUT_DOMAIN1\n'
        #fileObj.write(inStr)
        fileObj.close        
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generateMpiScript(jobData,gageID,runDir,gageMeta,iteration):
    """
    Generic function to create a run script that will use mpiexec/mpirun to execute
    the model.
    """
    
    outFile = runDir + "/run_WH.sh"
    
    if os.path.isfile(outFile):
        jobData.errMsg = "ERROR: Run script: " + outFile + " already exists."
        raise Exception()
        
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        if jobData.jobRunType == 4:
            inStr = 'mpiexec -n ' + str(int(jobData.nCoresMod)) + ' ./WHS' + \
                    str(jobData.jobID) + str(gageID) + str(iteration) + '\n'
        if jobData.jobRunType == 5:
            inStr = 'mpirun -np ' + str(int(jobData.nCoresMod)) + ' ./WHS' + \
                    str(jobData.jobID) + str(gageID) + str(iteration) + '\n'
        fileObj.write(inStr)
        inStr = "Rscript " + runDir + "/../Collect_simulated_flow.R " + runDir + "\n"
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
    
    # Make the file an executable.
    cmd = "chmod +x " + outFile
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + outFile + " to an executable."
        raise
        
def generateBsubPostProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic Function function to create BSUB script for running R
    sensitivity post-processing routines.
    """
    
    outFile1 = workDir + "/run_WH_SENS_POSTPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just going to manually over-write the file everytime to be safe.
        os.remove(outFile1)
    
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            fileObj.write('# LSF Batch Script to Run WRF-Hydro Calibration R Code\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#BSUB -n 1\n"
            fileObj.write(inStr)
            inStr = "#BSUB -J WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#BSUB -o ' + workDir + '/%J.out\n'
            fileObj.write(inStr)
            inStr = '#BSUB -e ' + workDir + '/%J.err\n'
            fileObj.write(inStr)
            fileObj.write('#BSUB -W 3:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#BSUB -q ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            if socket.gethostname()[0:8] == 'cheyenne':
                inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
                fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./sensPreProc.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/sensPreProc.sh"
    
    runRProgram = workDir + "/sens_workflow_pre.R"
        
    if not os.path.isfile(outFile2):
        # This is the file that will run the R code first to generate params_new.txt and
        # params_stats.txt. Python is called next, which will read in 
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + gageMeta.fullDom + \
                          ' ' + gageMeta.hydroSpatial + ' ' + gageMeta.soilFile + ' ' + \
                          gageMeta.gwFile + ' ' + workDir + ' ' + str(jobData.nSensIter) + ' \n')
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
        
def generatePbsPostProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic Function function to create PBS script for running R
    post-processing routines.
    """
    
    outFile1 = workDir + "/run_WH_SENS_POSTPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just going to manually over-write the file everytime to be safe.
        os.remove(outFile1)
    
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            fileObj.write('# PBS Batch Script to Run WRF-Hydro Calibration R Code\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#PBS -A " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#PBS -N WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#PBS -o ' + workDir + "/WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + str(gageID) + '.out\n'
            fileObj.write(inStr)
            inStr = '#PBS -e ' + workDir + "/WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + str(gageID) + '.err\n'
            fileObj.write(inStr)
            #nCoresPerNode = int(jobData.nCoresR/jobData.nNodesR)
            #inStr = "#PBS -l select=" + str(jobData.nNodesR) + ":ncpus=" + str(nCoresPerNode) + \
            #        ":mpiprocs=" + str(nCoresPerNode) + "\n"
            inStr = "#PBS -l select=1:ncpus=1:mpiprocs=1\n"
            fileObj.write(inStr)
            fileObj.write('#PBS -l walltime=03:00:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#PBS -q ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            #if socket.gethostname()[0:8] == 'cheyenne':
            #    inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
            #    fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./sensPostProc.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/sensPostProc.sh"
    
    runRProgram = workDir + "/sens_workflow_post.R"
        
    if not os.path.isfile(outFile2):
        # This is the file that will run the R code first to generate params_new.txt and
        # params_stats.txt. Python is called next, which will read in 
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
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
        
def generateSlurmPostProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic Function function to create Slurm script for running R
    post-processing routines. 
    """
    
    outFile1 = workDir + "/run_WH_SENS_POSTPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just going to manually over-write the file everytime to be safe.
        os.remove(outFile1)
    
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            fileObj.write('# Slurm Batch Script to Run WRF-Hydro Calibration R Code\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#SBATCH -A " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#SBATCH -J WH_SENS_POSTPROC_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
            fileObj.write(inStr)
            inStr = '#SBATCH -o ' + workDir + '/WH_SENS_POSTPROC_' + str(jobData.jobID) + '_' + str(gageID) + '.out\n'
            fileObj.write(inStr)
            inStr = '#SBATCH -e ' + workDir + '/WH_SENS_POSTPROC_' + str(jobData.jobID) + '_' + str(gageID) + '.err\n'
            fileObj.write(inStr)
            inStr = "#SBATCH -N 1\n"
            fileObj.write(inStr)
            fileObj.write('#SBATCH -t 03:00:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#SBATCH -p ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            if socket.gethostname()[0:8] == 'cheyenne':
                inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
                fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./sensPostProc.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile1
            raise    
            
    outFile2 = workDir + "/sensPostProc.sh"
    
    runRProgram = workDir + "/sens_workflow_post.R"
        
    if not os.path.isfile(outFile2):
        # This is the file that will run the R code first to generate params_new.txt and
        # params_stats.txt. Python is called next, which will read in 
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
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
        
def generateMpiPostProcScript(jobData,gageID,runDir,workDir,gageMeta):
    """
    Generic function to create mpiexec/mpirun script for running R post-processing
    routines.
    """
    
    outFile1 = workDir + "/run_WH_SENS_POSTPROC.sh"
    
    if os.path.isfile(outFile1):
        # We are just gonig to manually over-write the file everytime to be safe.
        os.remove(outFile1)
        
    if not os.path.isfile(outFile1):
        try:
            fileObj = open(outFile1,'w')
            fileObj.write('#!/bin/bash\n')
            inStr = 'cd ' + workDir + '\n'
            fileObj.write(inStr)
            if jobData.analysisRunType == 4:
                inStr = 'mpiexec -n 1 ./SPOS' + \
                str(jobData.jobID) + str(gageID) +'\n'
            if jobData.analysisRunType == 5:
                inStr = 'mpirun -np 1 ./SPOS' + \
                str(jobData.jobID) + str(gageID) +'\n'
            fileObj.write(inStr)
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create " + outFile1
            raise
            
    # Make the file an executable.
    cmd = "chmod +x " + outFile1
    try:
        subprocess.call(cmd,shell=True)
    except:
        jobData.errMsg = "ERROR: Failure to convert: " + outFile1 + " to an executable."
        raise
            
    outFile2 = workDir + '/sensPostProc.sh'
    outLink2 = workDir + '/SPOS' + str(jobData.jobID) + str(gageID) 
    
    runRProgram = workDir + '/sens_workflow_post.R'
    
    if not os.path.isfile(outFile2):
        # This is the file that will run R code. First to generate params_new.txt and
        # params_stats.txt. Python is called next, which will generate new parameters.
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + '\n')
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
        
    # Make symbolic link to newly created executable, which will be called by
    # mpiexec/mpirun.
    if not os.path.islink(outLink2):
        try:
            os.symlink(outFile2,outLink2)
        except:
            jobData.errMsg = "ERROR: Failure to create symbolic link: " + outLink2
            raise
            
def generateBsubCollectScript(jobData,gageID,runDir,gageMeta,iteration,workDir):
    """
    Generic function to create BSUB script for running an R program to collect
    data into an Rdataset for sensitivity analysis.
    """
    
    outFile = runDir + "/run_collection.sh"
    
    if os.path.isfile(outFile):
        # We are just going to manually over-write the file to be safe.
        os.remove(outFile)
        
    if not os.path.isfile(outFile): 
        try:
            fileObj = open(outFile,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#BSUB -P " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#BSUB -n 1\n"
            fileObj.write(inStr)
            inStr = "#BSUB -J WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
                    str(gageID) + '_' + str(iteration) + '\n'
            fileObj.write(inStr)
            inStr = '#BSUB -o ' + runDir + '/%J.out\n'
            fileObj.write(inStr)
            inStr = '#BSUB -e ' + runDir + '/%J.err\n'
            fileObj.write(inStr)
            fileObj.write('#BSUB -W 3:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#BSUB -q ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            if socket.gethostname()[0:8] == 'cheyenne':
                inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
                fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + runDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./collectOutput.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile
            raise    
            
    outFile2 = runDir + "/collectOutput.sh"
    
    runRProgram = runDir + "/Collect_simulated_flow.R"
    
    # Make a link to the R namelist created during pre-processing.
    nameListR = workDir + "/namelist.sensitivity"
    link = runDir + "/namelist.sensitivity"
    
    if not os.path.isfile(link):
        if not os.path.isfile(nameListR):
            jobData.errMsg = "ERROR: Failure to find: " + nameListR
            raise
        try:
            os.symlink(nameListR,link)
        except:
            jobData.errMsg = "ERROR: Failure to make link: " + link
            raise
    
    if not os.path.isfile(outFile2):
        # This is the file that will run R code to collect model output into a single Rdataset
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + ' ' + runDir + '\n')
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
        
def generatePbsCollectScript(jobData,gageID,runDir,gageMeta,iteration,workDir):
    """
    Generic function to create QSUB script for running an R program to collect
    data into an Rdataset for sensitivity analysis.
    """
    
    outFile = runDir + "/run_collection.sh"
    
    if os.path.isfile(outFile):
        # We are just going to manually over-write the file to be safe.
        os.remove(outFile)
        
    if not os.path.isfile(outFile): 
        try:
            fileObj = open(outFile,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#PBS -A " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#PBS -N WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + str(gageID) + \
                    "_" + str(iteration) + '\n'
            fileObj.write(inStr)
            inStr = '#PBS -o ' + runDir + "/WH_SENS_COLLECT_" + str(jobData.jobID) + \
                    "_" + str(gageID) + "_" + str(iteration) + '.out\n'
            fileObj.write(inStr)
            inStr = '#PBS -e ' + runDir + "/WH_SENS_COLLECT_" + str(jobData.jobID) + \
                    "_" + str(gageID) + "_" + str(iteration) + '.err\n'
            fileObj.write(inStr)
            nCoresPerNode = int(jobData.nCoresR/jobData.nNodesR)
            inStr = "#PBS -l select=" + str(jobData.nNodesR) + ":ncpus=" + str(nCoresPerNode) + \
                    ":mpiprocs=" + str(nCoresPerNode) + "\n"
            #inStr = "#PBS -l select=1:ncpus=1:mpiprocs=1\n"
            fileObj.write(inStr)
            fileObj.write('#PBS -l walltime=01:00:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#PBS -q ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            #if socket.gethostname()[0:8] == 'cheyenne':
            #    inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
            #    fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + runDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./collectOutput.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile
            raise    
            
    outFile2 = runDir + "/collectOutput.sh"
    
    runRProgram = runDir + "/Collect_simulated_flow.R"
    
    # Make a link to the R namelist created during pre-processing.
    nameListR = workDir + "/namelist.sensitivity"
    link = runDir + "/namelist.sensitivity"
    
    if not os.path.isfile(link):
        if not os.path.isfile(nameListR):
            jobData.errMsg = "ERROR: Failure to find: " + nameListR
            raise
        try:
            os.symlink(nameListR,link)
        except:
            jobData.errMsg = "ERROR: Failure to make link: " + link
            raise
    
    if not os.path.isfile(outFile2):
        # This is the file that will run R code to collect model output into a single Rdataset
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + ' ' + runDir + '\n')
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
        
def generateSlurmCollectScript(jobData,gageID,runDir,gageMeta,iteration,workDir):
    """
    Generic function to create Slurm script for running an R program to collect
    data into an Rdataset for sensitivity analysis.
    """
    
    outFile = runDir + "/run_collection.sh"
    
    if os.path.isfile(outFile):
        # We are just going to manually over-write the file to be safe.
        os.remove(outFile)
        
    if not os.path.isfile(outFile): 
        try:
            fileObj = open(outFile,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('#\n')
            if len(jobData.acctKey.strip()) > 0:
                inStr = "#SBATCH -A " + str(jobData.acctKey) + '\n'
                fileObj.write(inStr)
            inStr = "#SBATCH -J WH_SENS_COLLECT_" + str(jobData.jobID) + "_" + \
                    str(gageID) + "_" + str(iteration) + '\n'
            fileObj.write(inStr)
            inStr = '#SBATCH -o ' + runDir + '/WH_SENS_COLLECTION_' + str(jobData.jobID) + \
                    '_' + str(gageID) + "_" + str(iteration) + '.out\n'
            fileObj.write(inStr)
            inStr = '#SBATCH -e ' + runDir + '/WH_SENS_COLLECTION_' + str(jobData.jobID) + \
                    '_' + str(gageID) + "_" + str(iteration) + '.err\n'
            fileObj.write(inStr)
            inStr = "#SBATCH -N 1\n"
            fileObj.write(inStr)
            fileObj.write('#SBATCH -t 03:00:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#SBATCH -p ' + str(jobData.queNameAnalysis) + '\n'
                fileObj.write(inStr)
            # Temporary handling of Cheyenne/Geyser environment for NCAR.
            if socket.gethostname()[0:8] == 'cheyenne':
                inStr = 'source /glade/u/home/karsten/.profile_yellowstone\n'
                fileObj.write(inStr)
            fileObj.write('\n')
            inStr = 'cd ' + runDir + '\n'
            fileObj.write(inStr)
            fileObj.write('./collectOutput.sh\n')
            fileObj.close
        except:
            jobData.errMsg = "ERROR: Failure to create: " + outFile
            raise    
            
    outFile2 = runDir + "/collectOutput.sh"
    
    runRProgram = runDir + "/Collect_simulated_flow.R"
    
    # Make a link to the R namelist created during pre-processing.
    nameListR = workDir + "/namelist.sensitivity"
    link = runDir + "/namelist.sensitivity"
    
    if not os.path.isfile(link):
        if not os.path.isfile(nameListR):
            jobData.errMsg = "ERROR: Failure to find: " + nameListR
            raise
        try:
            os.symlink(nameListR,link)
        except:
            jobData.errMsg = "ERROR: Failure to make link: " + link
            raise
    
    if not os.path.isfile(outFile2):
        # This is the file that will run R code to collect model output into a single Rdataset
        try:
            fileObj = open(outFile2,'w')
            fileObj.write('#!/bin/bash\n')
            fileObj.write('Rscript ' + runRProgram + ' ' + runDir + '\n')
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
        
def generateMpiCollectScript(jobData,gageID,runDir,gageMeta,iteration,workDir):
    """
    Generic function to create MPI script for running an R program to collect
    data into an Rdataset for sensitivity analysis.
    """
    
    outFile = runDir + "/SCOL" + str(jobData.jobID) + str(gageID) + str(iteration)
    runRProgram = runDir + "/Collect_simulated_flow.R"
    
    # Make a link to the R namelist created during pre-processing.
    nameListR = workDir + "/namelist.sensitivity"
    link = runDir + "/namelist.sensitivity"
    
    if os.path.isfile(outFile):
        # We are just going to manually over-write the file to be safe.
        os.remove(outFile)
        
    if not os.path.isfile(link):
        if not os.path.isfile(nameListR):
            jobData.errMsg = "ERROR: Failure to find: " + nameListR
            raise
        try:
            os.symlink(nameListR,link)
        except:
            jobData.errMsg = "ERROR: Failure to make link: " + link
            raise
        
    if not os.path.isfile(outFile): 
        try:
            fileObj = open(outFile,'w')
            fileObj.write('#!/bin/bash\n')
            inStr = 'cd ' + runDir + '\n'
            fileObj.write(inStr)
            fileObj.write('Rscript ' + runRProgram + ' ' + runDir + '\n')
            fileObj.write('exit\n')
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
    link1 = runDir + "/RESTART." + statusData.bSensDate.strftime('%Y%m%d') + "00_DOMAIN1"
    link2 = runDir + "/HYDRO_RST." + statusData.bSensDate.strftime('%Y-%m-%d') + "_00:00_DOMAIN1"
    if not os.path.islink(link1):
        os.symlink(check1,link1)
    if not os.path.islink(link2):
        os.symlink(check2,link2)