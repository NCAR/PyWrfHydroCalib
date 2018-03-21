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

def runModel(statusData,staticData,db,gageID,gage,keySlot,basinNum,pbsJobId):
    """
    Generic function for running the model. Some basic information about
    the run directory, beginning date, ending dates, account keys,
    number of cores to use, etc will be used to compose a BSUB
    submision script,execute mpiexec/mpirun, or a QSUB script. 
    This function will walk the run directory 
    to determine where the model left off. If no restart files exist,
    then the function will assume the model has not ran at all. Both
    the LSM and hydro restart files must be present in order for the
    model to restart. 
    """
    runDir = statusData.jobDir + "/" + gage + "/RUN.SPINUP/OUTPUT"
    workDir = statusData.jobDir + "/" + gage + "/RUN.SPINUP"
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
        raise Exception()
    if not os.path.isdir(runDir):
        statusData.errMsg = "ERROR: " + runDir + " not found."
        raise Exception()
        
    # Pull gage metadata
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
        
    if statusData.jobRunType == 1:
        # If BSUB run script doesn't exist, create it here.
        bsubFile = runDir + "/run_WH.sh"
        if not os.path.isfile(bsubFile):
            try:
                generateBsubScript(statusData,int(gageID),runDir,gageMeta)
            except:
                raise
    if statusData.jobRunType == 2:
        pbsFile = runDir + "/run_WH.sh"
        if not os.path.isfile(pbsFile):
            try:
                generatePbsScript(statusData,int(gageID),runDir,gageMeta)
            except:
                raise
    if statusData.jobRunType == 3:
        slurmFile = runDir + "/run_WH.sh"
        if not os.path.isfile(slurmFile):
            try:
                generateSlurmScript(statusData,int(gageID),runDir,gageMeta)
            except:
                raise
    if statusData.jobRunType == 4 or statusData.jobRunType == 5:
        # If run script doesn't exist, create it here.
        runScript = runDir + "/run_WH.sh"
        if not os.path.isfile(runScript):
            try:
                generateMpiScript(statusData,int(gageID),runDir,gageMeta)
            except:
                raise
    
    # Calculate datetime objects
    begDate = statusData.bSpinDate
    endDate = statusData.eSpinDate
        
    # Initialize status
    keyStatus = keySlot[basinNum]
    
    try:
        basinStatus = statusMod.checkBasJob(statusData,basinNum,pbsJobId)
    except:
        raise
        
    # Create path to LOCK file if neeced
    lockPath = workDir + "/RUN.LOCK"
    
    # If the LOCK file is present, report this and lock things up.
    if os.path.isfile(lockPath):
        keySlot[basinNum] = -1.0
        keyStatus = -1.0
        runFlag = False
        print "MODEL IS LOCKED"
        #statusData.genMsg = "ERROR: Basin ID: " + str(gageID) + " Is locked. " + \
        #                    "Please remove: " + lockPath + " before continuing."
        #errMod.sendMsg(statusData)
                            
    
    if keyStatus == 1.0:
        # Model has already completed
        runFlag = False
        return
        
    # For uncompleted simulations that are still listed as running.
    if keyStatus == 0.5:
        # If a model is running for this basin, continue and set keyStatus to 0.5
        if basinStatus:
            keySlot[basinNum] = 0.5
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
                statusData.genMsg = "WARNING: Simulation for gage: " + statusData.gages[basinNum] + \
                                    " Failed. Attempting to restart."
                print statusData.genMsg
                keySlot[basinNum] = -0.25
                keyStatus = -0.25
            else:
                # Model has completed!
                keySlot[basinNum] = 1.0
                keyStatus = 1.0
                runFlag = False
           
    # For simulations that are fresh
    if keyStatus == 0.0:
        if basinStatus:
            # Model is still running from previous instance of workflow. Allow it to continue.
            keySlot[basinNum] = 0.5
            keyStatus = 0.5
            runFlag = False
        else:
            runStatus = statusMod.walkMod(begDate,endDate,runDir)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if not runFlag:
                # Model simulation completed before workflow was restarted
                keySlot[basinNum] = 1.0
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
                keySlot[basinNum] = 0.0
                keyStatus = 0.0
            else:
                # Model sucessfully completed.
                keySlot[basinNum] = 1.0
                keyStatus = 1.0
                runFlag = False
                
    # For when the model crashed ONCE
    if keyStatus == -0.5:
        if basinStatus:
            # Model is running again, upgrade status
            # PLACEHOLDER FOR MORE ROBUST METHOD HERE.
            keySlot[basinNum] = 0.5
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
                keySlot[basinNum] = -1.0
                keyStatus = -1.0
                runFlag = False
            else:
                # Model sucessfully completed from first failed attempt.
                keySlot[basinNum] = 1.0
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
        
        if begDate == staticData.bSpinDate:
            startType = 1
        else:
            startType = 2
        
        try:
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,0)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,0)
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
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        # Revert statuses to -0.5 for next loop to convey the model crashed once. 
        keyStatus = -0.5
        keySlot[basinNum] = -0.5
        
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
            namelistMod.createHrldasNL(gageMeta,staticData,runDir,startType,begDate,endDate,0)
            namelistMod.createHydroNL(gageMeta,staticData,runDir,startType,begDate,endDate,0)
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
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
            raise
            
        keyStatus = 0.5
        keySlot[basinNum] = 0.5
                
def generateBsubScript(jobData,gageID,runDir,gageMeta):
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
        inStr = "#BSUB -J WH_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
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
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.LDASOUT_DOMAIN1\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.CHRTOUT_DOMAIN1\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.CHANOBS_DOMAIN1\n'
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generatePbsScript(jobData,gageID,runDir,gageMeta):
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
        inStr = "#PBS -N WH_" + str(jobData.jobID) + "_" + str(gageID) + '\n'
        fileObj.write(inStr)
        if len(jobData.acctKey.strip()) > 0:
            inStr = "#PBS -A " + str(jobData.acctKey) + '\n'
            fileObj.write(inStr)
        inStr = "#PBS -l walltime=08:00:00\n"
        fileObj.write(inStr)
        if len(jobData.queName.strip()) > 0:
            inStr = "#PBS -q " + str(jobData.queName) + "\n"
            fileObj.write(inStr)
        inStr = "#PBS -o " + runDir + "/WH_" + str(jobData.jobID) + "_" + str(gageID) + ".out\n"
        fileObj.write(inStr)
        inStr = "#PBS -e " + runDir + "/WH_" + str(jobData.jobID) + "_" + str(gageID) + ".err\n"
        fileObj.write(inStr)
        nCoresPerNode = int(jobData.nCoresMod/jobData.nNodesMod)
        inStr = "#PBS -l select=" + str(jobData.nNodesMod) + ":ncpus=" + str(nCoresPerNode) + \
                ":mpiprocs=" + str(nCoresPerNode) + "\n"
        fileObj.write(inStr)
        fileObj.write("\n")
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        fileObj.write('mpiexec_mpt ./wrf_hydro.exe\n')
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.LDASOUT_DOMAIN1\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.CHRTOUT_DOMAIN1\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.CHANOBS_DOMAIN1\n'
        fileObj.write(inStr)
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generateSlurmScript(jobData,gageID,runDir,gageMeta):
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
        inStr = '#SBATCH -J WH_' + str(jobData.jobID) + "_" + str(gageID) + '\n'
        fileObj.write(inStr)
        if len(jobData.acctKey.strip()) > 0:
            inStr = '#SBATCH -A ' + str(jobData.acctKey) + '\n'
            fileObj.write(inStr)
        inStr = '#SBATCH -t 08:00:00\n'
        fileObj.write(inStr)
        if len(jobData.queName.strip()) > 0:
            inStr = '#SBATCH -p ' + str(jobData.queName) + '\n'
            fileObj.write(inStr)
        inStr = "#SBATCH -o " + runDir + "/WH_" + str(jobData.jobID) + "_" + str(gageID) + ".out\n"
        fileObj.write(inStr)
        inStr = "#SBATCH -e " + runDir + "/WH_" + str(jobData.jobID) + "_" + str(gageID) + ".err\n"
        fileObj.write(inStr)
        inStr = '#SBATCH -N ' + str(jobData.nNodesMod) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        inStr = 'srun -n ' + str(jobData.nCoresMod) + ' ./wrf_hydro.exe\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.LDASOUT_DOMAIN1\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.CHRTOUT_DOMAIN1\n'
        fileObj.write(inStr)
        inStr = 'rm -rf *.CHANOBS_DOMAIN1\n'
        fileObj.write(inStr)
        fileObj.close        
    except:
        jobData.errMsg = "ERROR: Failure to create: " + outFile
        raise
        
def generateMpiScript(jobData,gageID,runDir,gageMeta):
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
            inStr = 'mpiexec -n ' + str(int(jobData.nCoresMod)) + ' ./W' + \
                    str(jobData.jobID) + str(gageID) + '\n'
        if jobData.jobRunType == 5:
            inStr = 'mpirun -np ' + str(int(jobData.nCoresMod)) + ' ./W' + \
                    str(jobData.jobID) + str(gageID) + '\n'
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
