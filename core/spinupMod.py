# Module file for containing functions for executing WRF-Hydro model runs.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

#import datetime
import os
import sys 
from core import calibIoMod
from core import namelistMod
from core import statusMod
from core import errMod
import subprocess
from yaml import SafeDumper 
import yaml
import warnings
warnings.filterwarnings("ignore")
import pickle

def runTroute(statusData,staticData,db,gageID,gage,gageMeta, basinNum):
    
    if statusData.trouteFlag == 0:
        return
    groupNum = statusData.gageGroup[basinNum]
    print("the group num is " + str(groupNum))
    runDir = statusData.jobDir + "/" + gage + "/RUN.SPINUP/OUTPUT/"
    workDir = statusData.jobDir + "/" + gage + "/RUN.SPINUP"
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
        raise Exception()
    if not os.path.isdir(runDir):
        statusData.errMsg = "ERROR: " + runDir + " not found."
        raise Exception()
    
    lockPath = workDir + "/TROUTE.LOCK"
        
    trouteCompleteFlag = runDir + '/trouteFlag.COMPLETE'
    if os.path.exists(trouteCompleteFlag):
        print('Troute processing already complete.\n')
        return
     
    yamlPath = runDir + '/troute_config.yaml' 
    try:
        generateTrouteScript(statusData,runDir,yamlPath, basinNum)
    except:
        raise
 
    yamlFile = open(statusData.trouteConfig)
    yamlDict = yaml.load(yamlFile, Loader=yaml.FullLoader)
    begDate = statusData.bSpinDate
    endDate = statusData.eSpinDate 

    runStatus = statusMod.walkModTroute(begDate,endDate,runDir,yamlDict)
    begDate = runStatus[0]
    endDate = runStatus[1]
    runFlag = runStatus[2] 
     
    if runFlag == False:
        if not os.path.exists(trouteCompleteFlag):
            try:
                open(trouteCompleteFlag, 'a').close()
            except:
                statusData.errMsg = "Unable to create complete flag: " + trouteCompleteFlag
                errMod.errOut(statusData)
    else:
        if os.path.isfile(lockPath):
            print("There is a lock path " + lockPath)
            return

        if begDate == statusData.bSpinDate:
            #It is a cold run
             yamlDict['compute_parameters']['restart_parameters']['start_datetime'] = begDate.strftime('%Y-%m-%d_%H:%M')
        else:
            #It is a restart
             yamlDict['compute_parameters']['restart_parameters']['start_datetime'] = begDate.strftime('%Y-%m-%d_%H:%M')
             yamlDict['compute_parameters']['restart_parameters']['lite_channel_restart_file'] = runDir + '/channel_restart_' + begDate.strftime('%Y%m%d%H%M')

        yamlDict['compute_parameters']['forcing_parameters']['qlat_input_folder'] = runDir
        yamlDict['compute_parameters']['forcing_parameters']['nts'] = (endDate - begDate).days * 24 * 12
        yamlDict['output_parameters']['lite_restart']['lite_restart_output_directory'] = runDir
        yamlDict['output_parameters']['chanobs_output']['chanobs_output_directory'] = runDir
        yamlDict['network_topology_parameters']['supernetwork_parameters']['geo_file_path'] = str(gageMeta.rtLnk)
        if str(gageMeta.lkFile) != '-9999':
            yamlDict['network_topology_parameters']['waterbody_parameters']['level_pool']['level_pool_waterbody_parameter_file_path'] = str(gageMeta.lkFile)
            yamlDict['compute_parameters']['restart_parameters']['wrf_hydro_waterbody_ID_crosswalk_file'] = str(gageMeta.lkFile)
        yamlDict['compute_parameters']['restart_parameters']['wrf_hydro_channel_ID_crosswalk_file'] = str(gageMeta.rtLnk)
        yamlDict['compute_parameters']['restart_parameters']['wrf_hydro_waterbody_crosswalk_filter_file'] = str(gageMeta.rtLnk)
        SafeDumper.add_representer(type(None), lambda dumper, value: dumper.represent_scalar(u'tag:yaml.org,2002:null', ''))

        with open(yamlPath, 'w') as output: 
            yaml.safe_dump(yamlDict, output, default_flow_style=False)
        
        cmd = runDir + "/run_troute.sh 1>" + runDir + "/troute_" + \
              str(statusData.jobID) + "_" + str(gageID) + ".out" + \
              ' 2>' + runDir + "/troute_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        try:
            #os.system(cmd)
            p = subprocess.Popen([cmd], shell = True)
        except Exception as e:
            print("Now it has raised the issue" + str(e))
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gage)
            raise
         
    return
    
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
        gageMeta.pullGageMeta(statusData,db,gage,gageID)
    except:
        raise

    # Create the shell scripts that will use the MPI command specified by the user to run
    # or restart the model.
    runFile = runDir + "/run_WH.sh"
    rstFile = runDir + "/run_WH_Restart.sh"
    if os.path.isfile(runFile):
        os.remove(runFile)
    if os.path.isfile(rstFile):
        os.remove(rstFile)

    try:
        generateMpiScript(statusData,int(gageID),int(basinNum),runDir,gageMeta)
    except:
        raise
    #if statusData.jobRunType == 1:
    #    # If BSUB run script doesn't exist, create it here.
    #    bsubFile = runDir + "/run_WH.sh"
    #    if not os.path.isfile(bsubFile):
    #        try:
    #            generateBsubScript(statusData,int(gageID),runDir,gageMeta)
    #        except:
    #            raise
    #if statusData.jobRunType == 2:
    #    pbsFile = runDir + "/run_WH.sh"
    #    if not os.path.isfile(pbsFile):
    #        try:
    #            generatePbsScript(statusData,int(gageID),runDir,gageMeta)
    #        except:
    #            raise
    #if statusData.jobRunType == 3 or statusData.jobRunType == 6:
    #    slurmFile = runDir + "/run_WH.sh"
    #    if not os.path.isfile(slurmFile):
    #        try:
    #            generateSlurmScript(statusData,int(gageID),runDir,gageMeta)
    #        except:
    #            raise
    #if statusData.jobRunType == 4 or statusData.jobRunType == 5:
    #    # If run script doesn't exist, create it here.
    #    runScript = runDir + "/run_WH.sh"
    #    if not os.path.isfile(runScript):
    #        try:
    #            generateMpiScript(statusData,int(gageID),runDir,gageMeta)
    #        except:
    #            raise

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
    trouteCompleteFlag = runDir + '/trouteFlag.COMPLETE' 
    # If the LOCK file is present, report this and lock things up.
    if os.path.isfile(lockPath):
        keySlot[basinNum] = -1.0
        keyStatus = -1.0
        runFlag = False
        print("MODEL IS LOCKED")

    if keyStatus == 1.0:
        # Model has already completed
        runFlag = False
        return
       
    if keyStatus == 0.95:
        if os.path.isfile(trouteCompleteFlag):
            keySlot[basinNum] = 1.0
            keyStatus = 1.0
            return
        yamlFile = open(statusData.trouteConfig)
        yamlDict = yaml.load(yamlFile, Loader=yaml.FullLoader)        
        runStatus = statusMod.walkModTroute(staticData.bSpinDate,staticData.eSpinDate,runDir,yamlDict)
        begDate = runStatus[0]
        endDate = runStatus[1]
        tRunFlag = runStatus[2]
        if tRunFlag == False:
            if not os.path.exists(trouteCompleteFlag):
                try:
                    open(trouteCompleteFlag, 'a').close()
                except Exception as e:
                    statusData.errMsg = "Unable to create complete flag: " + trouteCompleteFlag + str(e)
                    errMod.errOut(statusData)
            else:
                tLockPath = workDir + "/TROUTE.LOCK"
                if os.path.isfile(tLockPath):
                    return
                else:
                    open(tLockPath,'a').close()
                    statusData.errMsg = "Unable to create complete flag because Troute didn't run successfully. Remove TROUTE.LOCK file: " + tLockPath
                    errMod.errOut(statusData)
            #print("Unable to create complete flag because Troute didn't run successfully. Remove TROUTE.LOCK file: " + lockPath)
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
                print(statusData.genMsg)
                keySlot[basinNum] = -0.25
                keyStatus = -0.25
            else:
                # Model has completed!
                if statusData.trouteFlag == 1:
                    runTroute(statusData,staticData,db,gageID,gage,gageMeta, int(basinNum))
                    keySlot[basinNum] = 0.95
                    keyStatus = 0.95
                    runFlag = False
                else:
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
                if statusData.trouteFlag == 1:
                    runTroute(statusData,staticData,db,gageID,gage,gageMeta, int(basinNum))
                    keySlot[basinNum] = 0.95
                    keyStatus = 0.95
                    runFlag = False
 
                else:
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
            runStatus = statusMod.walkMod(begDate,endDate,runDir, jobData, basinNum)
            begDate = runStatus[0]
            endDate = runStatus[1]
            runFlag = runStatus[2]
            if runFlag:
                keySlot[basinNum] = 0.0
                keyStatus = 0.0
            else:
                # Model sucessfully completed.
                if statusData.trouteFlag == 1:
                    runTroute(statusData,staticData,db,gageID,gage,gageMeta, int(basinNum))
                    keySlot[basinNum] = 0.95
                    keyStatus = 0.95
                    runFlag = False

                else:
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
                if statusData.trouteFlag == 1:
                    runTroute(statusData,staticData,db,gageID,gage,gageMeta, int(basinNum))
                    keySlot[basinNum] = 0.95
                    keyStatus = 0.95

                else:
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
            # Always cold start the model for the beginning......
            startType = 1
        else:
            # Alternative is the model HAS to have a restart file. 
            startType = 2
        
        try:
            namelistMod.createHrldasNL(statusData,gageMeta,staticData,runDir,startType,begDate,endDate,0)
            namelistMod.createHydroNL(statusData,gageMeta,staticData,runDir,startType,begDate,endDate,0)
        except:
            raise
            
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        # Fire off model.
        cmd = runDir + "/run_WH.sh 1>" + runDir + "/WH_" + \
              str(statusData.jobID) + "_" + str(gageID) + ".out" + \
              ' 2>' + runDir + "/WH_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        try:
            p = subprocess.Popen([cmd], shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
            raise
        #if statusData.jobRunType == 1:
        #    cmd = "bsub < " + runDir + "/run_WH.sh"
        #if statusData.jobRunType == 2:
        #    try:
        #        jobTmp = subprocess.check_output(['qsub',runDir + '/run_WH.sh'])
        #        pbsJobId[basinNum] = int(jobTmp.decode("UTF-8").split('.')[0])
        #    except:
        #        statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
        #        raise
        #if statusData.jobRunType == 3 or statusData.jobRunType == 6:
        #    cmd = "sbatch " + runDir + "/run_WH.sh"
        #if statusData.jobRunType == 4 or statusData.jobRunType == 5:
        #    cmd = runDir + "/run_WH.sh 1>" + runDir + "/WH_" + \
        #          str(statusData.jobID) + "_" + str(gageID) + ".out" + \
        #          ' 2>' + runDir + "/WH_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        #try:
        #    if statusData.jobRunType == 1 or statusData.jobRunType == 3 or statusData.jobRunType == 6:
        #        subprocess.call(cmd,shell=True)
        #    if statusData.jobRunType == 4 or statusData.jobRunType == 5:
        #        p = subprocess.Popen([cmd],shell=True)
        #except:
        #    statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
        #    raise
            
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
            # Model always has to start from a cold start.....
            startType = 1
        else:
            # Otherwise, there HAS to be a restart file.....
            startType = 2
        
        try:
            namelistMod.createHrldasNL(statusData,gageMeta,staticData,runDir,startType,begDate,endDate,0)
            namelistMod.createHydroNL(statusData,gageMeta,staticData,runDir,startType,begDate,endDate,0)
        except:
            raise
          
        if startType == 2:
            # Clean run directory of any old diagnostics files
            try:
                errMod.cleanRunDir(statusData,runDir)
            except:
                raise
                
        # Fire off model.
        cmd = runDir + "/run_WH.sh 1>" + runDir + "/WH_" + \
              str(statusData.jobID) + "_" + str(gageID) + ".out" + \
              ' 2>' + runDir + "/WH_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        try:
            p = subprocess.Popen([cmd], shell=True)
        except:
            statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
            raise
        #if statusData.jobRunType == 1:
        #    cmd = "bsub < " + runDir + "/run_WH.sh"
        #if statusData.jobRunType == 2:
        #    try:
        #        jobTmp = subprocess.check_output(['qsub',runDir + '/run_WH.sh'])
        #        pbsJobId[basinNum] = int(jobTmp.decode("UTF-8").split('.')[0])
        #    except:
        #        statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
        #        raise
        #if statusData.jobRunType == 3 or statusData.jobRunType == 6:
        #    cmd = "sbatch " + runDir + "/run_WH.sh"
        #if statusData.jobRunType == 4 or statusData.jobRunType == 5:
        #    cmd = runDir + "/run_WH.sh 1>" + runDir + "/WH_" + \
        #          str(statusData.jobID) + "_" + str(gageID) + ".out" + \
        #          ' 2>' + runDir + "/WH_" + str(statusData.jobID) + "_" + str(gageID) + ".err"
        #try:
        #    if statusData.jobRunType == 1 or statusData.jobRunType == 3 or statusData.jobRunType == 6:
        #        subprocess.call(cmd,shell=True)
        #    if statusData.jobRunType == 4 or statusData.jobRunType == 5:
        #        p = subprocess.Popen([cmd],shell=True)
        #except:
        #    statusData.errMsg = "ERROR: Unable to launch WRF-Hydro job for gage: " + str(gageMeta.gage[basinNum])
        #    raise
            
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
        fileObj.write('mpiexec ./wrf_hydro.exe\n')
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
        inStr = "#SBATCH -n " + str(jobData.nCoresMod) + "\n"
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        if jobData.jobRunType == 3:
            inStr = 'srun ./wrf_hydro.exe\n'
        if jobData.jobRunType == 6:
            inStr = 'mpirun -n ' + str(jobData.nCoresMod) + ' ./wrf_hydro.exe\n'
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

def generateTrouteScript(statusData,runDir,yamlPath, basinNum):
    """
    Generic function to create a run script that will be used to execute the troute model.
    """        
    outFile = runDir + "/run_troute.sh"
    """
    if os.path.isfile(outFile):
        statusData.errMsg = "ERROR: Run script: " + outFile + " already exists."
        raise Exception()
    """
    try:
        fileObj = open(outFile,'w')
        fileObj.write('#!/bin/bash\n')
        for m in statusData.moduleLoadTrouteStr:
            fileObj.write(m)
            fileObj.write("\n")
        inStr = 'cd ' + runDir + '\n'
        fileObj.write(inStr)
        if len(statusData.cpuPinCmd) > 0:
            inStr = "dplace -c " + \
                    str(statusData.gageBegModelCpu[basinNum]) + "-" + \
                    str(statusData.gageEndModelCpu[basinNum]) +  \
                    " python3 -u -m nwm_routing -V3 -f %s" %yamlPath
            print(inStr)
            fileObj.write(inStr)

        else:
          inStr = "python3 -u -m nwm_routing -V3 -f %s" %yamlPath
          fileObj.write(inStr)
    except:
        statusData.errMsg = "ERROR: Failure to create: " + outFile
        raise
    
    # Make the file an executable.
    cmd = "chmod +x " + outFile
    try:
        subprocess.call(cmd,shell=True)
    except:
        statusData.errMsg = "ERROR: Failure to convert: " + outFile + " to an executable."
        raise
 
def generateMpiScript(jobData,gageID,basinNum,runDir,gageMeta):
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
        if len(jobData.cpuPinCmd) > 0:
            inStr = jobData.mpiCmd + " " + str(jobData.nCoresMod) + " " + jobData.cpuPinCmd + \
                    str(jobData.gageBegModelCpu[basinNum]) + "-" + \
                    str(jobData.gageEndModelCpu[basinNum]) + " ./W" + \
                    str(jobData.jobID) + str(gageID) + '\n'
        else:
            inStr = jobData.mpiCmd + " " + str(jobData.nCoresMod) + " ./W" + \
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
