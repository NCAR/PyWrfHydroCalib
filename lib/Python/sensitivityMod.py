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

def preProc(preProcStatus,statusData,staticData,db,gageID,gage):
    """
    Function to run R/Python pre-processing for all the potential permutations
    of the sensivity analysi. The first step is to run R code to generate a 
    table of parameter values. Followed up with that, Python code will be 
    ran to create parameter NetCDF files that will be used by the model 
    simulations. 
    """
    # Establish some constant paths
    workDir = statusData.jobDir + "/" + gage + "/RUN.SENSITIVITY"
    if not os.path.isdir(workDir):
        statusData.errMsg = "ERROR: " + workDir + " not found."
        
    # Pull gage metadata for this particular basin.
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
    except:
        raise
    
    # First step is to create a R namelist file the pre-processing R code will
    # expect
    rNameList = workDir + "/namelist.sensitivity"
    if not os.path.isfile(rNameList):
        try:
            genRNameList(staticData,workDir,gageMeta,gage)
        except:
            statusData.errMsg = "ERROR: Unable to generate sensitivity pre-processing R namelist"
            raise
            
    # Generate run script to generate parameters for this basin. 
    if statusData.analysisRunType == 1:
        #BSUB
    if statusData.analysisRunType == 2:
        #PBS
    if statusData.analysisRunType == 3:
        #SLURM
    if statusData.analysisRunType == 4 or statusData.analysisRunType == 5:
        #MPIEXEC/MPIRUN
    
            
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
    # First check to make sure previous iteration's status is 1.0 (unless iteration 0).
    # This is to prevent the program from doing unecessary work. 
    if iteration > 0:
        if keySlot[basinNum,iteration-1] < 1.0:
            return
        
    # Pull gage metadata for this particular basin.
    gageMeta = calibIoMod.gageMeta()
    try:
        gageMeta.pullGageMeta(staticData,db,gage)
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
        inStr = "sideId <- \"" + str(gageMeta.gage) + "\"\n"
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
        
def generateBsubPreProcScript(jobData,gageID,runDir,workDir,domainDir):
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
            inStr = "#BSUB -n 1\n'
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
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + workDir + ' ' + runDir + ' \n')
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
        
def generatePbsPreProcScript(jobData,gageID,runDir,workDir,domainDir):
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
            inStr = '#PBS -o ' + workDir + '/WH_CALIB_' + str(jobData.jobID) + '_' + str(gageID) + '.out\n'
            fileObj.write(inStr)
            inStr = '#PBS -e ' + workDir + '/WH_CALIB_' + str(jobData.jobID) + '_' + str(gageID) + '.err\n'
            fileObj.write(inStr)
            nCoresPerNode = int(jobData.nCoresR/jobData.nNodesR)
            inStr = "#PBS -l select=1:ncpus=1:mpiprocs=1\n"
            fileObj.write(inStr)
            fileObj.write('#PBS -l walltime=01:00:00\n')
            if len(jobData.queNameAnalysis.strip()) > 0:
                inStr = '#PBS -q ' + str(jobData.queNameAnalysis) + '\n'
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
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + workDir + ' ' + runDir + ' \n')
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
        
def generateSlurmPreProcScript(jobData,gageID,runDir,workDir,domainDir):
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
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + workDir + ' ' + runDir + ' \n')
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
        
def generateMpiPreProcScript(jobData,gageID,runDir,workDir,domainDir):
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
                inStr = 'mpiexec -n 1 ./C' + \
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
            fileObj.write('python ' + workDir + '/adjust_parameters_sensitivity.py ' + workDir + ' ' + runDir + ' \n')
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