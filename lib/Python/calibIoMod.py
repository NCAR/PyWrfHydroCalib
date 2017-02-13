# Generate IO library for handling high level IO.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import pandas as pd
from errMod import wipeJobDir
import namelistMod
import subprocess
import pwd
import shutil
import calibMod

class gageMeta:
    def __init__(self):
        # Initialize object to hold metadata about a particular basin.
        # This will be used to populate namelist files, DB tables,
        # etc. These files will be listed in the DB metadata table and
        # should exist on the system.
        self.gage = []
        self.gageID = []
        self.comID = []
        self.geoFile = []
        self.fullDom = []
        self.rtLnk = []
        self.lkFile = []
        self.gwFile = []
        self.udMap = []
        self.wrfInput = []
        self.soilFile = []
        self.forceDir = []
        self.obsFile = []
    def pullGageMeta(self,jobData,db,gageName):
        # Function to extract locations of gage-specific spatial files.
        
        tmpMeta = {'gageName':gageName,'geoFile':'','fullDomFile':'',\
                   'rtLnk':'','lkFile':'','gwFile':'','udMap':'',\
                   'wrfInput':'','soilFile':'','forceDir':'',\
                   'obsFile':'','gageID':'','comID':''}
        try:
            db.queryGageMeta(jobData,tmpMeta)
        except:
            raise
            
        self.gage = tmpMeta['gageName']
        self.gageID = tmpMeta['gageID']
        self.geoFile = tmpMeta['geoFile']
        self.fullDom = tmpMeta['fullDomFile']
        self.rtLnk = tmpMeta['rtLnk']
        self.lkFile = tmpMeta['lkFile']
        self.gwFile = tmpMeta['gwFile']
        self.udMap = tmpMeta['udMap']
        self.wrfInput = tmpMeta['wrfInput']
        self.soilFile = tmpMeta['soilFile']
        self.forceDir = tmpMeta['forceDir']
        self.obsFile = tmpMeta['obsFile']
        self.comID = tmpMeta['comID']
        
def getGageList(jobData,db):
    # Function for extracting list of gages 
    # based on either the CSV file, or an SQL
    # command to extract gages based on a user 
    # specified command.

    if len(jobData.gList) > 0:
        # User provided CSV file with list of gages.
        gListTmp = pd.read_csv(jobData.gList,dtype={0: str})
        
        jobData.gages = gListTmp.Gage[:]

        if len(jobData.gages) == 0:
            errMsg = "ERROR: List of gages for calibration is zero."
            jobData.errMsg = errMsg
            raise Exception()
            
        for tmpGage in range(0,len(gListTmp.Gage)):
            try:
                db.lookupGage(jobData,gListTmp.Gage[tmpGage])
            except:
                raise
    elif len(jobData.gSQL) > 0:
        # User provided SQL command to extract list of gages.
        try:
            gageList = db.queryGageList(jobData)
            jobData.gageIDs = gageList[:][0]
            jobData.gages = gageList[:][1]
        except:
            raise
        
def setupModels(jobData,db,args):
    # Function for setting up all model directories,
    # links to forcings, namelist files, etc. 
    # Function will loop through each basin to calibrate,
    # extract information about geospatial files, nested
    # index values, etc. This information will be used in 
    # the setup. 
    
    # First create top level directory based on the job name.
    parentDir = jobData.outDir + "/" + jobData.jobName
    
    if os.path.isdir(parentDir):
        jobData.errMsg = "ERROR: Top level directory: " + parentDir + " already exists"
        raise Exception()
        
    try:
        os.mkdir(parentDir)
    except:
        wipeJobDir(jobData)
        jobData.errMsg = "ERROR: Failure to create directory: " + parentDir
        raise
        
    # Create gage-specific object that will contain gage-specific information.
    gageData = gageMeta()
    
    # Copy config file to the top level directory. This will be used during
    # restarts to extract information about the job. It was decided to do
    # this opposed to attempting to enter the plethura of information 
    # specific to the job into the metadata table. 
    configPath = str(args.configFile[0])
    copyPath = parentDir + '/setup.config'
    try:
        shutil.copy(configPath,copyPath)
    except:
        wipeJobDir(jobData)
        jobData.errMsg = "ERROR: Failure to copy configuration setup file."
        raise
        
    # Loop through each basin and setup appropriate directories.
    for gage in range(0,len(jobData.gages)):
        gageDir = parentDir + "/" + str(jobData.gages[gage])
        
        try:
            os.mkdir(gageDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + gageDir
            raise
            
        # Create observations directory to hold obs for calibration/eval, etc
        obsDir = gageDir + "/OBS"
        try:
            os.mkdir(obsDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + obsDir
            raise
        
        # Create sub-directories for spinup/calibration runs.
        spinupDir = gageDir + "/RUN.SPINUP"
        try:
            os.mkdir(spinupDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + spinupDir
            raise
            
        outDir = gageDir + "/RUN.SPINUP/OUTPUT"
        try:
            os.mkdir(outDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + outDir
            raise
            
        calibDir = gageDir + "/RUN.CALIB"
        try:
            os.mkdir(calibDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + calibDir
            raise
            
        outDir = gageDir + "/RUN.CALIB/OUTPUT"
        try:
            os.mkdir(outDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + outDir
            raise
            
        validDir = gageDir + "/RUN.VALID"
        try:
            os.mkdir(validDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + validDir
            raise
            
        outDir = gageDir + "/RUN.VALID/OUTPUT"
        try:
            os.mkdir(outDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + outDir
            raise
            
        # Create subdirectory that will hold the original parameter files. These
        # files will be modified by the workflow in-between calibration iterations.
        baseParmDir = gageDir + "/RUN.CALIB/BASELINE_PARAMETERS"
        try:
            os.mkdir(baseParmDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + baseParmDir
            raise
            
        # Copy table user provided with calibration parameters to the calibration directory.
        origPath = str(args.parmTbl[0])
        newPath = gageDir + "/RUN.CALIB/calib_parms.tbl"
        if not os.path.isfile(origPath):
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Input file: " + origPath + " not found."
            raise
        try:
            shutil.copy(origPath,newPath)
        except:
            wipeJobDir
            jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
            raise
            
        # Create sub-directory where fianl calibrated parameters will reside.
        finalParmDir = gageDir + "/RUN.CALIB/FINAL_PARAMETERS"
        try:
            os.mkdir(finalParmDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + finalParmDir
            raise
            
        # Create symbolic links necessary for model runs.
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/wrf_hydro.exe"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/wrf_hydro.exe"
        link3 = gageDir + "/RUN.VALID/OUTPUT/wrf_hydro.exe"
        try:
            os.symlink(str(jobData.exe),link1)
            os.symlink(str(jobData.exe),link2)
            os.symlink(str(jobData.exe),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to WRF-Hydro executable."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/CHANPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/CHANPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CHANPARM.TBL"
        try:
            os.symlink(str(jobData.chanParmTbl),link1)
            os.symlink(str(jobData.chanParmTbl),link2)
            os.symlink(str(jobData.chanParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to channel parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/GENPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/GENPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/GENPARM.TBL"
        try:
            os.symlink(str(jobData.genParmTbl),link1)
            os.symlink(str(jobData.genParmTbl),link2)
            os.symlink(str(jobData.genParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to general parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/HYDRO.TBL"
        #link2 = gageDir + "/RUN.CALIB/OUTPUT/HYDRO.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/HYDRO.TBL"
        try:
            os.symlink(str(jobData.hydroTbl),link1)
            #os.symlink(str(jobData.hydroTbl),link2)
            os.symlink(str(jobData.hydroTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to hydro table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/LAKEPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/LAKEPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/LAKEPARM.TBL"
        try:
            os.symlink(str(jobData.lakeParmTbl),link1)
            os.symlink(str(jobData.lakeParmTbl),link2)
            os.symlink(str(jobData.lakeParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to lake parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/MPTABLE.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/MPTABLE.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/MPTABLE.TBL"
        try:
            os.symlink(str(jobData.mpParmTbl),link1)
            os.symlink(str(jobData.mpParmTbl),link2)
            os.symlink(str(jobData.mpParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to MP parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/SOILPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/SOILPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/SOILPARM.TBL"
        try:
            os.symlink(str(jobData.soilParmTbl),link1)
            os.symlink(str(jobData.soilParmTbl),link2)
            os.symlink(str(jobData.soilParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to soil parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/URBPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/URBPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/URBPARM.TBL"
        try:
            os.symlink(str(jobData.urbParmTbl),link1)
            os.symlink(str(jobData.urbParmTbl),link2)
            os.symlink(str(jobData.urbParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to urban parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/VEGPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/VEGPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/VEGPARM.TBL"
        try:
            os.symlink(str(jobData.vegParmTbl),link1)
            os.symlink(str(jobData.vegParmTbl),link2)
            os.symlink(str(jobData.vegParmTbl),link3)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to vegetation parameter table."
            raise
            
        # Extract gage-specific information (geogrid file, fulldom file, etc)
        # from metadata DB.
        try:
            gageData.pullGageMeta(jobData,db,str(jobData.gages[gage]))
        except:
            wipeJobDir(jobData)
            raise
            
        # Copy original Fulldom, spatial soils, and hydro.tbl file for calibrations.
        origPath = str(gageData.fullDom)
        newPath = baseParmDir + "/Fulldom.nc"
        try:
            shutil.copy(origPath,newPath)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
            raise
            
        origPath = str(gageData.soilFile)
        newPath = baseParmDir + "/soil_properties.nc"
        try:
            shutil.copy(origPath,newPath)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
            raise
            
        origPath = str(jobData.hydroTbl)
        newPath = baseParmDir + "/HYDRO.TBL"
        try:
            shutil.copy(origPath,newPath)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
            raise
            
        origPath = str(gageData.gwFile)
        newPath = baseParmDir + "/GWBUCKPARM.nc"
        try:
            shutil.copy(origPath,newPath)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
            raise
            
        # Create symbolic link to forcing directory.
        fLink = gageDir + "/FORCING"
        try:
            os.symlink(str(gageData.forceDir),fLink)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create FORCING link to: " + str(gageData.forceDir)
            raise
            
        # Create symbolic link to the observations file.
        obsLink = gageDir + "/OBS/obsStrData.Rdata"
        try:
            os.symlink(str(gageData.obsFile),obsLink)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create Observations link to: " + str(gageData.obsFile)
            raise
            
        # Create symbolic link to OBS directory in both the calibration and validation directories.
        obsLink = gageDir + "/RUN.CALIB/OBS"
        obsDir = gageDir + "/OBS"
        try:
            os.symlink(obsDir,obsLink)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create OBS link in RUN.CALIB to: " + obsDir
            raise
        obsLink = gageDir + "/RUN.VALID/OBS"
        try:
            os.symlink(obsDir,obsLink)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create OBS link in RUN.VALID to: " + obsDir
            raise
            
        # Create Rscript file that will be sourced by R for calibration
        #try:
        #    calibMod.generateRScript(jobData,gageData,gage)
        #except:
        #    wipeJobDir(jobData)
        #    jobData.errMsg = "ERROR: Failure to write calibration R script."
        #    raise
            
        # Copy Python and R program necessary to run calibration and parameter 
        # adjustments into the calibration run directory.
        calibPyProgram = os.getcwd() + '/lib/Python/adjust_parameters.py'
        calibRProgram = os.getcwd() + '/lib/R/calib_workflow.R'
        calibRUtils = os.getcwd() + "/lib/R/calib_utils.R"
        try:
            link = gageDir + "/RUN.CALIB/adjust_parameters.py"
            os.symlink(calibPyProgram,link)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to link: " + calibPyProgram
            raise
            
        try:
            link = gageDir + '/RUN.CALIB/calib_workflow.R'
            os.symlink(calibRProgram,link)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to link: " + calibRProgram
            raise

        try:
            link = gageDir + "/RUN.CALIB/calib_utils.R"
            os.symlink(calibRUtils,link)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to link: " + calibRUtils
            raise

        # Copy R program necessary to run validation evaluation.
        #validationRProgram = os.getcwd() + '/lib/R/validation.R'
        #try:
        #    link = gageDir + "/RUN.VALID/validation.R"
        #    os.symlink(validationRProgram,link)
        #except:
        #    wipeJobDir(jobData)
        #    jobData.errMsg = "ERROR: Failure to link: " + validationRProgram
        #    raise