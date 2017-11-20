# Generate IO library for handling high level IO.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import pandas as pd
from errMod import wipeJobDir
import shutil

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
        self.landSpatialMeta = []
        self.fullDom = []
        self.rtLnk = []
        self.lkFile = []
        self.gwFile = []
        self.gwMask = []
        self.udMap = []
        self.wrfInput = []
        self.soilFile = []
        self.forceDir = []
        self.obsFile = []
    def pullGageMeta(self,jobData,db,gageName):
        # Function to extract locations of gage-specific spatial files.
        
        tmpMeta = {'gageName':gageName,'geoFile':'','landSpatialMeta':'','fullDomFile':'',\
                   'rtLnk':'','lkFile':'','gwFile':'','udMap':'',\
                   'wrfInput':'','soilFile':'','forceDir':'',\
                   'obsFile':'','gageID':'','comID':'','nCoresMod':''}
        try:
            db.queryGageMeta(jobData,tmpMeta)
        except:
            raise
            
        self.gage = tmpMeta['gageName']
        self.gageID = tmpMeta['gageID']
        self.geoFile = tmpMeta['geoFile']
        self.landSpatialMeta = tmpMeta['landSpatialMeta']
        self.fullDom = tmpMeta['fullDomFile']
        self.rtLnk = tmpMeta['rtLnk']
        self.lkFile = tmpMeta['lkFile']
        self.gwFile = tmpMeta['gwFile']
        self.gwMask = tmpMeta['gwMask']
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
            
def copyDefaultParms(jobData,runDir,gage):
    """
    Generic function to copy the first set of default parameters
    (per user input in the table) to a DEFAULT_PARMS directory.
    These files will be used by the validation control simulation.
    """
    inPath = runDir + "/Fulldom.nc"
    outPath = str(jobData.jobDir) + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS/Fulldom.nc"
    if not os.path.isfile(inPath):
        jobData.errMsg = "ERROR: Expected to find: " + inPath + " but was not found."
        raise Exception()
    try:
        shutil.copy(inPath,outPath)
    except:
        jobData.errMsg = "ERROR: Failure to copy: " + inPath + " to: " + outPath
        raise
    
    inPath = runDir + "/GWBUCKPARM.nc"
    outPath = str(jobData.jobDir) + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS/GWBUCKPARM.nc"
    if not os.path.isfile(inPath):
        jobData.errMsg = "ERROR: Expected to find: " + inPath + " but was not found."
        raise Exception()
    try:
        shutil.copy(inPath,outPath)
    except:
        jobData.errMsg = "ERROR: Failure to copy: " + inPath + " to: " + outPath
        raise
    
    inPath = runDir + "/HYDRO.TBL"
    outPath = str(jobData.jobDir) + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS/HYDRO.TBL"
    if not os.path.isfile(inPath):
        jobData.errMsg = "ERROR: Expected to find: " + inPath + " but was not found."
        raise Exception()
    try:
        shutil.copy(inPath,outPath)
    except:
        jobData.errMsg = "ERROR: Failure to copy: " + inPath + " to: " + outPath
        raise
    
    inPath = runDir + "/soil_properties.nc"
    outPath = str(jobData.jobDir) + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS/soil_properties.nc"
    if not os.path.isfile(inPath):
        jobData.errMsg = "ERROR: Expected to find: " + inPath + " but was not found."
        raise Exception()
    try:
        shutil.copy(inPath,outPath)
    except:
        jobData.errMsg = "ERROR: Failure to copy: " + inPath + " to: " + outPath
        raise
    
        
def setupModels(jobData,db,args,libPathTop):
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
            
        outDir = gageDir + "/RUN.VALID/OUTPUT/CTRL"
        try:
            os.mkdir(outDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + outDir
            raise
            
        outDir = gageDir + "/RUN.VALID/OUTPUT/BEST"
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
            
        # Create subdirectory that will hold the initial parameter files. These
        # are generated by the first set of parameter estimation programs that 
        # will use the values specified in the table by the user. They will be
        # used for the control run of the validation.
        defaultParmDir = gageDir + "/RUN.CALIB/DEFAULT_PARAMETERS"
        try:
            os.mkdir(defaultParmDir)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Failure to create directory: " + defaultParmDir
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
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/W" + str(jobData.jobID) + str(jobData.gageIDs[gage]) 
        link2 = gageDir + "/RUN.CALIB/OUTPUT/W" + str(jobData.jobID) + str(jobData.gageIDs[gage]) 
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/WC" + str(jobData.jobID) + str(jobData.gageIDs[gage])
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/WB" + str(jobData.jobID) + str(jobData.gageIDs[gage])
        try:
            os.symlink(str(jobData.exe),link1)
            os.symlink(str(jobData.exe),link2)
            os.symlink(str(jobData.exe),link3)
            os.symlink(str(jobData.exe),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to WRF-Hydro executable."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/wrf_hydro.exe"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/wrf_hydro.exe"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/wrf_hydro.exe"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/wrf_hydro.exe"
        try:
            os.symlink(str(jobData.exe),link1)
            os.symlink(str(jobData.exe),link2)
            os.symlink(str(jobData.exe),link3)
            os.symlink(str(jobData.exe),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to WRF-Hydro executable."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/CHANPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/CHANPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/CHANPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/CHANPARM.TBL"
        try:
            os.symlink(str(jobData.chanParmTbl),link1)
            os.symlink(str(jobData.chanParmTbl),link2)
            os.symlink(str(jobData.chanParmTbl),link3)
            os.symlink(str(jobData.chanParmTbl),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to channel parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/GENPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/GENPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/GENPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/GENPARM.TBL"
        try:
            os.symlink(str(jobData.genParmTbl),link1)
            os.symlink(str(jobData.genParmTbl),link2)
            os.symlink(str(jobData.genParmTbl),link3)
            os.symlink(str(jobData.genParmTbl),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to general parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/HYDRO.TBL"
        try:
            os.symlink(str(jobData.hydroTbl),link1)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to hydro table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/LAKEPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/LAKEPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/LAKEPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/LAKEPARM.TBL"
        try:
            os.symlink(str(jobData.lakeParmTbl),link1)
            os.symlink(str(jobData.lakeParmTbl),link2)
            os.symlink(str(jobData.lakeParmTbl),link3)
            os.symlink(str(jobData.lakeParmTbl),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to lake parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/MPTABLE.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/MPTABLE.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/MPTABLE.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/MPTABLE.TBL"
        try:
            os.symlink(str(jobData.mpParmTbl),link1)
            os.symlink(str(jobData.mpParmTbl),link2)
            os.symlink(str(jobData.mpParmTbl),link3)
            os.symlink(str(jobData.mpParmTbl),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to MP parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/SOILPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/SOILPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/SOILPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/SOILPARM.TBL"
        try:
            os.symlink(str(jobData.soilParmTbl),link1)
            os.symlink(str(jobData.soilParmTbl),link2)
            os.symlink(str(jobData.soilParmTbl),link3)
            os.symlink(str(jobData.soilParmTbl),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to soil parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/URBPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/URBPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/URBPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/URBPARM.TBL"
        try:
            os.symlink(str(jobData.urbParmTbl),link1)
            os.symlink(str(jobData.urbParmTbl),link2)
            os.symlink(str(jobData.urbParmTbl),link3)
            os.symlink(str(jobData.urbParmTbl),link4)
        except:
            wipeJobDir(jobData)
            jobData.errMsg = "ERROR: Unable to create symbolic link to urban parameter table."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/VEGPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/VEGPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/VEGPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/VEGPARM.TBL"
        try:
            os.symlink(str(jobData.vegParmTbl),link1)
            os.symlink(str(jobData.vegParmTbl),link2)
            os.symlink(str(jobData.vegParmTbl),link3)
            os.symlink(str(jobData.vegParmTbl),link4)
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
            
        # Copy Python and R program necessary to run calibration and parameter 
        # adjustments into the calibration run directory.
        calibPyProgram = libPathTop + '/Python/adjust_parameters.py'
        calibRProgram = libPathTop + '/R/calib_workflow.R'
        calibRUtils = libPathTop + '/R/calib_utils.R'
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
