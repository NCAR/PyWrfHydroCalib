# Generate IO library for handling high level IO.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import os
import pandas as pd
from core import errMod
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
        self.hydroSpatial = []
        self.forceDir = []
        self.obsFile = []
        self.dxHydro = []
        self.aggFact = []
        self.optLandRstFile = []
        self.optHydroRstFile = []
        self.chanParmFile = []
    def pullGageMeta(self,jobData,db,gageName,domainID):
        # Function to extract locations of gage-specific spatial files.
        
        tmpMeta = {'gageName':gageName,'geoFile':'','landSpatialMeta':'','fullDomFile':'',\
                   'rtLnk':'','lkFile':'','gwFile':'','udMap':'',\
                   'wrfInput':'','soilFile':'','hydroSpatial':'','forceDir':'',\
                   'obsFile':'','gageID':'','comID':'','nCoresMod':'','dxHydro':'',\
                   'aggFactor':'','domainID':domainID,'optLandRstFile':'',\
                   'optHydroRstFile':'','chanParmFile':''}
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
        self.hydroSpatial = tmpMeta['hydroSpatial']
        self.forceDir = tmpMeta['forceDir']
        self.obsFile = tmpMeta['obsFile']
        self.comID = tmpMeta['comID']
        self.dxHydro = tmpMeta['dxHydro']
        self.aggFact = tmpMeta['aggFactor']
        self.optLandRstFile = tmpMeta['optLandRstFile']
        self.optHydroRstFile = tmpMeta['optHydroRstFile']
        self.chanParmFile = tmpMeta['chanParmFile']
        
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
            
def copyDefaultParms(jobData,runDir,gage,staticData):
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
    
    if staticData.gwBaseFlag == 1:
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
    
    inPath = runDir + "/HYDRO_TBL_2D.nc"
    outPath = str(jobData.jobDir) + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS/HYDRO_TBL_2D.nc"
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
        
    if staticData.chnRtOpt == 3:
        # Copy the CHANPARM file.
        inPath = runDir + "/CHANPARM.TBL"
        outPath = str(jobData.jobDir) + "/" + gage + "/RUN.CALIB/DEFAULT_PARAMETERS/CHANPARM.TBL"
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
        errMod.wipeJobDir(jobData,db)
        jobData.errMsg = "ERROR: Failure to copy configuration setup file."
        raise
        
    # Loop through each basin and setup appropriate directories.
    for gage in range(0,len(jobData.gages)):
        gageDir = parentDir + "/" + str(jobData.gages[gage])
        
        try:
            os.mkdir(gageDir)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Failure to create directory: " + gageDir
            raise
            
        # Create observations directory to hold obs for calibration/eval, etc
        obsDir = gageDir + "/OBS"
        try:
            os.mkdir(obsDir)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Failure to create directory: " + obsDir
            raise
        
        # Create sub-directories for spinup/calibration runs.
        spinupDir = gageDir + "/RUN.SPINUP"
        try:
            os.mkdir(spinupDir)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Failure to create directory: " + spinupDir
            raise
            
        outDir = gageDir + "/RUN.SPINUP/OUTPUT"
        try:
            os.mkdir(outDir)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Failure to create directory: " + outDir
            raise
            
        if jobData.calibFlag == 1:
            calibDir = gageDir + "/RUN.CALIB"
            try:
                os.mkdir(calibDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + calibDir
                raise
            
            outDir = gageDir + "/RUN.CALIB/OUTPUT"
            try:
                os.mkdir(outDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + outDir
                raise
            
            validDir = gageDir + "/RUN.VALID"
            try:
                os.mkdir(validDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + validDir
                raise
            
            outDir = gageDir + "/RUN.VALID/OUTPUT"
            try:
                os.mkdir(outDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + outDir
                raise
            
            outDir = gageDir + "/RUN.VALID/OUTPUT/CTRL"
            try:
                os.mkdir(outDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + outDir
                raise
            
            outDir = gageDir + "/RUN.VALID/OUTPUT/BEST"
            try:
                os.mkdir(outDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + outDir
                raise
            
        # Create the top level directories for the sensitivity analysis. 
        if jobData.sensFlag == 1:
            sensDir = gageDir + "/RUN.SENSITIVITY"
            try:
                os.mkdir(sensDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + sensDir
                raise
            for i in range(0,jobData.nSensIter):
                tmpDir = sensDir + "/OUTPUT_" + str(i)
                try:
                    os.mkdir(tmpDir)
                except:
                    jobData.errMsg = "ERROR: Failure to create directory: " + tmpDir
                    raise
            
        if jobData.calibFlag == 1:
            # Create subdirectory that will hold the original parameter files. These
            # files will be modified by the workflow in-between calibration iterations.
            baseParmDir = gageDir + "/RUN.CALIB/BASELINE_PARAMETERS"
            try:
                os.mkdir(baseParmDir)
            except:
                errMod.wipeJobDir(jobData,db)
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
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + defaultParmDir
                raise
            
            # Copy table user provided with calibration parameters to the calibration directory.
            origPath = str(jobData.calibTbl)
            newPath = gageDir + "/RUN.CALIB/calib_parms.tbl"
            if not os.path.isfile(origPath):
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Input file: " + origPath + " not found."
                raise Exception()
            try:
                shutil.copy(origPath,newPath)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
                raise
            
        # Copy parameter table with parameters to sensitivity directory.
        if jobData.sensFlag == 1:
            origPath = str(jobData.sensTbl)
            newPath = gageDir + "/RUN.SENSITIVITY/sens_params.tbl"
            if not os.path.isfile(origPath):
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Input file: " + origPath + " not found."
                raise Exception()
            try:
                shutil.copy(origPath,newPath)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
                
        if jobData.calibFlag == 1:
            # Create sub-directory where fianl calibrated parameters will reside.
            finalParmDir = gageDir + "/RUN.CALIB/FINAL_PARAMETERS"
            try:
                os.mkdir(finalParmDir)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create directory: " + finalParmDir
                raise

        # Create symbolic links necessary for model runs.
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/W" + str(jobData.jobID) + str(jobData.gageIDs[gage])
        link2 = gageDir + "/RUN.CALIB/OUTPUT/W" + str(jobData.jobID) + str(jobData.gageIDs[gage]) 
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/WC" + str(jobData.jobID) + str(jobData.gageIDs[gage])
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/WB" + str(jobData.jobID) + str(jobData.gageIDs[gage])
        try:
            os.symlink(str(jobData.exe),link1)
            if jobData.calibFlag == 1:
                os.symlink(str(jobData.exe),link2)
                os.symlink(str(jobData.exe),link3)
                os.symlink(str(jobData.exe),link4)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Unable to create symbolic link to WRF-Hydro executable."
            raise
            
        if jobData.sensFlag == 1:
            for i in range(0,jobData.nSensIter):
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/WHS" + \
                        str(jobData.jobID) + str(jobData.gageIDs[gage]) + str(i)
                link2 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/wrf_hydro.exe"
                try:
                    os.symlink(str(jobData.exe),link1)
                except:
                    jobData.errMsg = "ERROR: Unable to link WRF-Hydro executable to: " + link1
                    raise
                try:
                    os.symlink(str(jobData.exe),link2)
                except:
                    jobData.errMsg = "ERROR: Unable to link WRF-Hydro executable to: " + link2
                    raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/wrf_hydro.exe"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/wrf_hydro.exe"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/wrf_hydro.exe"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/wrf_hydro.exe"
        try:
            os.symlink(str(jobData.exe),link1)
            if jobData.calibFlag == 1:
                os.symlink(str(jobData.exe),link2)
                os.symlink(str(jobData.exe),link3)
                os.symlink(str(jobData.exe),link4)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Unable to create symbolic link to WRF-Hydro executable."
            raise
            
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/GENPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/GENPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/GENPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/GENPARM.TBL"
        try:
            os.symlink(str(jobData.genParmTbl),link1)
            if jobData.calibFlag == 1:
                os.symlink(str(jobData.genParmTbl),link2)
                os.symlink(str(jobData.genParmTbl),link3)
                os.symlink(str(jobData.genParmTbl),link4)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Unable to create symbolic link to general parameter table."
            raise
            
        if jobData.sensFlag == 1:
            for i in range(0,jobData.nSensIter):
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/GENPARM.TBL"
                try:
                    os.symlink(str(jobData.genParmTbl),link1)
                except:
                    jobData.errMsg = "ERROR: Unable to create symbolic link to: " + link1
                    raise
                    
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/MPTABLE.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/MPTABLE.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/MPTABLE.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/MPTABLE.TBL"
        try:
            os.symlink(str(jobData.mpParmTbl),link1)
            if jobData.calibFlag == 1:
                os.symlink(str(jobData.mpParmTbl),link2)
                os.symlink(str(jobData.mpParmTbl),link3)
                os.symlink(str(jobData.mpParmTbl),link4)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Unable to create symbolic link to MP parameter table."
            raise
            
        if jobData.sensFlag == 1:
            for i in range(0,jobData.nSensIter):
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/MPTABLE.TBL"
                try:
                    os.symlink(str(jobData.mpParmTbl),link1)
                except:
                    jobData.errMsg = "ERROR: Unable to create symbolic link to: " + link1
                    raise
                    
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/SOILPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/SOILPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/SOILPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/SOILPARM.TBL"
        try:
            os.symlink(str(jobData.soilParmTbl),link1)
            if jobData.calibFlag == 1:
                os.symlink(str(jobData.soilParmTbl),link2)
                os.symlink(str(jobData.soilParmTbl),link3)
                os.symlink(str(jobData.soilParmTbl),link4)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Unable to create symbolic link to soil parameter table."
            raise
            
        if jobData.sensFlag == 1:
            for i in range(0,jobData.nSensIter):
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/SOILPARM.TBL"
                try:
                    os.symlink(str(jobData.soilParmTbl),link1)
                except:
                    jobData.errMsg = "ERROR: Unable to create symbolic link to: " + link1
                    raise
                    
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/URBPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/URBPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/URBPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/URBPARM.TBL"
        try:
            os.symlink(str(jobData.urbParmTbl),link1)
            if jobData.calibFlag == 1:
                os.symlink(str(jobData.urbParmTbl),link2)
                os.symlink(str(jobData.urbParmTbl),link3)
                os.symlink(str(jobData.urbParmTbl),link4)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Unable to create symbolic link to urban parameter table."
            raise
            
        if jobData.sensFlag == 1:
            for i in range(0,jobData.nSensIter):
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/URBPARM.TBL"
                try:
                    os.symlink(str(jobData.urbParmTbl),link1)
                except:
                    jobData.errMsg = "ERROR: Unable to create symbolic link to: " + link1
                    raise
                    
        link1 = gageDir + "/RUN.SPINUP/OUTPUT/VEGPARM.TBL"
        link2 = gageDir + "/RUN.CALIB/OUTPUT/VEGPARM.TBL"
        link3 = gageDir + "/RUN.VALID/OUTPUT/CTRL/VEGPARM.TBL"
        link4 = gageDir + "/RUN.VALID/OUTPUT/BEST/VEGPARM.TBL"
        try:
            os.symlink(str(jobData.vegParmTbl),link1)
            if jobData.calibFlag == 1:
                os.symlink(str(jobData.vegParmTbl),link2)
                os.symlink(str(jobData.vegParmTbl),link3)
                os.symlink(str(jobData.vegParmTbl),link4)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Unable to create symbolic link to vegetation parameter table."
            raise
            
        if jobData.sensFlag == 1:
            for i in range(0,jobData.nSensIter):
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/VEGPARM.TBL"
                try:
                    os.symlink(str(jobData.vegParmTbl),link1)
                except:
                    jobData.errMsg = "ERROR: Unable to create symbolic link to: " + link1
                    raise
        
        # Extract gage-specific information (geogrid file, fulldom file, etc)
        # from metadata DB.
        try:
            gageData.pullGageMeta(jobData,db,str(jobData.gages[gage]),jobData.gageIDs[gage])
        except:
            errMod.wipeJobDir(jobData,db)
            raise
            
        # Make a copy of the CHANPARM table file (if gridded routing) for spinup
        # purposes.
        if gageData.chanParmFile != "-9999":
            link = gageDir + "/RUN.SPINUP/OUTPUT/CHANPARM.TBL"
            try:
                os.symlink(str(gageData.chanParmFile),link)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Unable to create CHANPARM symlink for spinup for gage: " + \
                                 str(jobData.gages[gage])
                raise
                
        if jobData.calibFlag == 1:
            # Copy original Fulldom, spatial soils, and HYDRO_TBL_2D file for calibrations.
            origPath = str(gageData.fullDom)
            newPath = baseParmDir + "/Fulldom.nc"
            try:
                shutil.copy(origPath,newPath)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
                raise
            
            origPath = str(gageData.soilFile)
            newPath = baseParmDir + "/soil_properties.nc"
            try:
                shutil.copy(origPath,newPath)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
                raise
            
            origPath = str(gageData.hydroSpatial)
            newPath = baseParmDir + "/HYDRO_TBL_2D.nc"
            try:
                shutil.copy(origPath,newPath)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to : " + newPath
                raise
                
            origPath = str(gageData.chanParmFile)
            newPath = baseParmDir + "/CHANPARM.TBL"
            if str(gageData.chanParmFile) != "-9999":
                try:
                    shutil.copy(origPath,newPath)
                except:
                    errMod.wipeJobDir(jobData,db)
                    jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
                    raise
            
            if jobData.gwBaseFlag == 1:
                origPath = str(gageData.gwFile)
                newPath = baseParmDir + "/GWBUCKPARM.nc"
                try:
                    shutil.copy(origPath,newPath)
                except:
                    errMod.wipeJobDir(jobData,db)
                    jobData.errMsg = "ERROR: Failure to copy: " + origPath + " to: " + newPath
                    raise
            
        # Create symbolic link to forcing directory.
        fLink = gageDir + "/FORCING"
        try:
            os.symlink(str(gageData.forceDir),fLink)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Failure to create FORCING link to: " + str(gageData.forceDir)
            raise
            
        # Create symbolic link to the observations file.
        obsLink = gageDir + "/OBS/obsStrData.Rdata"
        try:
            os.symlink(str(gageData.obsFile),obsLink)
        except:
            errMod.wipeJobDir(jobData,db)
            jobData.errMsg = "ERROR: Failure to create Observations link to: " + str(gageData.obsFile)
            raise
            
        if jobData.calibFlag == 1:
            # Create symbolic link to OBS directory in both the calibration and validation directories.
            obsLink = gageDir + "/RUN.CALIB/OBS"
            obsDir = gageDir + "/OBS"
            try:
                os.symlink(obsDir,obsLink)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create OBS link in RUN.CALIB to: " + obsDir
                raise
            obsLink = gageDir + "/RUN.VALID/OBS"
            try:
                os.symlink(obsDir,obsLink)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create OBS link in RUN.VALID to: " + obsDir
                raise
        if jobData.sensFlag == 1:
            obsLink = gageDir + "/RUN.SENSITIVITY/OBS"
            obsDir = gageDir + "/OBS"
            try:
                os.symlink(obsDir,obsLink)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to create OBS link in RUN.SENSITIVITY to: " + obsDir
                raise
            
        # Copy Python and R program necessary to run calibration and parameter 
        # adjustments into the calibration run directory.
        calibPyProgram = libPathTop + '/adjust_parameters.py'
        sensPyProgram = libPathTop + '/adjust_parameters_sensitivity.py'
        calibRProgram = libPathTop + '/calib_workflow.R'
        calibRUtils = libPathTop + '/calib_utils.R'
        if jobData.calibFlag == 1:
            try:
                link = gageDir + "/RUN.CALIB/adjust_parameters.py"
                os.symlink(calibPyProgram,link)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to link: " + calibPyProgram
                raise
                
            try:
                link = gageDir + '/RUN.CALIB/calib_workflow.R'
                os.symlink(calibRProgram,link)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to link: " + calibRProgram
                raise

            try:
                link = gageDir + "/RUN.CALIB/calib_utils.R"
                os.symlink(calibRUtils,link)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to link: " + calibRUtils
                raise
                
        # Copy Python and R programs necessary to run parameter generation and 
        # sensitivity analysis. 
        sensPreRProgram = libPathTop + "/sens_workflow_pre.R"
        sensCollectRProgram = libPathTop + "/Collect_simulated_flow.R"
        sensPostRProgram = libPathTop + "/sens_workflow_post.R"
        if jobData.sensFlag == 1:
            try:
                link = gageDir + "/RUN.SENSITIVITY/sens_workflow_pre.R"
                os.symlink(sensPreRProgram,link)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to link: " + sensPreRProgram
                raise
            try:
                link = gageDir + "/RUN.SENSITIVITY/adjust_parameters_sensitivity.py"
                os.symlink(sensPyProgram,link)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to link: " + sensPyProgram
                raise
            try:
                link = gageDir + "/RUN.SENSITIVITY/calib_utils.R"
                os.symlink(calibRUtils,link)
            except:
                errMod.wipeJobDir(jobData,db)
                jobData.errMsg = "ERROR: Failure to link: " + calibRUtils
                raise
            for i in range(0,jobData.nSensIter):
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/Collect_simulated_flow.R"
                try:
                    os.symlink(sensCollectRProgram,link1)
                except:
                    jobData.errMsg = "ERROR: Unable to create symbolic link to: " + link1
                    raise
                link1 = gageDir + "/RUN.SENSITIVITY/OUTPUT_" + str(i) + "/calib_utils.R"
                try:
                    os.symlink(calibRUtils,link1)
                except:
                    jobData.errMsg = "ERROR: Unable to create symbolic link to: " + link1
                    raise
            try:
                link = gageDir + "/RUN.SENSITIVITY/sens_workflow_post.R"
                os.symlink(sensPostRProgram,link)
            except:
                jobData.errMsg = "ERROR: Failure to link: " + sensPostRProgram
                raise

def generateCalibGroupScript(jobData,groupNum,scriptPath):
    """
    Function to generate the run script for a particular group of basins.
    :param jobData:
    :param groupNum:
    :return:
    """
    if jobData.jobRunType == 2:
        # We are running PBS
        fileObj = open(scriptPath,'w')
        fileObj.write('#!/bin/bash\n')
        fileObj.write('#\n')
        fileObj.write('# PBS Batch Script to Run WRF-Hydro Group Calibrations\n')
        fileObj.write('#\n')
        inStr = '#PBS -N WH_CALIB_GROUP_' + str(jobData.jobID) + '_' + str(groupNum) + '\n'
        fileObj.write(inStr)
        if len(jobData.acctKey.strip()) > 0:
            inStr = "#PBS -A " + str(jobData.acctKey) + '\n'
            fileObj.write(inStr)
        fileObj.write('#PBS -l walltime=12:00:00\n')
        if len(jobData.queName.strip()) > 0:
            inStr = "#PBS -q " + str(jobData.queName) + "\n"
            fileObj.write(inStr)
            
