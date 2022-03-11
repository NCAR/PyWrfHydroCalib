# File to read in information from the config file and store it in a Python
# class object. This is called one time for each job that is initialized. 
# Options are checked to ensure no improper entries were put into the
# config file.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

from configparser import SafeConfigParser
import os
import datetime
import ast
import pwd
import pandas as pd
import numpy as np
import math
#from slacker import Slacker

import warnings
warnings.filterwarnings("ignore")

class jobMeta:
    def __init__(self):
        # Initialize empty Python object. 
        self.jobName = []
        self.jobID = []
        self.acctKey = []
        self.queName = []
        self.nCoresMod = []
        self.nNodesMod = []
        self.nCoresPerNode = []
        self.sensFlag = []
        self.sensTbl = []
        self.calibFlag = []
        self.trouteFlag = []
        self.trouteConfig = []
        self.moduleLoadStr = []
        self.moduleLoadTrouteStr = []
        self.calibTbl = []
        self.dailyAnalysis = []
        self.coldStart = []
        self.optSpinFlag = []
        self.optCalStripFlag = []
        self.optCalStripHrs = []
        self.jobRunType = []
        self.mpiCmd = []
        self.cpuPinCmd = []
        self.nIter = []
        self.calibMethod = []
        self.enableStreamflowCalib = []
        self.enableSnowCalib = []
        self.enableSoilMoistureCalib = []
        self.streamflowObjFunc = []
        self.snowObjFunc = []
        self.soilMoistureObjFunc = []
        self.streamflowWeight=[]
        self.snowWeight = []
        self.soilMoistureWeight = []
        self.basinType = []  # Xia 20210610
        self.weight1event = []
        self.weight2event = []
        self.ddsR = []
        self.outDir = []
        self.email = None
        self.slChan = None
        self.slToken = None
        self.slUser = None
        self.slackObj = None
        self.owner = []
        self.errMsg = []
        self.genMsg = []
        self.exe = []
        self.genParmTbl = []
        self.mpParmTbl = []
        self.urbParmTbl = []
        self.vegParmTbl = []
        self.soilParmTbl = []
        self.bSpinDate = []
        self.eSpinDate = []
        self.bCalibDate = []
        self.eCalibDate = []
        self.bCalibEvalDate = []
        self.bCalibFullOutputs = []
        self.bValidDate = []
        self.eValidDate = []
        self.bValidEvalDate = []
        self.nSensSample = []
        self.nSensIter = []
        self.nSensBatch = []
        self.bSensDate = []
        self.eSensDate = []
        self.bSensEvalDate = []
        self.gSQL = []
        self.gList = []
        self.dynVegOpt = []
        self.canStomOpt = []
        self.btrOpt = []
        self.runOffOpt = []
        self.sfcDragOpt = []
        self.frzSoilOpt = []
        self.supCoolOpt = []
        self.radTOpt = []
        self.snAlbOpt = []
        self.pcpPartOpt = []
        self.tbotOpt = []
        self.timeSchmOpt = []
        self.sfcResOpt = []
        self.glacier = []
        self.IMPERV_OPTION = []
        self.soilThick = []
        self.zLvl = []
        self.fType = []
        self.fDT = []
        self.lsmDt = []
        self.lsmOutDt = []
        self.lsmRstFreq = []
        self.hydroRstFreq = []
        self.hydroOutDt = []
        self.rstType = []
        self.ioConfigOutputs = []
        self.ioFormOutputs = []
        self.chrtoutDomain = []
        self.chanObs = []
        self.chrtoutGrid = []
        self.lsmDomain = []
        self.rtoutDomain = []
        self.gwOut = []
        self.lakeOut = []
        self.frxstPts = []
        self.resetHydro = []
        self.strOrder = []
        self.dtChRt = []
        self.dtTerRt = []
        self.subRtFlag = []
        self.ovrRtFlag = []
        self.chnRtFlag = []
        self.crocusFlag = []
        self.crocusOpt = []
        self.actLev = []
        self.chnRtOpt = []
        self.rtOpt = []
        self.imperv_adj = []
        self.udmpOpt = []
        self.gwBaseFlag = []
        self.gwRst = []
        self.enableCmpdChan = []
        self.cmpdChan = []
        self.enableGwLoss = []
        self.gwLoss = []
        self.gages = []
        self.gageIDs = []
        self.dbPath = []
        self.dbBackup = []
        self.nGroups = None
        self.numBasPerGroup = None
        self.gageGroup = []
        self.gageBegModelCpu = []
        self.gageEndModelCpu = []
        self.groupComplete = []
        self.lsmSplitOutputCount = []
        self.SplitOutputCount = []
        self.enableMask = []
        #self.maskFile = []
        self.enableMultiSites = []


    def checkGages2(self,db):
        #Function to extract domain ID values based on the SQL command placed into the
        #configuration file.
        gagesTmp = []
        gageIDsTmp = []

        try:
            gageInfo = db.getDomainID2(self)
        except:
            raise

        for gTmp in range(0,len(gageInfo)):
            gagesTmp.append(gageInfo[gTmp][1])
            gageIDsTmp.append(gageInfo[gTmp][0])

        self.gages = gagesTmp[:]
        self.gageIDs = gageIDsTmp[:]

    def calcGroupNum(self):
        """
        Function to calculate the number of basin groups based on the CPU layout provided
        by the user. This function also assigns a group number to each basin, along with
        a pre-determined beginning/ending CPU number based on the user-provided CPU layout
        informaiton in the configuration file.
        :return:
        """
        nCoresAvail = self.nCoresPerNode * self.nNodesMod
        self.numBasPerGroup = math.floor(nCoresAvail/self.nCoresMod)
        self.nGroups = math.ceil(len(self.gages)/self.numBasPerGroup)

        print(self.gages)
        print('NUM CORES PER NODE = ' + str(self.nCoresPerNode))
        print('NUM CORES AVAIL = ' + str(nCoresAvail))
        print('NUM BASINS PER GROUP = ' + str(self.numBasPerGroup))
        print('NUM BASINS = ' + str(len(self.gages)))
        print('NUM GROUPS = ' + str(self.nGroups))
        # Temporary arrays to calculate groups, CPU layout, etc
        gGroupTmp = []
        gBcpuTmp = []
        gEcpuTmp = []
        gCompleteTmp = []
        countTmp = 0

        for groupTmp in range(0,self.nGroups):
            begCpuTmpVal = 0
            endCpuTmpVal = 0
            # Initialize the complete flag for this group of basins to be 0. The
            # orchestrator program will set things to 1 if they are already complete.
            gCompleteTmp.append(0)
            for basinTmp in range(0,self.numBasPerGroup):
                if basinTmp == 0:
                    endCpuTmpVal = endCpuTmpVal + self.nCoresMod - 1
                else:
                    endCpuTmpVal = endCpuTmpVal + self.nCoresMod
                # Create CPU strides for each basin in this group.
                if basinTmp == 0:
                    begCpuTmpVal = begCpuTmpVal
                else:
                    begCpuTmpVal = begCpuTmpVal + self.nCoresMod
                gGroupTmp.append(groupTmp)
                gBcpuTmp.append(begCpuTmpVal)
                gEcpuTmp.append(endCpuTmpVal)

        self.gageGroup = gGroupTmp
        self.gageEndModelCpu = gEcpuTmp
        self.gageBegModelCpu = gBcpuTmp
        self.groupComplete = gCompleteTmp


    def readConfig(self,parser):
        """ Read in and check options passed by the config file.
        """
        self.jobName = str(parser.get('logistics','expName'))
        self.outDir = str(parser.get('logistics','outDir'))
        self.acctKey = str(parser.get('logistics','acctKey'))
        self.queName = str(parser.get('logistics','optQueName'))
        self.optCalStripFlag = int(parser.get('logistics','stripCalibOutputs'))
        self.optCalStripHrs = int(parser.get('logistics','stripCalibHours'))
        self.nCoresMod = int(parser.get('logistics','nCoresModel'))
        self.nNodesMod = int(parser.get('logistics','nNodesModel'))
        self.nCoresPerNode = int(parser.get('logistics','nCoresPerNode'))
        self.mpiCmd = str(parser.get('logistics','mpiCmd'))
        self.cpuPinCmd = str(parser.get('logistics','cpuPinCmd'))
        self.nIter = int(parser.get('logistics','numIter'))
        self.sensFlag = int(parser.get('logistics','runSens'))
        self.sensTbl = str(parser.get('logistics','sensParmTbl'))
        self.calibFlag = int(parser.get('logistics','runCalib'))
        self.trouteFlag = int(parser.get('logistics','runTroute'))
        self.trouteConfig = str(parser.get('logistics','trouteConfig'))
        self.moduleLoadStr = ast.literal_eval(parser.get('logistics','moduleLoadStr'))
        self.moduleLoadTrouteStr = ast.literal_eval(parser.get('logistics','moduleLoadTrouteStr'))
        self.calibTbl = str(parser.get('logistics','calibParmTbl'))
        self.dailyAnalysis = int(parser.get('logistics','dailyStats'))
        self.dbBackup = int(parser.get('logistics','dbBackup'))
        self.coldStart = int(parser.get('logistics','coldStart'))
        self.optSpinFlag = int(parser.get('logistics','optSpinFlag'))
        self.jobRunType = int(parser.get('logistics','jobRunType'))
        self.enableStreamflowCalib = int(parser.get('logistics','enableStreamflowCalib'))
        self.enableSnowCalib = int(parser.get('logistics','enableSnowCalib'))
        self.enableSoilMoistureCalib = int(parser.get('logistics','enableSoilMoistureCalib'))
        self.streamflowObjFunc = str(parser.get('logistics','streamflowObjectiveFunction'))
        self.snowObjFunc = str(parser.get('logistics','snowObjectiveFunction'))
        self.soilMoistureObjFunc = str(parser.get('logistics','soilMoistureObjectiveFunction'))
        self.streamflowWeight = float(parser.get('logistics','streamflowWeight'))
        self.snowWeight = float(parser.get('logistics','snowWeight'))
        self.soilMoistureWeight = float(parser.get('logistics','soilMoistureWeight'))
        self.basinType = str(parser.get('logistics','basinType')) #Xia 20210610
        self.weight1Event = str(parser.get('logistics','weight1Event'))
        self.weight2Event = str(parser.get('logistics','weight2Event'))
        self.ddsR = str(parser.get('logistics','ddsR'))
        if len(self.ddsR) != 0:
            self.ddsR = float(self.ddsR)
        self.email = str(parser.get('logistics','email'))
        self.enableMask = int(parser.get('logistics','enableMask'))
        #self.maskFile = str(parser.get('logistics','maskFile'))
        self.enableMultiSites=int(parser.get('logistics','enableMultiSites'))
        #self.slChan = str(parser.get('logistics','slackChannel'))
        #self.slToken = str(parser.get('logistics','slackToken'))
        #self.slUser = str(parser.get('logistics','slackUser'))
        # Initiate Slack object if user has specified. Throw an error message
        # if Slack is not successfully inititated.
        #if len(self.slChan) > 0:
        #    try:
        #        self.slackObj = Slacker(str(self.slToken))
        #    except:
        #        print "ERROR: Failure to initiate Slack."
        #        raise
        self.exe = str(parser.get('logistics','wrfExe'))
        self.genParmTbl = str(parser.get('logistics','genParmTbl'))
        self.mpParmTbl = str(parser.get('logistics','mpParmTbl'))
        self.urbParmTbl = str(parser.get('logistics','urbParmTbl'))
        self.vegParmTbl = str(parser.get('logistics','vegParmTbl'))
        self.soilParmTbl = str(parser.get('logistics','soilParmTbl'))
        self.bSpinDate = parser.get('logistics','bSpinDate')
        self.bSpinDate = datetime.datetime.strptime(self.bSpinDate,'%Y-%m-%d')
        self.eSpinDate = parser.get('logistics','eSpinDate')
        self.eSpinDate = datetime.datetime.strptime(self.eSpinDate,'%Y-%m-%d')
        self.bCalibDate = parser.get('logistics','bCalibDate')
        self.bCalibDate = datetime.datetime.strptime(self.bCalibDate,'%Y-%m-%d')
        self.eCalibDate = parser.get('logistics','eCalibDate')
        self.eCalibDate = datetime.datetime.strptime(self.eCalibDate,'%Y-%m-%d')
        self.bCalibEvalDate = parser.get('logistics','bCalibEvalDate')
        self.bCalibEvalDate = datetime.datetime.strptime(self.bCalibEvalDate,'%Y-%m-%d')
        # Calculate the beginning date for full outputs. If no optional
        # flag for stripping outputs is off, then set this date to the beginning
        # of the model simulation.
        if self.optCalStripFlag == 1:
            self.bCalibFullOutputs = self.bCalibDate + datetime.timedelta(seconds=3600*self.optCalStripHrs)
            # Run a check here..... If the user has specified a date that is NOT
            # the beginning of the month, throw an error. When minimal outputs are activated,
            # only restart files at the beginning of the month are available. If the
            # user specifies to stript outputs to a date that is any other step,
            # the workflow will continuously initialize calibration model simulations
            # from a timestep with no restart file available. 
            if self.bCalibFullOutputs.day != 1 and self.bCalibFullOutputs.hour != 0:
                print("ERROR: Please specify a stripCalibHours value that results in a date at the beginning of the month.")
                raise Exception()
        else:
            self.bCalibFullOutputs = self.bCalibDate
        self.bValidDate = parser.get('logistics','bValidDate')
        self.bValidDate = datetime.datetime.strptime(self.bValidDate,'%Y-%m-%d')
        self.eValidDate = parser.get('logistics','eValidDate')
        self.eValidDate = datetime.datetime.strptime(self.eValidDate,'%Y-%m-%d')
        self.bValidEvalDate = parser.get('logistics','bValidEvalDate')
        self.bValidEvalDate = datetime.datetime.strptime(self.bValidEvalDate,'%Y-%m-%d')
        self.nSensSample = int(parser.get('Sensitivity','sensParmSample'))
        self.nSensBatch = int(parser.get('Sensitivity','sensBatchNum'))
        self.bSensDate = parser.get('Sensitivity','bSensDate')
        self.bSensDate = datetime.datetime.strptime(self.bSensDate,'%Y-%m-%d')
        self.eSensDate = parser.get('Sensitivity','eSensDate')
        self.eSensDate = datetime.datetime.strptime(self.eSensDate,'%Y-%m-%d')
        self.bSensEvalDate = parser.get('Sensitivity','bSensEvalDate')
        self.bSensEvalDate = datetime.datetime.strptime(self.bSensEvalDate,'%Y-%m-%d')
        self.gSQL = parser.get('gageInfo','gageListSQL')
        self.gList = str(parser.get('gageInfo','gageListFile'))
        self.lsmSplitOutputCount = int(parser.get('lsmPhysics','SplitOutputCount'))
        self.dynVegOpt = int(parser.get('lsmPhysics','dynVegOption'))
        self.canStomOpt = int(parser.get('lsmPhysics','canStomResOption'))
        self.btrOpt = int(parser.get('lsmPhysics','btrOption'))
        self.runOffOpt = int(parser.get('lsmPhysics','runoffOption'))
        self.sfcDragOpt = int(parser.get('lsmPhysics','sfcDragOption'))
        self.frzSoilOpt = int(parser.get('lsmPhysics','frzSoilOption'))
        self.supCoolOpt = int(parser.get('lsmPhysics','supCoolOption'))
        self.radTOpt = int(parser.get('lsmPhysics','radTransferOption'))
        self.snAlbOpt = int(parser.get('lsmPhysics','snAlbOption'))
        self.pcpPartOpt = int(parser.get('lsmPhysics','pcpPartOption'))
        self.tbotOpt = int(parser.get('lsmPhysics','tbotOption'))
        self.timeSchmOpt = int(parser.get('lsmPhysics','tempTimeSchOption'))
        self.sfcResOpt = int(parser.get('lsmPhysics','sfcResOption'))
        self.glacier = int(parser.get('lsmPhysics','glacierOption'))
        self.IMPERV_OPTION = int(parser.get('lsmPhysics','IMPERV_OPTION'))
        self.soilThick = ast.literal_eval(parser.get('lsmPhysics','soilThick'))
        self.zLvl = float(parser.get('lsmPhysics','zLvl'))
        self.crocusOpt = int(parser.get('crocus','crocusOpt'))
        self.crocusFlag = int(parser.get('crocus','enableCrocusNamelist'))
        self.actLev = int(parser.get('crocus','actLev'))
        self.fType = int(parser.get('forcing','forceType'))
        self.fDT = int(parser.get('modelTime','forceDt'))
        self.lsmDt = int(parser.get('modelTime','lsmDt'))
        self.lsmOutDt = int(parser.get('modelTime','lsmOutDt'))
        self.lsmRstFreq = int(parser.get('modelTime','lsmRstFreq'))
        self.hydroRstFreq = int(parser.get('modelTime','hydroRstFreq'))
        self.hydroOutDt = int(parser.get('modelTime','hydroOutDt'))
        self.rstType = int(parser.get('hydroIO','rstType'))
        self.SplitOutputCount = int(parser.get('hydroIO','SplitOutputCount'))
        self.ioConfigOutputs = int(parser.get('hydroIO','ioConfigOutputs'))
        self.ioFormOutputs = int(parser.get('hydroIO','ioFormOutputs'))
        self.chrtoutDomain = int(parser.get('hydroIO','chrtoutDomain'))
        self.chanObs = int(parser.get('hydroIO','chanObsDomain'))
        self.chrtoutGrid = int(parser.get('hydroIO','chrtoutGrid'))
        self.lsmDomain = int(parser.get('hydroIO','lsmDomain'))
        self.rtoutDomain = int(parser.get('hydroIO','rtoutDomain'))
        self.gwOut = int(parser.get('hydroIO','gwOut'))
        self.lakeOut = int(parser.get('hydroIO','lakeOut'))
        self.frxstPts = int(parser.get('hydroIO','frxstOut'))
        self.resetHydro = int(parser.get('hydroIO','resetHydroAcc'))
        self.strOrder = int(parser.get('hydroIO','streamOrderOut'))
        self.dtChRt = int(parser.get('hydroPhysics','dtChSec'))
        self.dtTerRt = int(parser.get('hydroPhysics','dtTerSec'))
        self.subRtFlag = int(parser.get('hydroPhysics','subRouting'))
        self.ovrRtFlag = int(parser.get('hydroPhysics','ovrRouting'))
        self.rtOpt = int(parser.get('hydroPhysics','rtOpt'))
        self.imperv_adj=int(parser.get('hydroPhysics','imperv_adj'))
        self.chnRtFlag = int(parser.get('hydroPhysics','channelRouting'))
        self.chnRtOpt = int(parser.get('hydroPhysics','chanRtOpt'))
        self.udmpOpt = int(parser.get('hydroPhysics','udmpOpt'))
        self.gwBaseFlag = int(parser.get('hydroPhysics','gwBaseSw'))
        self.gwRst = int(parser.get('hydroPhysics','gwRestart'))
        self.enableCmpdChan = int(parser.get('hydroPhysics','enableCompoundChannel'))
        self.cmpdChan = int(parser.get('hydroPhysics','compoundChannel'))
        self.enableGwLoss = int(parser.get('hydroPhysics','enableGwBucketLoss'))
        self.gwLoss = int(parser.get('hydroPhysics','bucket_loss'))
        
def readConfig(configFile):
    """
    Generic function to read in data from a configuration file.
    """
    parser = SafeConfigParser()
    parser.read(configFile)
    
    jobObj = jobMeta()
    
    # Read in values
    try:
        jobMeta.readConfig(jobObj,parser)
    except:
        print("ERROR: Unable to assign values from config file.")
        raise
        
    return jobObj
    
def createJob(argsUser):
    """ Reads in options from the setup.parm file
    """
    # Check to make sure a non-zero length config file was passed by the user.
    if len(argsUser.configFile[0]) ==0:
        print("ERROR: Zero Length Configuration File Passed To Program.")
        raise Exception()

    configPath = argsUser.configFile[0]    
    parser = SafeConfigParser()
    
    if os.path.isfile(configPath):
        parser.read(configPath)
    else:
        print("ERROR: Config file not found.")
        raise Exception()

    # Check entries into the config file to make sure they make sense.
    try:
        checkConfig(parser)
    except:
        print("ERROR: Improper Entries Into Config File.")
        raise
    
    # Initialize job object
    jobObj = jobMeta()
    
    # Read in value
    try:
        jobMeta.readConfig(jobObj,parser)
    except:
        print("ERROR: Unable to assign values from config file.")
        raise
        
    # If calibration has been activated, check to make sure a valid parameter table was specified. 
    if jobObj.calibFlag == 1:
        if not os.path.isfile(jobObj.calibTbl):
            print("ERROR: Calibration parameter table: " + str(jobObj.calibTbl) + " not found.")
            raise Exception()
            
    # If sensitivity analysis was activated, check to make sure a valid parameter table was specified.
    if jobObj.sensFlag == 1:
        if not os.path.isfile(jobObj.sensTbl):
            print("ERROR: Sensitivity parameter table: " + str(jobObj.sensTbl) + " not found.")
            raise Exception()
        else:
            # Read in the sensitivity parameter table and calculate the total number 
            # of model iterations that will take place. Make sure the total number
            # is a multiple of the batch number
            tblTmp = pd.read_csv(jobObj.sensTbl,sep=',')
            nIterTmp = jobObj.nSensSample*(len(np.where(tblTmp.sens_flag == 1)[0])+1)
            if nIterTmp % jobObj.nSensBatch != 0:
                print("ERROR: Invalid number of sensitivity batch runs. Must be compatible with num_sens_params * (sample+1)")
                raise Exception()
            else:
                jobObj.nSensIter = nIterTmp
    else:
        jobObj.nSensIter = 0
        
    # Assign ownership to this job
    jobObj.owner = pwd.getpwuid(os.getuid()).pw_name
    
    return jobObj
    
def queryJob(argsUser):
    """
    Generic function to return information from a config file. This is mostly
    used for getJobID to provide a user with a jobID.
    """
    # Check to make sure a non-zero length config file was passed by the user.
    if len(argsUser.configFile[0]) ==0:
        print("ERROR: Zero Length Configuration File Passed To Program.")
        raise Exception()

    configPath = argsUser.configFile[0]    
    parser = SafeConfigParser()
    
    if os.path.isfile(configPath):
        parser.read(configPath)
    else:
        print("ERROR: Config file not found.")
        raise Exception()

    # Initialize job object
    jobObj = jobMeta()
    
    # Read in values
    try:
        jobMeta.readConfig(jobObj,parser)
    except:
        print("ERROR: Unable to assign values from config file.")
        raise
        
    return jobObj
    
def checkConfig(parser):
    """ Function to check all options in the config file.
    """
    # Go through and check everything put into the config file.
    check = str(parser.get('logistics','outDir'))
    if len(check) == 0:
        print("ERROR: Zero length output directory provided.")
        raise Exception()
    if not os.path.isdir(check):
        print("ERROR: Directory: " + check + " not found.")
        raise Exception()

    check = str(parser.get('logistics','expName'))
    if len(check) == 0:
        print("ERROR: Zero length expName provided.")
        raise Exception()
        
    check = str(parser.get('logistics','acctKey'))
    if len(check) == 0:
        print("WARNING: Zero length account key passed to program.")

    # We won't check the optional que name as it's optional. Even if some 
    # run with a job submission method, they may not need to run with a que.
        
    # Either email or Slack must be chosen. If Slack is chosen, user
    # must provide both channel and API token.
    # FOR NOW WILL RELAX EMAIL CONSTRAINT
    check1 = str(parser.get('logistics','email'))
    if len(check1) == 0:
        print("WARNING: Zero length email passed. Proceed with caution....")
    #check2 = str(parser.get('logistics','slackChannel'))
    #check3 = str(parser.get('logistics','slackToken'))
    #check4 = str(parser.get('logistics','slackUser'))
    #if len(check1) > 0 and len(check2) > 0:
    #    print "ERROR: You must choose either email or Slack for error reporting."
    #    raise Exception()
    #if len(check1) == 0 and len(check2) == 0:
    #    print "ERROR: You must specify an error reporting method."
    #    raise Exception()
    #if len(check2) > 0 and len(check3) == 0:
    #    print "ERROR: You must enter a Slack token."
    #    raise Exception()
    #if len(check2) > 0 and len(check4) == 0:
    #    print "ERROR: You must enter a Slack user name."
    #    raise Exception()

    checkCoresModel = int(parser.get('logistics','nCoresModel'))
    if not checkCoresModel:
        print("ERROR: Number of model cores to use not specified.")
        raise Exception()
    if checkCoresModel <= 0:
        print("ERROR: Invalid number of model cores to use.")
        raise Exception()
    checkNodesModel = int(parser.get('logistics','nNodesModel'))
    if not checkNodesModel:
        print("ERROR: Number of model nodes to use not specified.")
        raise Exception()
    if checkNodesModel <= 0:
        print("ERROR: Invalid number of model nodes to use.")
        raise Exception()
        
    # Check calibration/sensitivity activation flags.
    check = int(parser.get('logistics','runSens'))
    if check < 0 or check > 1:
        print("ERROR: Invalid runSens flag specified.")
        raise Exception()
    check = int(parser.get('logistics','runCalib'))
    if check < 0 or check > 1:
        print("ERROR: Invalid runCalib flag specified.")
        raise Exception()
        
    # Check to make sure a valid option was passed for running model/R code
    check = int(parser.get('logistics','jobRunType'))
    if check < 1 or check > 6:
        print("ERROR: Invalid jobRunType specified.")
        raise Exception()

    # Make sure a proper MPI command was passed. This is required.
    check = str(parser.get('logistics','mpiCmd'))
    if len(check) == 0:
        print('ERROR: Please specify an mpiCmd to use for running the model.')
        raise Exception()
        
    checkCoresPerNode = int(parser.get('logistics','nCoresPerNode'))
    if not checkCoresPerNode:
        print('ERROR: Number of nCoresPerNode to use not specified.')
        raise Exception()
    if checkNodesModel > 1:
        if (checkCoresModel*checkNodesModel)%(checkCoresPerNode) != 0:
            print("ERROR: Number of cores being used to run the model is not an equal divider " + \
                  "of the number of cores per node.")
            raise Exception()
    else:
        if checkCoresPerNode%checkCoresModel != 0:
            print('ERROR Number of cores being sued to run the model is not an equal divider ' + \
                  'of the number of cores per node.')
            raise Exception()

    check = int(parser.get('logistics','dailyStats'))
    if check < 0 or check > 1:
        print("ERROR: Invalid dailyStats value specified.")
        raise Exception()

    check = int(parser.get('logistics','dbBackup'))
    if check < 0 or check > 1:
        print("ERROR: Invalid dbBackup value specified.")
        raise Exception()
        
    check = int(parser.get('logistics','coldStart'))
    if check < 0 or check > 1:
        print("ERROR: Invalid coldStart value specified.")
        raise Exception()
        
    check = int(parser.get('logistics','optSpinFlag'))
    if check < 0 or check > 1:
        print("ERROR: Invalid optSpinFlag value specified.")
        raise Exception()
        
    check1 = int(parser.get('logistics','coldStart'))
    check2 = int(parser.get('logistics','optSpinFlag'))
    if check1 == 1 and check2 == 1:
        print("ERROR: Cannot run cold start calibrations with optional spinup files.")
        raise Exception()
    
    # Check to make sure calibration method is DDS
    check = str(parser.get('logistics','calibMethod'))
    if check != "DDS":
        print("ERROR: Invalid calibration method passed to program.")
        raise Exception()
        
    # Check optional calibration output strip options.
    check1 = int(parser.get('logistics','stripCalibOutputs'))
    if check1 < 0 or check1 > 1:
        print("ERROR: Invalid stripCalibOutputs option passed to program.")
        raise Exception()
    check2 = int(parser.get('logistics','stripCalibHours'))
    if check1 == 1:
        if check2 < 0:
            print("ERROR: Invalid stripCalibHours passed to program.")
            raise Exception()
        
    check = str(parser.get('logistics','streamflowObjectiveFunction'))
    if len(check) == 0:
        print("ERROR: Zero length calibration objective function provided.")
        raise Exception()
    # For now, restrict the user to a set of pre-defined objective functions.
    #if check != "Rmse" and check != "Nse" and check != "NseLog" and check != "NseWt" and check != "Kge" and check != "Msof" and check != "hyperResMultiObj":
    #    print("ERROR: Only acceptable objectiveFunction values are: Rmse, Nse, NseLog, NseWt, Kge, and hyperResMultiObj") # commented by Xia 20200618
    if (check != "Rmse" and check != "Nse" and check != "NseLog" and check != "NseWt" and check != "Kge" and check != "Msof" and check != "hyperResMultiObj"  
      and check != "NNseSq" and check != "LBEmPrime" and check != "EventMultiObj"):
        print("ERROR: Only acceptable objectiveFunction values are: Rmse, Nse, NseLog, NseWt, Kge, hyperResMultiObj, NNseSq, LBEmPrime, EventMultiObj")
        raise Exception()
        
    check = int(parser.get('logistics','numIter'))
    if not check:
        print("ERROR: Number of calibration iterations not specified.")
        raise Exception()
    if check <= 0:
        print("ERROR: Invalid number of calibration iterations specified.")
        raise Exception()
        
    check = str(parser.get('logistics','wrfExe'))
    if len(check) == 0:
        print("ERROR: Zero length executable provided.")
        raise Exception()
    if not os.path.isfile(check):
        print("ERROR: File: " + check + " not found.")
        raise Exception()
        
    # Parameter tables
    check = str(parser.get('logistics','genParmTbl'))
    if len(check) == 0:
        print("ERROR: Zero length general parameter table provided.")
        raise Exception()
    if not os.path.isfile(check):
        print("ERROR: File: " + check + " not found.")
        raise Exception()
        
    check = str(parser.get('logistics','mpParmTbl'))
    if len(check) == 0:
        print("ERROR: Zero length MP parameter table provided.")
        raise Exception()
    if not os.path.isfile(check):
        print("ERROR: File: " + check + " not found.")
        raise Exception()
        
#    check = str(parser.get('logistics','urbParmTbl'))
#    if len(check) == 0:
#        print("ERROR: Zero length urban parameter table provided.")
#        raise Exception()
#    if not os.path.isfile(check):
#        print("ERROR: File: " + check + " not found.")
#        raise Exception()
        
#    check = str(parser.get('logistics','vegParmTbl'))
#    if len(check) == 0:
#        print("ERROR: Zero length vegetation parameter table provided.")
#        raise Exception()
#    if not os.path.isfile(check):
#        print("ERROR: File: " + check + " not found.")
#        raise Exception()
        
    check = str(parser.get('logistics','soilParmTbl'))
    if len(check) == 0:
        print("ERROR: Zero length soil parameter table provided.")
        raise Exception()
    if not os.path.isfile(check):
        print("ERROR: File: " + check + " not found.")
        raise Exception()
        
    # Date information
    bDate = parser.get('logistics','bSpinDate')
    eDate = parser.get('logistics','eSpinDate')
    bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
    eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
    if bDate >= eDate:
        print("ERROR: Must specify ending spinup date greater than beginning spinup date.")
        raise Exception()
        
    bDate = parser.get('logistics','bCalibDate')
    eDate = parser.get('logistics','eCalibDate')
    bEDate = parser.get('logistics','bCalibEvalDate')
    bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
    eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
    bEDate = datetime.datetime.strptime(str(bEDate),'%Y-%m-%d')
    if bDate >= eDate:
        print("ERROR: Must specify ending calibration date greater than beginning spinup date.")
        raise Exception()
    if bEDate >= eDate:
        print("ERROR: Must specify the beginning date for calibration evaluation date " + \
              " that is before the ending date for calibration simulations.")
        raise Exception()
        
    bDate = parser.get('logistics','bValidDate')
    eDate = parser.get('logistics','eValidDate')
    bEDate = parser.get('logistics','bValidEvalDate')
    bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
    eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
    bEDate = datetime.datetime.strptime(str(bEDate),'%Y-%m-%d')
    if bDate >= eDate:
        print("ERROR: Must specify ending validation date greater than beginning validation date.")
        raise Exception()
    if bEDate >= eDate:
        print("ERROR: Must specify the beginning date for validation evaluation date " + \
              " that is before the ending date for validation simulations.")
        raise Exception()
        
    check = int(parser.get('logistics','runSens'))
    # Only check these options if sensitivity analysis has been turned on.
    if check == 1:
        check1 = int(parser.get('Sensitivity','sensParmSample'))
        if check1 <= 0:
            print("ERROR: Please choose numSensIter greater than 0.")
            raise Exception()
        check2 = int(parser.get('Sensitivity','sensBatchNum'))
        if check2 <= 0:
            print("ERROR: Please choose sensBatchNum greater than 0.")
            raise Exception()
        bDate = parser.get('Sensitivity','bSensDate')
        eDate = parser.get('Sensitivity','eSensDate')
        bEDate = parser.get('Sensitivity','bSensEvalDate')
        bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
        eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
        bEDate = datetime.datetime.strptime(str(bEDate),'%Y-%m-%d')
        if bDate >= eDate:
            print("ERROR: Must specify ending sensitivity date greater than beginning sensitivity date.")
            raise Exception()
        if bEDate >= eDate:
            print("ERROR: Must specify the beginning date for sensitivity evaluation date " + \
                  " that is before the ending date for validation simulations.")
            raise Exception()
    
    # Check gauge information
    check1 = str(parser.get('gageInfo','gageListFile'))
    check2 = str(parser.get('gageInfo','gageListSQL'))
    if len(check1) == 0 and len(check2) == 0:
        print("ERROR: Zero length gage list file and SQL command passed to program.")
        raise Exception()
    if len(check1) > 0 and len(check2) > 0:
        print("ERROR: Cannot have both gage list and SQL command.")
        raise Exception()
    if len(check1) > 0:
        if not os.path.isfile(check1):
            print("ERROR: File: " + check2 + " not found.")
            raise Exception()
        
    # Check LSM physics options
    check = int(parser.get('lsmPhysics','dynVegOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid dynamic vegetation option chosen.")
        raise Exception()
    
    check = int(parser.get('lsmPhysics','canStomResOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid canopy stomatal resistance option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','btrOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid BTR option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','runoffOption'))
    if check < 0 or check > 7:
        print("ERROR: Invalid LSM runoff option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','sfcDragOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid surface drag coefficient option chosen.")
        raise Exception()
    
    check = int(parser.get('lsmPhysics','frzSoilOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid frozen soil option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','supCoolOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid supercooled water option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','radTransferOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid radiative transfer option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','snAlbOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid snow albedo option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','pcpPartOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid precip partitioning option chosen.")
        raise Exception()
    
    check = int(parser.get('lsmPhysics','tbotOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid TBOT option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','tempTimeSchOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid temperature time scheme option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','sfcResOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid surface resistence option chosen.")
        raise Exception()
        
    check = int(parser.get('lsmPhysics','glacierOption'))
    if check < 0 or check > 4:
        print("ERROR: Invalid glacier option chosen.")
        raise Exception()

    # Check soil moisture thickness values
    check = ast.literal_eval(parser.get('lsmPhysics','soilThick'))
    if len(check) != 4:
        print("ERROR: Must specify four soil layer thicknesses.")
        raise Exception()
        
    # Check z-level
    check = parser.get('lsmPhysics','zLvl')
    if float(check) < 0:
        print("ERROR: zLvl must be greater than or equal to 0.0 meters.")
        raise Exception()
    if len(check) == 0:
        print("ERROR: Zero length zLvl passed to program.")
        raise Exception()
       
    # Check crocus options
    check = int(parser.get('crocus','enableCrocusNamelist'))
    if check < 0 or check > 1:
        print("ERROR: Invalid crocus switch passed to program.")
        raise Exception()  

    check = int(parser.get('crocus','crocusOpt'))
    if check < 0 or check > 1:
        print("ERROR: Invalid crocus opt value passed to program.")
        raise Exception()

    check = int(parser.get('crocus','actLev'))
    if not check:
        print("ERROR: actLev not specified.")
        raise Exception()
    if check <= 0 or check > 50:
        print("ERROR: Invalid number of actLev specified.")
        raise Exception()

    # Check forcing options
    check = parser.get('forcing','forceType')
    if len(check) == 0:
        print("ERROR: Zero length forceType value passed to program.")
        raise Exception()
    if int(check) < 1 or int(check) > 6:
        print("ERROR: Invalid forceType value passed to program.")
        raise Exception()
        
    # Make sure output frequencies aren't < 0
    check = int(parser.get('modelTime','forceDt'))
    if check < 0:
        print("ERROR: Invalid forcing DT passed to program.")
        raise Exception()
        
    check = int(parser.get('modelTime','lsmDt'))
    if check < 0:
        print("ERROR: Invalid LSM DT passed to program.")
        raise Exception()
        
    check = int(parser.get('modelTime','lsmOutDt'))
    if check < 0:
        print("ERROR: Invalid LSM Output DT passed to program.")
        raise Exception()
        
    check = int(parser.get('modelTime','lsmRstFreq'))
    if check < 0:
        if check != -9999:
            print("ERROR: Invalid LSM restart frequency passed to program.")
            raise Exception()
        
    check = int(parser.get('modelTime','hydroRstFreq'))
    if check < 0:
        if check != -99999:
            print("ERROR: Invalid Hydro restart frequency passed to program.")
            raise Exception()
        
    check = int(parser.get('modelTime','hydroOutDt'))
    if check < 0:
        print("ERROR: Invalid Hydro output DT passed to program.")
        raise Exception()
        
    # Check Hydro IO options
    check = parser.get('hydroIO','rstType')
    if len(check) == 0:
        print("ERROR: Zero length rstType passed to program.")
        raise Exception()
    if int(check) < 0 or int(check) > 1:
        print("ERROR: Invalid rstType passed to program.")
        raise Exception()
        
    check = parser.get('hydroIO','ioConfigOutputs')
    if len(check) == 0:
        print("ERROR: Zero length ioConfigOutputs passed to program.")
        raise Exception()
    if int(check) < 0 or int(check) > 6:
        print("ERROR: Invalid ioConfigOutputs passed to program.")
        raise Exception()
        
    check = parser.get('hydroIO','ioFormOutputs')
    if len(check) == 0:
        print("ERROR: Zero length ioFormOutputs passed to program.")
        raise Exception()
    if int(check) < 0 or int(check) > 4:
        print("ERROR: Invalid ioFormOutputs passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','chrtoutDomain'))
    if check < 0 or check > 1:
        print("ERROR: Invalid CHRTOUT_DOMAIN option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','chanObsDomain'))
    if check < 0 or check > 1:
        print("ERROR: Invalid CHANOBS_DOMAIN optino passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','chrtoutGrid'))
    if check < 0 or check > 1:
        print("ERROR: Invalid CHRTOUT_GRID option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','lsmDomain'))
    if check < 0 or check > 1:
        print("ERROR: Invalid LSMOUT_DOMAIN option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','rtoutDomain'))
    if check < 0 or check > 1:
        print("ERROR: Invalid RTOUT_DOMAIN option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','gwOut'))
    if check < 0 or check > 1:
        print("ERROR: Invalid GW_OUT option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','lakeOut'))
    if check < 0 or check > 1:
        print("ERROR: Invalid LAKE_OUT option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','frxstOut'))
    if check < 0 or check > 1:
        print("ERROR: Invalid frxstOut option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','resetHydroAcc'))
    if check < 0 or check > 1:
        print("ERROR: Invalid RSTRT_SWC option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroIO','streamOrderOut'))
    if check < 0 or check > 4:
        print("ERROR: Invalid stream order output option passed to program.")
        raise Exception()
        
    # Check hydro physics options
    check = int(parser.get('hydroPhysics','dtChSec'))
    if check < 0:
        print("ERROR: Invalid DTRT_CH option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroPhysics','dtTerSec'))
    if check < 0:
        print("ERROR: Invalid DTRT_TER option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroPhysics','subRouting'))
    if check < 0 or check > 1:
        print("ERROR: Invalid sub-surface routing switch passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroPhysics','ovrRouting'))
    if check < 0 or check > 1:
        print("ERROR: Invalid overland routing switch passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroPhysics','channelRouting'))
    if check < 0 or check > 1:
        print("ERROR: Invalid channel routing switch passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroPhysics','rtOpt'))
    if check < 0 or check > 2:
        print("ERROR: Invalid overland/subsurface routing option passed to program.")
        raise Exception()
    
    check = int(parser.get('hydroPhysics','chanRtOpt'))
    if check < -1 or check > 3 or check == 0:
        print("ERROR: Invalid channel routing option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroPhysics','udmpOpt'))
    if check < 0 or check > 1:
        print("ERROR: Invalid user-defined mapping option passed to program.")
        raise Exception()
        
    check = int(parser.get('hydroPhysics','gwBaseSw'))
    if check < 0 or check > 4:
        print("ERROR: Invalid groundwater bucket switch passed to program.")
        raise Exception()
    
    check = int(parser.get('hydroPhysics','gwRestart'))
    if check < 0 or check > 1:
        print("ERROR: Invalid ground water restart switch passed to program.")
        raise Exception()

    check1 = int(parser.get('hydroPhysics','enableCompoundChannel'))
    if check1 < 0 or check1 > 1:
        print('ERROR: Inavlid enableCompoundChannel option passed to program.')
        raise Exception()
        
    check2 = int(parser.get('hydroPhysics','compoundChannel'))
    if check2 < 0 or check2 > 1:
        print("ERROR: Invalid compoundChannel switch passed to program.")
        raise Exception()

    if check1 == 0 and check2 == 1:
        print("ERROR: Cannot turn on compound channel without enabling the namelist option.")
        raise Exception()
        
    # Ensure muskingum cunge routing has been chosen if compound channel is activated.
    check1 = int(parser.get('hydroPhysics','compoundChannel'))
    check2 = int(parser.get('hydroPhysics','chanRtOpt'))
    if check1 == 1 and check2 != 2:
        print("ERROR: Compound channel can only be used with Muskingum Cunge Reach channel routing.")
        raise Exception()
    
    # Read in the groundwater loss options.
    check1 = int(parser.get('hydroPhysics','enableGwBucketLoss'))
    check2 = int(parser.get('hydroPhysics','bucket_loss'))
    if check1 < 0 or check1 > 1:
        print('ERROR: Invalid enableGwBucketLoss option specified in the configuration file.')
        raise Exception()
    if check1 == 0 and check2 == 1:
        print('ERROR: Cannot activate bucket_loss in the namelist if enableGwBucketLoss is off.')
        raise  Exception()

    # Read in the mask options.
    check1 = int(parser.get('logistics','enableMask'))
    #check2 = str(parser.get('logistics','maskFile'))
    if check1 < 0 or check1 > 1:
        print('ERROR: Invalid enableMask option specified in the configuration file.')
        raise Exception()
    #    if check1 == 1 and len(check2) <= 0:
    #        print('ERROR: Name of the mask file should be specified and \
    #            the mask should be located under the domain directory specified in the domainMeta.csv file.')
    #        raise  Exception()

    # Read in the multis site calibraton option
    check1 = int(parser.get('logistics','enableMultiSites'))
    if check1 < 0 or check1 > 1:
        print(check1)
        print('ERROR: Invalid enableMultiSites specified in the configuration file.')
        raise Exception()





