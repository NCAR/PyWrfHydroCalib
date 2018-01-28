# File to read in information from the config file and store it in a Python
# class object. This is called one time for each job that is initialized. 
# Options are checked to ensure no improper entries were put into the
# config file.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

from ConfigParser import SafeConfigParser
import os
import datetime
import ast
import pwd
import pandas as pd
import numpy as np
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
        self.queNameAnalysis = []
        self.nCoresMod = []
        self.nNodesMod = []
        self.nCoresR = []
        self.nNodesR = []
        self.sensFlag = []
        self.sensTbl = []
        self.calibFlag = []
        self.calibTbl = []
        self.jobRunType = []
        self.analysisRunType = []
        self.host = []
        self.port = []
        self.nIter = []
        self.calibMethod = []
        self.objFunc = []
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
        self.chanParmTbl = []
        self.soilParmTbl = []
        self.bSpinDate = []
        self.eSpinDate = []
        self.bCalibDate = []
        self.eCalibDate = []
        self.bCalibEvalDate = []
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
        self.chnRtOpt = []
        self.rtOpt = []
        self.udmpOpt = []
        self.gwBaseFlag = []
        self.gwRst = []
        self.gages = []
        self.gageIDs = []
        self.dbUName = []
        self.dbPwd = []
    def readConfig(self,parser):
        """ Read in and check options passed by the config file.
        """
        self.jobName = str(parser.get('logistics','jobName'))
        self.outDir = str(parser.get('logistics','outDir'))
        self.acctKey = str(parser.get('logistics','acctKey'))
        self.queName = str(parser.get('logistics','optQueNameModel'))
        self.queNameAnalysis = str(parser.get('logistics','optQueNameAnalysis'))
        self.nCoresMod = int(parser.get('logistics','nCoresModel'))
        self.nNodesMod = int(parser.get('logistics','nNodesModel'))
        self.nCoresR = int(parser.get('logistics','nCoresR'))
        self.nNodesR = int(parser.get('logistics','nNodesR'))
        #self.host = str(parser.get('logistics','postgresHost'))
        self.nIter = int(parser.get('logistics','numIter'))
        self.sensFlag = int(parser.get('logistics','runSens'))
        self.sensTbl = str(parser.get('logistics','sensParmTbl'))
        self.calibFlag = int(parser.get('logistics','runCalib'))
        self.calibTbl = str(parser.get('logistics','calibParmTbl'))
        self.jobRunType = int(parser.get('logistics','jobRunType'))
        self.analysisRunType = int(parser.get('logistics','analysisRunType'))
        self.objFunc = str(parser.get('logistics','objectiveFunction'))
        self.ddsR = str(parser.get('logistics','ddsR'))
        if len(self.ddsR) != 0:
            self.ddsR = float(self.ddsR)
        self.email = str(parser.get('logistics','email'))
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
        self.chanParmTbl = str(parser.get('logistics','chanParmTbl'))
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
        self.soilThick = ast.literal_eval(parser.get('lsmPhysics','soilThick'))
        self.zLvl = float(parser.get('lsmPhysics','zLvl'))
        self.fType = int(parser.get('forcing','forceType'))
        self.fDT = int(parser.get('modelTime','forceDt'))
        self.lsmDt = int(parser.get('modelTime','lsmDt'))
        self.lsmOutDt = int(parser.get('modelTime','lsmOutDt'))
        self.lsmRstFreq = int(parser.get('modelTime','lsmRstFreq'))
        self.hydroRstFreq = int(parser.get('modelTime','hydroRstFreq'))
        self.hydroOutDt = int(parser.get('modelTime','hydroOutDt'))
        self.rstType = int(parser.get('hydroIO','rstType'))
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
        self.chnRtFlag = int(parser.get('hydroPhysics','channelRouting'))
        self.chnRtOpt = int(parser.get('hydroPhysics','chanRtOpt'))
        self.udmpOpt = int(parser.get('hydroPhysics','udmpOpt'))
        self.gwBaseFlag = int(parser.get('hydroPhysics','gwBaseSw'))
        self.gwRst = int(parser.get('hydroPhysics','gwRestart'))
        
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
        print "ERROR: Unable to assign values from config file."
        raise
        
    return jobObj
    
def createJob(argsUser):
    """ Reads in options from the setup.parm file
    """
    # Check to make sure a non-zero length config file was passed by the user.
    if len(argsUser.configFile[0]) ==0:
        print "ERROR: Zero Length Configuration File Passed To Program."
        raise Exception()

    configPath = argsUser.configFile[0]    
    parser = SafeConfigParser()
    
    if os.path.isfile(configPath):
        parser.read(configPath)
    else:
        print "ERROR: Config file not found."
        raise Exception()

    # Check entries into the config file to make sure they make sense.
    try:
        checkConfig(parser)
    except:
        print "ERROR: Improper Entries Into Config File."
        raise
        
    # Initialize job object
    jobObj = jobMeta()
    
    # Read in values
    try:
        jobMeta.readConfig(jobObj,parser)
    except:
        print "ERROR: Unable to assign values from config file."
        raise
        
    # If calibration has been activated, check to make sure a valid parameter table was specified. 
    if jobObj.calibFlag == 1:
        if not os.path.isfile(jobObj.calibTbl):
            print "ERROR: Calibration parameter table: " + str(jobObj.calibTbl) + " not found."
            raise Exception()
            
    # If sensitivity analysis was activated, check to make sure a valid parameter table was specified.
    if jobObj.sensFlag == 1:
        if not os.path.isfile(jobObj.sensTbl):
            print "ERROR: Sensitivity parameter table: " + str(jobObj.sensTbl) + " not found."
            raise Exception()
        else:
            # Read in the sensitivity parameter table and calculate the total number 
            # of model iterations that will take place. Make sure the total number
            # is a multiple of the batch number
            tblTmp = pd.read_csv(jobObj.sensTbl,sep=',')
            nIterTmp = jobObj.nSensSample*(len(np.where(tblTmp.sens_flag == 1)[0])+1)
            if nIterTmp % jobObj.nSensBatch != 0:
                print "ERROR: Invalid number of sensitivity batch runs. Must be compatible with num_sens_params * (sample+1)"
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
        print "ERROR: Zero Length Configuration File Passed To Program."
        raise Exception()

    configPath = argsUser.configFile[0]    
    parser = SafeConfigParser()
    
    if os.path.isfile(configPath):
        parser.read(configPath)
    else:
        print "ERROR: Config file not found."
        raise Exception()

    # Initialize job object
    jobObj = jobMeta()
    
    # Read in values
    try:
        jobMeta.readConfig(jobObj,parser)
    except:
        print "ERROR: Unable to assign values from config file."
        raise
        
    return jobObj
    
def checkConfig(parser):
    """ Function to check all options in the config file.
    """
    # Go through and check everything put into the config file.
    check = str(parser.get('logistics','outDir'))
    if len(check) == 0:
        print "ERROR: Zero length output directory provided."
        raise Exception()
    if not os.path.isdir(check):
        print "ERROR: Directory: " + check + " not found."
        raise Exception()

    check = str(parser.get('logistics','jobName'))
    if len(check) == 0:
        print "ERROR: Zero length job name provided."
        raise Exception()
        
    check = str(parser.get('logistics','acctKey'))
    if len(check) == 0:
        print "WARNING: Zero length account key passed to program."

    # We won't check the optional que name as it's optional. Even if some 
    # run with a job submission method, they may not need to run with a que.
        
    #check = str(parser.get('logistics','postgresHost'))
    #if len(check) == 0:
    #    print "ERROR: Zero length Postgres Host passed length passed to program."
    #    raise Exception()
        
    # Either email or Slack must be chosen. If Slack is chosen, user
    # must provide both channel and API token.
    # FOR NOW WILL RELAX EMAIL CONSTRAINT
    check1 = str(parser.get('logistics','email'))
    if len(check1) == 0:
        print "WARNING: Zero length email passed. Proceed with caution...."
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

    check = int(parser.get('logistics','nCoresModel'))
    if not check:
        print "ERROR: Number of model cores to use not specified."
        raise Exception()
    if check <= 0:
        print "ERROR: Invalid number of model cores to use."
        raise Exception()
    check = int(parser.get('logistics','nNodesModel'))
    if not check:
        print "ERROR: Number of model nodes to use not specified."
        raise Exception()
    if check <= 0:
        print "ERROR: Invalid number of model nodes to use."
        raise Exception()
        
    # Check calibration/sensitivity activation flags.
    check = int(parser.get('logistics','runSens'))
    if check < 0 or check > 1:
        print "ERROR: Invalid runSens flag specified."
        raise Exception()
    check = int(parser.get('logistics','runCalib'))
    if check < 0 or check > 1:
        print "ERROR: Invalid runCalib flag specified."
        raise Exception()
        
    # Check to make sure a valid option was passed for running model/R code
    check = int(parser.get('logistics','jobRunType'))
    if check < 1 or check > 5:
        print "ERROR: Invalid jobRunType specified."
        raise Exception()
        
    check = int(parser.get('logistics','analysisRunType'))
    if check < 1 or check > 5:
        print "ERROR: Invalid analysisRunType specified."
        raise Exception()
        
    check = int(parser.get('logistics','nCoresR'))
    if not check:
        print "ERROR: Number of R Cores to use not specified."
        raise Exception()
    check = int(parser.get('logistics','nNodesR'))
    if not check:
        print "ERROR: Number of R Nodes to use not specified."
        raise Exception()
    if check <= 0:
        print "ERROR: Invalid number of R Nodes to use."
        raise Exception()
        
    # Check to make sure calibration method is DDS
    check = str(parser.get('logistics','calibMethod'))
    if check != "DDS":
        print "ERROR: Invalid calibration method passed to program."
        raise Exception()
        
    check = str(parser.get('logistics','objectiveFunction'))
    if len(check) == 0:
        print "ERROR: Zero length calibration objective function provided."
        raise Exception()
    # For now, restrict the user to a set of pre-defined objective functions.
    if check != "Rmse" and check != "Nse" and check != "NseLog" and check != "NseWt" and check != "Kge" and check != "Msof":
        print "ERROR: Only acceptable objectiveFunction values are: Rmse, Nse, NseLog, NseWt, and Kge"
        raise Exception()
        
    check = int(parser.get('logistics','numIter'))
    if not check:
        print "ERROR: Number of calibration iterations not specified."
        raise Exception()
    if check <= 0:
        print "ERROR: Invalid number of calibration iterations specified."
        raise Exception()
        
    check = str(parser.get('logistics','wrfExe'))
    if len(check) == 0:
        print "ERROR: Zero length executable provided."
        raise Exception()
    if not os.path.isfile(check):
        print "ERROR: File: " + check + " not found."
        raise Exception()
        
    # Parameter tables
    check = str(parser.get('logistics','genParmTbl'))
    if len(check) == 0:
        print "ERROR: Zero length general parameter table provided."
        raise Exception()
    if not os.path.isfile(check):
        print "ERROR: File: " + check + " not found."
        raise Exception()
        
    check = str(parser.get('logistics','mpParmTbl'))
    if len(check) == 0:
        print "ERROR: Zero length MP parameter table provided."
        raise Exception()
    if not os.path.isfile(check):
        print "ERROR: File: " + check + " not found."
        raise Exception()
        
    check = str(parser.get('logistics','urbParmTbl'))
    if len(check) == 0:
        print "ERROR: Zero length urban parameter table provided."
        raise Exception()
    if not os.path.isfile(check):
        print "ERROR: File: " + check + " not found."
        raise Exception()
        
    check = str(parser.get('logistics','vegParmTbl'))
    if len(check) == 0:
        print "ERROR: Zero length vegetation parameter table provided."
        raise Exception()
    if not os.path.isfile(check):
        print "ERROR: File: " + check + " not found."
        raise Exception()
        
    check = str(parser.get('logistics','chanParmTbl'))
    if len(check) == 0:
        print "ERROR: Zero length channel parameter table provided."
        raise Exception()
    if not os.path.isfile(check):
        print "ERROR: File: " + check + " not found."
        raise Exception()
        
    check = str(parser.get('logistics','soilParmTbl'))
    if len(check) == 0:
        print "ERROR: Zero length soil parameter table provided."
        raise Exception()
    if not os.path.isfile(check):
        print "ERROR: File: " + check + " not found."
        raise Exception()
        
    # Date information
    bDate = parser.get('logistics','bSpinDate')
    eDate = parser.get('logistics','eSpinDate')
    bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
    eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
    if bDate >= eDate:
        print "ERROR: Must specify ending spinup date greater than beginning spinup date."
        raise Exception()
        
    bDate = parser.get('logistics','bCalibDate')
    eDate = parser.get('logistics','eCalibDate')
    bEDate = parser.get('logistics','bCalibEvalDate')
    bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
    eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
    bEDate = datetime.datetime.strptime(str(bEDate),'%Y-%m-%d')
    if bDate >= eDate:
        print "ERROR: Must specify ending calibration date greater than beginning spinup date."
        raise Exception()
    #if bEDate <= bDate:
    #    print "ERROR: Must specify the beginning date for calibration evaluation date " + \
    #          " that is after the beginning date for calibration simulations."
    #    raise Exception()
    if bEDate >= eDate:
        print "ERROR: Must specify the beginning date for calibration evaluation date " + \
              " that is before the ending date for calibration simulations."
        raise Exception()
        
    bDate = parser.get('logistics','bValidDate')
    eDate = parser.get('logistics','eValidDate')
    bEDate = parser.get('logistics','bValidEvalDate')
    bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
    eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
    bEDate = datetime.datetime.strptime(str(bEDate),'%Y-%m-%d')
    if bDate >= eDate:
        print "ERROR: Must specify ending validation date greater than beginning validation date."
        raise Exception()
    #if bEDate <= bDate:
    #    print "ERROR: Must specify the beginning date for validation evaluation date " + \
    #          " that is after the beginning date for validation simulations."
    #    raise Exception()
    if bEDate >= eDate:
        print "ERROR: Must specify the beginning date for validation evaluation date " + \
              " that is before the ending date for validation simulations."
        raise Exception()
        
    check = int(parser.get('logistics','runSens'))
    # Only check these options if sensitivity analysis has been turned on.
    if check == 1:
        check1 = int(parser.get('Sensitivity','sensParmSample'))
        if check1 <= 0:
            print "ERROR: Please choose numSensIter greater than 0."
            raise Exception()
        check2 = int(parser.get('Sensitivity','sensBatchNum'))
        if check2 <= 0:
            print "ERROR: Please choose sensBatchNum greater than 0."
            raise Exception()
        bDate = parser.get('Sensitivity','bSensDate')
        eDate = parser.get('Sensitivity','eSensDate')
        bEDate = parser.get('Sensitivity','bSensEvalDate')
        bDate = datetime.datetime.strptime(str(bDate),'%Y-%m-%d')
        eDate = datetime.datetime.strptime(str(eDate),'%Y-%m-%d')
        bEDate = datetime.datetime.strptime(str(bEDate),'%Y-%m-%d')
        if bDate >= eDate:
            print "ERROR: Must specify ending sensitivity date greater than beginning sensitivity date."
            raise Exception()
        #if bEDate >= bDate:
        #    print "ERROR: Must specify the beginning date for sensitivity evaluation date " + \
        #          " that is before the ending date for sensitivity simulation."
        #    raise Exception()
        if bEDate >= eDate:
            print "ERROR: Must specify the beginning date for sensitivity evaluation date " + \
                  " that is before the ending date for validation simulations."
            raise Exception()
    
    # Check gauge information
    check1 = str(parser.get('gageInfo','gageListFile'))
    check2 = str(parser.get('gageInfo','gageListSQL'))
    if len(check1) == 0 and len(check2) == 0:
        print "ERROR: Zero length gage list file and SQL command passed to program."
        raise Exception()
    if len(check1) > 0 and len(check2) > 0:
        print "ERROR: Cannot have both gage list and SQL command."
        raise Exception()
    if len(check1) > 0:
        if not os.path.isfile(check1):
            print "ERROR: File: " + check2 + " not found."
            raise Exception()
        
    # Check LSM physics options
    check = int(parser.get('lsmPhysics','dynVegOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid dynamic vegetation option chosen."
        raise Exception()
    
    check = int(parser.get('lsmPhysics','canStomResOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid canopy stomatal resistance option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','btrOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid BTR option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','runoffOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid LSM runoff option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','sfcDragOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid surface drag coefficient option chosen."
        raise Exception()
    
    check = int(parser.get('lsmPhysics','frzSoilOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid frozen soil option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','supCoolOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid supercooled water option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','radTransferOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid radiative transfer option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','snAlbOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid snow albedo option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','pcpPartOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid precip partitioning option chosen."
        raise Exception()
    
    check = int(parser.get('lsmPhysics','tbotOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid TBOT option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','tempTimeSchOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid temperature time scheme option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','sfcResOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid surface resistence option chosen."
        raise Exception()
        
    check = int(parser.get('lsmPhysics','glacierOption'))
    if check < 0 or check > 4:
        print "ERROR: Invalid glacier option chosen."
        raise Exception()
    
    # Check soil moisture thickness values
    check = ast.literal_eval(parser.get('lsmPhysics','soilThick'))
    if len(check) != 4:
        print "ERROR: Must specify four soil layer thicknesses."
        raise Exception()
        
    # Check z-level
    check = parser.get('lsmPhysics','zLvl')
    if float(check) < 0:
        print "ERROR: zLvl must be greater than or equal to 0.0 meters."
        raise Exception()
    if len(check) == 0:
        print "ERROR: Zero length zLvl passed to program."
        raise Exception()
        
    # Check forcing options
    check = parser.get('forcing','forceType')
    if len(check) == 0:
        print "ERROR: Zero length forceType value passed to program."
        raise Exception()
    if int(check) < 1 or int(check) > 6:
        print "ERROR: Invalid forceType value passed to program."
        raise Exception()
        
    # Make sure output frequencies aren't < 0
    check = int(parser.get('modelTime','forceDt'))
    if check < 0:
        print "ERROR: Invalid forcing DT passed to program."
        raise Exception()
        
    check = int(parser.get('modelTime','lsmDt'))
    if check < 0:
        print "ERROR: Invalid LSM DT passed to program."
        raise Exception()
        
    check = int(parser.get('modelTime','lsmOutDt'))
    if check < 0:
        print "ERROR: Invalid LSM Output DT passed to program."
        raise Exception()
        
    check = int(parser.get('modelTime','lsmRstFreq'))
    if check < 0:
        print "ERROR: Invalid LSM restart frequency passed to program."
        raise Exception()
        
    check = int(parser.get('modelTime','hydroRstFreq'))
    if check < 0:
        print "ERROR: Invalid Hydro restart frequency passed to program."
        raise Exception()
        
    check = int(parser.get('modelTime','hydroOutDt'))
    if check < 0:
        print "ERROR: Invalid Hydro output DT passed to program."
        raise Exception()
        
    # Check Hydro IO options
    check = parser.get('hydroIO','rstType')
    if len(check) == 0:
        print "ERROR: Zero length rstType passed to program."
        raise Exception()
    if int(check) < 0 or int(check) > 1:
        print "ERROR: Invalid rstType passed to program."
        raise Exception()
        
    check = parser.get('hydroIO','ioConfigOutputs')
    if len(check) == 0:
        print "ERROR: Zero length ioConfigOutputs passed to program."
        raise Exception()
    if int(check) < 0 or int(check) > 6:
        print "ERROR: Invalid ioConfigOutputs passed to program."
        raise Exception()
        
    check = parser.get('hydroIO','ioFormOutputs')
    if len(check) == 0:
        print "ERROR: Zero length ioFormOutputs passed to program."
        raise Exception()
    if int(check) < 0 or int(check) > 4:
        print "ERROR: Invalid ioFormOutputs passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','chrtoutDomain'))
    if check < 0 or check > 1:
        print "ERROR: Invalid CHRTOUT_DOMAIN option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','chanObsDomain'))
    if check < 0 or check > 1:
        print "ERROR: Invalid CHANOBS_DOMAIN optino passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','chrtoutGrid'))
    if check < 0 or check > 1:
        print "ERROR: Invalid CHRTOUT_GRID option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','lsmDomain'))
    if check < 0 or check > 1:
        print "ERROR: Invalid LSMOUT_DOMAIN option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','rtoutDomain'))
    if check < 0 or check > 1:
        print "ERROR: Invalid RTOUT_DOMAIN option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','gwOut'))
    if check < 0 or check > 1:
        print "ERROR: Invalid GW_OUT option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','lakeOut'))
    if check < 0 or check > 1:
        print "ERROR: Invalid LAKE_OUT option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','frxstOut'))
    if check < 0 or check > 1:
        print "ERROR: Invalid frxstOut option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','resetHydroAcc'))
    if check < 0 or check > 1:
        print "ERROR: Invalid RSTRT_SWC option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroIO','streamOrderOut'))
    if check < 0 or check > 4:
        print "ERROR: Invalid stream order output option passed to program."
        raise Exception()
        
    # Check hydro physics options
    check = int(parser.get('hydroPhysics','dtChSec'))
    if check < 0:
        print "ERROR: Invalid DTRT_CH option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroPhysics','dtTerSec'))
    if check < 0:
        print "ERROR: Invalid DTRT_TER option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroPhysics','subRouting'))
    if check < 0 or check > 1:
        print "ERROR: Invalid sub-surface routing switch passed to program."
        raise Exception()
        
    check = int(parser.get('hydroPhysics','ovrRouting'))
    if check < 0 or check > 1:
        print "ERROR: Invalid overland routing switch passed to program."
        raise Exception()
        
    check = int(parser.get('hydroPhysics','channelRouting'))
    if check < 0 or check > 1:
        print "ERROR: Invalid channel routing switch passed to program."
        raise Exception()
        
    check = int(parser.get('hydroPhysics','rtOpt'))
    if check < 0 or check > 2:
        print "ERROR: Invalid overland/subsurface routing option passed to program."
        raise Exception()
    
    check = int(parser.get('hydroPhysics','chanRtOpt'))
    if check < 0 or check > 3:
        print "ERROR: Invalid channel routing option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroPhysics','udmpOpt'))
    if check < 0 or check > 1:
        print "ERROR: Invalid user-defined mapping option passed to program."
        raise Exception()
        
    check = int(parser.get('hydroPhysics','gwBaseSw'))
    if check < 0 or check > 1:
        print "ERROR: Invalid groundwater bucket switch passed to program."
        raise Exception()
    
    check = int(parser.get('hydroPhysics','gwRestart'))
    if check < 0 or check > 1:
        print "ERROR: Invalid ground water restart switch passed to program."
        raise Exception()
        
    
    