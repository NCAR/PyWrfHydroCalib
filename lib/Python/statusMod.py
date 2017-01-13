# Module file containing objects and functions for handling interactions
# with the database on workflow monitoring and status updates. 

# Logan Karsten
# National Center for Atmospheric Research

from glob import glob
import os

class statusMeta:
    def __init__(self):
        # Initialize empty object containing variables.
        self.jobID = []
        self.jobDir = []
        self.nGages = []
        self.gages = []
        self.gageIDs = []
        self.bSpinDate = []
        self.eSpinDate = []
        self.spinComplete = []
        self.bCalibDate = []
        self.eCalibDate = []
        self.calibIter = []
        self.calibComplete = []
        self.bValidDate = []
        self.eValidDate = []
        self.validComplete = []
        self.nIter = []
        self.nCores = []
        self.acctKey = []
        self.exe = []
        self.errMsg = []
        self.dbUName = []
        self.dbPwd = []
        self.owner = []
        self.email = None
        self.slChan = None
        self.slToken = None
        self.slUser = None
        self.slackObj = None
    def checkGages(self,db):
        # Function to check number of gages in output directory. Function
        # also calls the database module to extract unique ID values for each
        # gage.
        subDirs = glob(self.jobDir + '/*/')
        
        if len(subDirs) != int(self.nGages):
            self.errMsg = "ERROR: Number of gages sub-directories does not match entry in database."
            raise Exception()
            
        gagesTmp = []
        gageIDsTmp = []
            
        # Walk job directory and extract gages.
        for subDir in range(0,len(subDirs)):
            pthTmp = subDirs[subDir]
            strArr = pthTmp.split('/')
            lenTmp = len(strArr)
            gagesTmp.append(strArr[lenTmp-2])
            gageID = db.getDomainID(self,strArr[lenTmp-2])
            gageIDsTmp.append(gageID)
            
        self.gages = gagesTmp[:]
        self.gageIDs = gageIDsTmp[:]
        print self.gageIDs
        
