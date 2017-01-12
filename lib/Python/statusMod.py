# Module file containing objects and functions for handling interactions
# with the database on workflow monitoring and status updates. 

# Logan Karsten
# National Center for Atmospheric Research

from glob import glob

class statusMeta:
    def __init__(self):
        # Initialize empty object containing variables.
        self.jobID = []
        self.jobDir = []
        self.nGages = []
        self.gages = []
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
    def checkGages(self):
        # Function to check number of gages in output directory.
        subDirs = glob(self.jobDir)
        
        if len(subDirs) != int(self.nGages):
            self.errMsg = "ERROR: Number of gages sub-directories does not match entry in database."
            raise
            
        gagesTmp = []
            
        # Walk job directory and extract gages.
        for subDir in range(0,len(subDirs)):
            pthTmp = subDirs[subDir]
            strArr = pthTmp.split('/')
            lenTmp = len(strArr)
            gagesTmp.append(strArr[lenTmp-2])
            
        self.gages = gagesTmp[:]