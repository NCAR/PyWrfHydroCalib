# Main calling program to initiate a job for calibration.
# The user will need to correctly fill out the setup.parm
# file before running this program. Options will be read in
# along with the list of gages to calibtrate. Additionally,
# the user will need to provide a table of parameters being
# calibrated, and their value ragnes. Directories
# for running the calibration will be setup, along with 
# entries into the calibration database to initialize 
# the workflow.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu
# 303-497-2693

import sys
import argparse
import os

from core import configMod
from core import calibIoMod
from core import errMod
from core import dbMod

# Set the Python path to include package specific functions included with this
# package.
prPath = os.path.realpath(__file__)
pathSplit = prPath.split('/')
libPath = '/'
for j in range(1,len(pathSplit)-1):
    libPath = libPath + pathSplit[j] + '/'
topDir = libPath
libPathTop = libPath + 'core'

#import warnings
#warnings.filterwarnings("ignore")

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Main program to initialize ' + \
             'calibration for the National Water Model')
    parser.add_argument('configFile',metavar='config',type=str,nargs='+',
                        help='Config file to initialize job.')
    parser.add_argument('--optDbPath',type=str,nargs='?',
                        help='Optional alternative path to SQLite DB file.')
    parser.add_argument('--optExpID',type=int,nargs='?',
                        help='Optional user-defined experiment ID for this calibration.')
            
    args = parser.parse_args()       

    # If the SQLite file does not exist, throw an error.
    if args.optDbPath is not None:
        if not os.path.isfile(args.optDbPath):
            print("ERROR: " + args.optDbPath + " Does Not Exist.")
            sys.exit(1)
        else:
            dbPath = args.optDbPath
    else:
        dbPath = topDir + "wrfHydroCalib.db"
        if not os.path.isfile(dbPath):
            print("ERROR: SQLite3 DB file: " + dbPath + " Does Not Exist.")
            sys.exit(1)

    # If the user passed an optional experiment ID to use, check to make sure it's a
    # non-zero number.
    if args.optExpID is not None:
        if args.optExpID <= 0:
            print("ERROR: Please specify an optional experiment ID that is non-zero.")
            sys.exit(1)
        else:
            optExpId = args.optExpID
    else:
        optExpId = -9999

    # Initialize job using setup.parm and calibration DB.
    try:
         jobData = configMod.createJob(args)
    except:
        print("ERROR: Failure to initialize calibration workflow job.")
        sys.exit(1)
        
    jobData.dbPath = dbPath
        
    # Establish database connection.
    db = dbMod.Database(jobData)
    try:
        db.connect(jobData)
    except:
        errMod.errOut(jobData)
        
    # First check to see if unique Job ID already exists.
    try:
        db.getJobID(jobData)
    except:
        errMod.errOut(jobData)
        
    # If a job ID value was found, this means information from this configuration
    # file has already been initiated by the workflow into the database. 
    if int(jobData.jobID) != -9999:
        jobData.errMsg = "ERROR: Information for this job has already " + \
                         "been entered as job ID: " + str(jobData.jobID)
        errMod.errOut(jobData)
        
    # Extract list of gages to perform workflow on
    try:
        calibIoMod.getGageList(jobData,db)
    except:
        errMod.errOut(jobData)
        
    # Check to see if this job ID contains any entries in other tables. If it does,
    # Warn the user that this data will be wiped, and prompt the user to confirm
    # they want to delete the data from the other tables. 
    try:
        statusTmp = db.checkPreviousEntries(jobData)
    except:
        errMod.errOut(jobData)

    # If any entries in the tables were found, warn the user that tables from an
    # orphaned ghost job are being deleted. This may be a situation where a previous 
    # job was ran in the DB, it was removed from Job_Meta, but the remaining tables
    # weren't cleaned up.
    if not statusTmp:
        print("WARNING: Old orphaned table entries from this jobID are being deleted.")
        try:
            db.cleanupJob(jobData)
        except:
            errMod.errOut(jobData)
        
    # Create DB entries for job name
    try:
        db.enterJobID(jobData,optExpId)
    except:
        errMod.errOut(jobData)
        
    # Pull Job ID from newly created job. Will be used for calibration 
    # parameter DB entries
    try:
        db.getJobID(jobData)
    except:
        errMod.errOut(jobData)
    
    # Create necessary run directories to hold output, analysis, etc.
    try:
        calibIoMod.setupModels(jobData,db,args,libPathTop)
    except:
        errMod.errOut(jobData)
        
    # Create DB entries to log the parameters being calibrated.
    try:
        db.enterJobParms(jobData)
    except:
        errMod.errOut(jobData)
        
    # Create empty table to hold calibrated parameter values that will be 
    # calculated during calibration.
    try:
        db.populateParmTable(jobData)
    except:
        errMod.errOut(jobData)
 
    jobData.nGages = len(jobData.gages) 
    try:
        jobData.checkGages2(db)
    except:
        errMod.errOut(jobData)
     
    # Create empty table entries into the Calib_Stats/Sens_Stats tables to be filled in as the workflow progresses.
    # If table entries have already been entered, continue on. This only needs to be done ONCE. Moved this
    # from calib.py as there's no reason to do this during the spinup program.
    for basin in range(0,len(jobData.gages)):
        domainID = jobData.gageIDs[basin]
        
        if domainID == -9999:
            jobData.errMsg = "ERROR: Unable to locate domainID for gage: " + str(jobData.gages[basin])
            errMod.errOut(jobData)

        if jobData.calibFlag == 1:
            try:
                db.populateCalibTable(jobData,domainID,str(jobData.gages[basin]))
            except:
                errMod.errOut(jobData)
        
        if jobData.sensFlag == 1:
            try:
                db.populateSensTable(jobData,domainID,str(jobData.gages[basin]))
            except:
                errMod.errOut(jobData)
    
    # Disconnect from the calibration database.
    try:
        db.disconnect(jobData)
    except:
        errMod.errOut(jobData)
        
    # Print the newly created job ID to the user
    jobData.genMsg = "WORKFLOW HAS BEEN SETUP FOR OWNER: " + str(jobData.owner) + \
                     " JOB ID = " + str(jobData.jobID)
    print(jobData.genMsg)
    errMod.sendMsg(jobData)
        
if __name__ == "__main__":
    main(sys.argv[1:])
