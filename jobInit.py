# Main calling program to initiate a job for calibration.
# The user will need to correctly fill out the setup.parm
# file before running this program. Options will be read in
# along with the list of gages to calibtrate. Directories
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
#import getpass

# Set the Python path to include package specific functions.
prPath = os.path.realpath(__file__)
pathSplit = prPath.split('/')
libPath = '/'
for j in range(1,len(pathSplit)-1):
    libPath = libPath + pathSplit[j] + '/'
libPath = libPath + 'lib/Python'
sys.path.insert(0,libPath)

import warnings
warnings.filterwarnings("ignore")

import configMod
import calibIoMod
import errMod
import dbMod

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Main program to initialize ' + \
             'calibration for the National Water Model')
    parser.add_argument('configFile',metavar='config',type=str,nargs='+',
                        help='Config file to initialize job.')
    parser.add_argument('parmTbl',metavar='parmTbl',type=str,nargs='+',
                        help='Calibration Parameter Table.')
            
    args = parser.parse_args()            

    # Initialize job using setup.parm and calibration DB.
    try:
         jobData = configMod.createJob(args)
    except:
        print "ERROR: Failure to initialize calibration workflow job."
        sys.exit(1)
        
    # Lookup database username/login credentials based on username
    # running program.
    #try:
    #    uNameTmp = raw_input('Enter Database Username: ')
    #    pwdTmp = getpass.getpass('Enter Database Password: ')
    #    jobData.dbUName= str(uNameTmp)
    #    jobData.dbPwd = str(pwdTmp)
    #except:
    #    print "ERROR: Unable to authenticate credentials for database."
    #    sys.exit(1)
    
    jobData.dbUName = 'NWM_Calib_rw'
    jobData.dbPwd = 'IJustWannaCalibrate'    
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
        
    # Create necessary run directories to hold output, analysis, etc.
    try:
        calibIoMod.setupModels(jobData,db,args)
    except:
        errMod.errOut(jobData)
       
    # Create DB entries for job name
    try:
        db.enterJobID(jobData)
    except:
        errMod.errOut(jobData)
        
    # Pull Job ID from newly created job. Will be used for calibration 
    # parameter DB entries
    try:
        db.getJobID(jobData)
    except:
        errMod.errOut(jobData)
        
    # Create DB entries to log the parameters being calibrated.
    try:
        db.enterCalibParms(jobData,str(args.parmTbl[0]))
    except:
        errMod.errOut(jobData)
        
    # Create empty table to hold calibrated parameter values that will be 
    # calculated during calibration.
    try:
        db.populateParmTable(jobData,str(args.parmTbl[0]))
    except:
        errMod.errOut(jobData)
    
    # Disconnect from the calibration database.
    try:
        db.disconnect(jobData)
    except:
        errMod.errOut(jobData)
        
if __name__ == "__main__":
    main(sys.argv[1:])