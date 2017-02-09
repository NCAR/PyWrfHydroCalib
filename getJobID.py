# Utility program that will return a unique JobID to the user,
# provided a configuration file specific to the job.
# This is particularily useful when a new user needs to take over, or
# restart a job and doesn't know the JobID, which is needed to restart
# or get status updates on a job.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory.

import sys
import argparse

# Set the Python path to include package specific functions.
sys.path.insert(0,'./lib/Python')

import warnings
warnings.filterwarnings("ignore")

import configMod
import calibIoMod
import errMod
import dbMod

def main(argv):
    # Parse arguments. User must input a job name and directory.
    parser = argparse.ArgumentParser(description='Utility program to extract ' + \
             'calibration job ID')
    parser.add_argument('configFile',metavar='config',type=str,nargs='+',
                        help='Config file to initialize job.')
            
    args = parser.parse_args()
    
    # Initialize job using setup.parm and calibration DB.
    try:
        jobData = configMod.queryJob(args)
    except:
        print "ERROR: Failure to read in configuration file."
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
    
    jobData.dbUName = 'NWM_Calib'
    jobData.dbPwd = 'CalibrateGoodTimes'    
    # Establish database connection.
    db = dbMod.Database(jobData)
    try:
        db.connect(jobData)
    except:
        print "ERROR: Unable to authenticate credentials for database."
        sys.exit(1)
        
    # First check to see if unique Job ID already exists. 
    try:
        db.getJobID(jobData)
    except:
        print "ERROR: Failure to extract unique Job ID for job."
        sys.exit(1)
        
    if int(jobData.jobID) == -9999:
        print "This job has not been entered into the workflow database."
    else:
        print "JOB ID = " + str(jobData.jobID)
        
if __name__ == "__main__":
    main(sys.argv[1:])