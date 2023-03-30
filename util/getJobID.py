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
import os
import getpass

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

from core import configMod
from core import dbMod

def main(argv):
    # Parse arguments. User must input a job name and directory.
    parser = argparse.ArgumentParser(description='Utility program to extract ' + \
             'calibration job ID')
    parser.add_argument('configFile',metavar='config',type=str,nargs='+',
                        help='Config file to initialize job.')
    parser.add_argument('inDB',metavar='inDB',type=str,nargs='+',
                        help='Required path to sqllite3 DB file.')
    #parser.add_argument('--pwd',metavar='pwd',type=str,nargs='?',help='Password to the Database.')
            
    args = parser.parse_args()
    
    # If the sqllite DB file does not exist, throw an error to the user.
    if not os.path.isfile(args.inDB[0]):
        print("ERROR: Unable to locate DB file: " + args.inDB[0])
        sys.exit(1)
        
    dbPath = args.inDB[0]
    
    # Initialize job using setup.parm and calibration DB.
    try:
        jobData = configMod.queryJob(args)
    except:
        print("ERROR: Failure to read in configuration file.")
        sys.exit(1)
        
    # Lookup database username/login credentials based on username
    # running program.
    #if not args.pwd:
    #    try:
    #        pwdTmp = getpass.getpass('Enter Database Password: ')
    #        jobData.dbPwd = str(pwdTmp)
    #    except:
    #        print "ERROR: Unable to authenticate credentials for database."
    #        sys.exit(1)
    #else:
    #    jobData.dbPwd = args.pwd
    
    #jobData.dbUName = 'WH_Calib_rw'
    # Establish database connection.
    db = dbMod.Database(jobData)
    try:
        db.connect(jobData)
    except:
        print("ERROR: Unable to authenticate credentials for database.")
        sys.exit(1)
        
    # First check to see if unique Job ID already exists. 
    try:
        db.getJobID(jobData)
    except:
        print("ERROR: Failure to extract unique Job ID for job.")
        sys.exit(1)
        
    if int(jobData.jobID) == -9999:
        print("This job has not been entered into the workflow database.")
    else:
        print("JOB ID = " + str(jobData.jobID))
        
if __name__ == "__main__":
    main(sys.argv[1:])