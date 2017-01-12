# Main calling program to initiate a spinup for calibration of the National
# Water Model. This program can either be used to initiate or restart a 
# spinup if it has crashed unexpectedly. The user will need to provide 
# a unique Job ID that is stored in the database.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu
# 303-497-2693

import sys
import argparse
import getpass
import os
import subprocess
import pandas as pd

# Set the Python path to include package specific functions.
sys.path.insert(0,'./lib/Python')

import warnings
warnings.filterwarnings("ignore")

import statusMod
import dbMod
import errMod

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Main program to start or restart ' + \
             'calibration spinup for the National Water Model')
    parser.add_argument('jobID',metavar='jobID',type=str,nargs='+',
                        help='Job ID specific to calibration spinup.')
    
    args = parser.parse_args()
    
    # Initialize object to hold status and job information
    jobData = statusMod.statusMeta()
    jobData.jobID = args.jobID
    
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
        print jobData.errMsg
        sys.exit(1)
        
    # Extract job data from database
    #db.jobStatus(jobData)
    try:
        db.jobStatus(jobData)
    except:
        print jobData.errMsg
        sys.exit(1)
    
    # Check gages in directory to match what's in the database
    jobData.checkGages()
    
    # Get unique PID.
    pidUnique = os.getpid()
    csvPath = jobData.jobDir + "/BJOBS_" + str(pidUnique) + ".csv"
    cmd = 'bjobs -u ' + str(jobData.owner) + ' -noheader > ' + csvPath
    subprocess.call(cmd,shell=True)
    colNames = ['JOBID','USER','STAT','QUEUE','FROM_HOST','EXEC_HOST','JOB_NAME',\
               'SUBMIT_MONTH','SUBMIT_DAY','SUBMIT_HHMM']
    jobs = pd.read_csv(csvPath,delim_whitespace=True,header=None,names=colNames)
    print jobs
    
    # Create unique CSV file that is a dump of current jobs being ran. 
    
if __name__ == "__main__":
    main(sys.argv[1:])