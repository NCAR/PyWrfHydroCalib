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
import getpass

# Set the Python path to include package specific functions.
sys.path.insert(0,'./lib/Python')

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
            
    args = parser.parse_args()            

    # Initialize job using setup.parm and calibration DB.
    try:
        jobData = configMod.createJob(args)
    except:
        print "ERROR: Failure to initialize calibration workflow job."
        sys.exit(1)
        
    # Lookup database username/login credentials based on username
    # running program.
    uNameTmp = raw_input('Enter Database Username: ')
    pwdTmp = getpass.getpass('Enter Database Password: ')
    try:
        uNameTmp = raw_input('Enter Database Username: ')
        pwdTmp = getpass.getpass('Enter Database Password: ')
        jobData.dbUName= str(uNameTmp)
        jobData.dbPwd = str(pwdTmp)
    except:
        print "ERROR: Unable to authenticate credentials for database."
        sys.exit(1)
        
    # First check to see if unique Job ID already exists. 
    # PLACEHOLDER FOR CHECKING DB TABLES TO ENSURE JOB NAME HASN'T 
    # ALREADY BEEN ENTERED INTO DB
        
    # Extract list of gages to perform workflow on
    try:
        calibIoMod.getGageList(jobData)
    except:
        errMod.errOut(jobData)
        
    # Create necessary run directories to hold output, analysis, etc.
    try:
        calibIoMod.setupModels(jobData)
    except:
        errMod.errOut(jobData)
        
    # Create DB entries for job name
    # PLACEHOLDER FOR ENTERING DB INFORMATION    
        
if __name__ == "__main__":
    main(sys.argv[1:])