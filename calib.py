# Main calling program to initiate a calibration of the National
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
#import getpass
import os
#import subprocess
#import pandas as pd
import pwd
import numpy as np

# Set the Python path to include package specific functions.
sys.path.insert(0,'./lib/Python')

import warnings
warnings.filterwarnings("ignore")

import statusMod
import dbMod
import errMod
import calibIoMod
import runMod
import configMod

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Main program to start or restart ' + \
             'calibration for the National Water Model')
    parser.add_argument('jobID',metavar='jobID',type=str,nargs='+',
                        help='Job ID specific to calibration spinup.')
    
    args = parser.parse_args()
    
    # Get current user who is running this program.
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    # Initialize object to hold status and job information
    jobData = statusMod.statusMeta()
    jobData.jobID = int(args.jobID[0])
    
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
    
    # Pull extensive meta-data describing the job from the config file.
    configPath = str(jobData.jobDir) + "/setup.config"
    if not os.path.isfile(configPath):
        print "ERROR: Configuration file: " + configPath + " not found."
        sys.exit(1)

    try:        
        staticData = configMod.readConfig(configPath)
    except:
        print "ERROR: Failure to read configuration file: " + configPath
        sys.exit(1)
        
    # Establish database connection.
    db = dbMod.Database(jobData)
    try:
        db.connect(jobData)
    except:
        print jobData.errMsg
        sys.exit(1)
        
    # Extract job data from database
    try:
        db.jobStatus(jobData)
    except:
        print jobData.errMsg
        sys.exit(1)
    
    # Check gages in directory to match what's in the database
    try:
        jobData.checkGages(db)
    except:
        errMod.errOut(jobData)
    
    # Extract active jobs for job owner
    try:
        statusMod.checkYsJobs(jobData)
    except:
        errMod.errOut(jobData)
        
    # Some house keeping here. If the spinup is already complete, throw an error. 
    # also, if this is a re-initiation under a different user, require the new
    # user to enter a new contact that will be unpdated in the database. 
    if int(jobData.spinComplete) == 1:
        jobData.errMsg = "ERROR: Spinup for job ID: " + str(jobData.jobID) + \
                         " has already completed."
        errMod.errOut(jobData)
        
    if userTmp != jobData.owner:
        print "User: " + userTmp + " is requesting to takeover jobID: " + \
              str(jobData.jobID) + " from owner: " + str(jobData.owner)
        strTmp = "Please enter new email address. Leave blank if no email " + \
                 "change is desired. NOTE if you leave both email and Slack " + \
                 "information blank, no change in contact will occur. Only " + \
                 "the owner will be modified:"
        newEmail = raw_input(strTmp)
        strTmp = "Please enter Slack channel:"
        newSlackChannel = raw_input(strTmp)
        strTmp = "Please enter Slack token:"
        newSlackToken = raw_input(strTmp)
        strTmp = "Please enter Slack user name:"
        newSlackUName = raw_input(strTmp)
        changeFlag = 1
        if len(newSlackChannel) != 0 and len(newSlackToken) == 0:
            print "ERROR: You must specify an associated Slacker API token."
            sys.exit(1)
        if len(newSlackChannel) != 0 and len(newSlackUName) == 0:
            print "ERROR: You must specify an associated Slacker user name."
            sys.exit(1)
        if len(newSlackToken) != 0 and len(newSlackChannel) == 0:
            print "ERROR: You must specify an associated Slacker channel name."
            sys.exit(1)
        if len(newSlackToken) != 0 and len(newSlackUName) == 0:
            print "ERROR: You must specify an associated Slacker user name."
            sys.exit(1)
        if len(newSlackUName) != 0 and len(newSlackChannel) == 0:
            print "ERROR: You must specify an associated Slacker channel name."
            sys.exit(1)
        if len(newSlackUName) != 0 and len(newSlackToken) == 0:
            print "ERROR: You must specify an associated Slacker API token."
            sys.exit(1)
        if len(newSlackChannel) != 0 and len(newEmail) != 0:
            print "ERROR: You cannot specify both email and Slack for notifications."
            sys.exit(1)
        if len(newSlackChannel) == 0 and len(newEmail) == 0:
            changeFlag = 0
            
        # PLACEHOLDER FOR CHECKING SLACK CREDENTIALS
            
        try:
            db.updateJobOwner(jobData,userTmp,newEmail,newSlackChannel,newSlackToken,newSlackUName,changeFlag)
        except:
            errMod.errOut(jobData)
        
if __name__ == "__main__":
    main(sys.argv[1:])