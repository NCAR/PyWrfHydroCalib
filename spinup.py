# Main calling program to initiate a spinup for calibration of the WRF-Hydro. 
# This program can either be used to initiate or restart a 
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
import pandas as pd
import pwd
import numpy as np
import datetime

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

import statusMod
import dbMod
import errMod
import spinupMod
import configMod
import time

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Main program to start or restart ' + \
             'calibration spinup for the National Water Model')
    parser.add_argument('jobID',metavar='jobID',type=str,nargs='+',
                        help='Job ID specific to calibration spinup.')
    parser.add_argument('--hostname',type=str,nargs='?',
                        help='Optional hostname MySQL DB resides on. Will use localhost if not passed.')
    parser.add_argument('--portNumber',type=int,nargs='?',
                        help='Optional port number to connect. Default is 5432.')
    parser.add_argument('--pwd',metavar='pwd',type=str,nargs='?',help='Password to the Database.')
    
    args = parser.parse_args()
    
    # Establish the beginning timestamp for this program.
    begTimeStamp = datetime.datetime.now()
    
    # Get current user who is running this program.
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    # Initialize object to hold status and job information
    jobData = statusMod.statusMeta()
    jobData.jobID = int(args.jobID[0])
    
    # Lookup database username/login credentials based on username
    # running program.
    if not args.pwd:
        try:
            pwdTmp = getpass.getpass('Enter Database Password: ')
            jobData.dbPwd = str(pwdTmp)
        except:
            print "ERROR: Unable to authenticate credentials for database."
            sys.exit(1)
    else:
        jobData.dbPwd = args.pwd
    
    jobData.dbUName = 'WH_Calib_rw'
    
    if not args.hostname:
        # We will assume localhost for Postgres DB
        hostTmp = 'localhost'
    else:
        hostTmp = str(args.hostname)
        
    if not args.portNumber:
        # We will default to 5432
        portTmp = '5432'
    else:
        portTmp = str(args.portNumber)
    jobData.port = portTmp
    jobData.host = hostTmp
    
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
    
    # Assign the SQL command from the config file into the jobData structure
    jobData.gSQL = staticData.gSQL
        
    # Check gages in directory to match what's in the database
    try:
        jobData.checkGages2(db)
    except:
        errMod.errOut(jobData)
    
    # Establish LOCK file to secure this Python program to make sure
    # no other instances over-step here. This is mostly designed to deal
    # with nohup processes being kicked off Yellowstone/Cheyenne/Crontabs arbitrarily.
    # Just another check/balance here.
    pyLockPath = str(jobData.jobDir) + "/PYTHON.LOCK"
    if os.path.isfile(pyLockPath):
        # Either a job is still running, or was running
        # and was killed.

        print 'LOCK FILE FOUND.'
        # Read in to get PID number
        pidObj = pd.read_csv(pyLockPath)
        pidCheck = int(pidObj.PID[0])
        if errMod.check_pid(pidCheck):
                print "JOB: " + str(pidCheck) + \
                      " Is still running."
                sys.exit(0)
        else:
                print "JOB: " + str(pidCheck) + \
                      " Has Failed. Removing LOCK " + \
                      " file."
                os.remove(pyLockPath)
                fileObj = open(pyLockPath,'w')
                fileObj.write('\"PID\"\n')
                fileObj.write(str(os.getpid()))
                fileObj.close()
        # TEMPORARY FOR CHEYENNE. Since all cron jobs are launched
        # from an administrative node, we cannot monitor the process at 
        # all, which is an inconvenience. So.... we will check the last
        # modified time. If it's more than 15 minutes old, we will assume
        # the process is no longer running and can continue on with calibration.
        #dtRunCheck = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(pyLockPath))
        #if dtRunCheck.seconds/60.0 < 15.0:
        #    # We are going to assume a previous process is still running on the system. 
        #    # exit gracefully.
        #    print 'ASSUMING PROCESS STILL RUNNING'
        #    sys.exit(0)
        #else:
        #    # We are assuming the process is no longer running on the system. Alow
        #    # the workflow to continue. 
        #    print 'ALLOWING WORKFLOW TO CONINUE. REMOVING LOCK FILE'
        #    os.remove(pyLockPath)
        #    fileObj = open(pyLockPath,'w')
        #    fileObj.write('\"PID\"\n')
        #    fileObj.write(str(os.getpid()))
        #    fileObj.close()
    else:
        # Write a LOCK file for this program.
        fileObj = open(pyLockPath,'w')
        fileObj.write('\"PID\"\n')
        fileObj.write(str(os.getpid()))
        fileObj.close()
    
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
        #strTmp = "Please enter Slack channel:"
        #newSlackChannel = raw_input(strTmp)
        #strTmp = "Please enter Slack token:"
        #newSlackToken = raw_input(strTmp)
        #strTmp = "Please enter Slack user name:"
        #newSlackUName = raw_input(strTmp)
        changeFlag = 1
        #if len(newSlackChannel) != 0 and len(newSlackToken) == 0:
        #    print "ERROR: You must specify an associated Slacker API token."
        #    sys.exit(1)
        #if len(newSlackChannel) != 0 and len(newSlackUName) == 0:
        #    print "ERROR: You must specify an associated Slacker user name."
        #    sys.exit(1)
        #if len(newSlackToken) != 0 and len(newSlackChannel) == 0:
        #    print "ERROR: You must specify an associated Slacker channel name."
        #    sys.exit(1)
        #if len(newSlackToken) != 0 and len(newSlackUName) == 0:
        #    print "ERROR: You must specify an associated Slacker user name."
        #    sys.exit(1)
        #if len(newSlackUName) != 0 and len(newSlackChannel) == 0:
        #    print "ERROR: You must specify an associated Slacker channel name."
        #    sys.exit(1)
        #if len(newSlackUName) != 0 and len(newSlackToken) == 0:
        #    print "ERROR: You must specify an associated Slacker API token."
        #    sys.exit(1)
        #if len(newSlackChannel) != 0 and len(newEmail) != 0:
        #    print "ERROR: You cannot specify both email and Slack for notifications."
        #    sys.exit(1)
        #if len(newSlackChannel) == 0 and len(newEmail) == 0:
        #    changeFlag = 0
            
        # PLACEHOLDER FOR CHECKING SLACK CREDENTIALS
        
        # TEMPORARY FOR VERSION 1.2 NWM CALIBRATION!!!!
        # If a new owner takes over, simply change the owner, but keep all 
        # other contact information the same.
        newEmail = jobData.email
        newSlackChannel = jobData.slChan
        newSlackToken = jobData.slToken
        newSlackUName = jobData.slUser
        if not newEmail:
            newEmail = ''
        if not newSlackChannel:
            newSlackChannel = ''
            newSlackToken = ''
            newSlackUName = ''
            
        try:
            db.updateJobOwner(jobData,userTmp,newEmail,newSlackChannel,newSlackToken,newSlackUName,changeFlag)
        except:
            errMod.errOut(jobData)
            
        jobData.genMsg = "MSG: User: " + userTmp + " Is Taking Over JobID: " + str(jobData.jobID) + \
                         " From Owner: " + str(jobData.owner)
        errMod.sendMsg(jobData)
        
    # Begin an "infinite" do loop. This loop will continue to loop through all 
    # the basins until spinups are complete. Basins are allowed ONE failure. A restart
    # will be attempted. If the restart fails again, a LOCK file is placed into the
    # run directory and an error email is sent to the user.
    completeStatus = False
    
    # Create a "key" array. This array is of length [numBasins] and is initialized to 0.0.
    # Each array element can have the following values based on current model status:
    # 0.0 - Initial value
    # 0.5 - Model simulation in progress
    # 1.0 - Model simulation complete
    # -0.5 - Model simulation failed once and a restart has been attempted
    # -1.0 - Model has failed twice. A LOCK file has been created.
    # Once all array elements are 1.0, then completeStatus goes to True, an entry into
    # the database occurs, and the program will complete.
    keySlot = np.empty(len(jobData.gages))
    keySlot[:] = 0.0
    entryValue = float(len(jobData.gages))
    
    # Create an array to hold systme job ID values. This will only be used for
    # PBS as qstat has demonstrated slow behavior when doing a full qstat command. 
    # We will track job ID values and do a qstat <jobID> and populate this array
    # to keep track of things. 
    pbsJobId = np.empty([len(jobData.gages)],np.int64)
    pbsJobId[:] = -9999

    while not completeStatus:
        # Walk through spinup directory for each basin. Determine the status of
        # the model runs by the files available. If restarting, modify the 
        # namelist files appropriately. Then, restart the model. Once all
        # basins have been accounted for, fire off the monitoring program through
        # nohup to keep track of the models. If anything goes wrong, notifications
        # will either be emailed per the user's info, or piped to Slack for group
        # notification.
        # Loop through each basin. Perform the following steps:
        # 1.) If status is -0.5,0.0, or 0.5, check to see if the model is running
        #     for this basin.
        # 2.) If the model is not running, check for expected output and perform
        #     necessary logistics. Continue to the next basin.
        # If the status goes to -1.0, a LOCK file is created and must be manually
        # removed from the user. Once the program detects this, it will restart the
        # model and the status goes back to 0.5.
        # If the status is -0.5 and no job is running, output must be complete, or 
        # status goes to -1.0.
        # If output is not complete, the model is still running, status stays at 0.5.
        # If job is not running, and output has been completed, status goes to 1.0.
        # This continues indefinitely until statuses for ALL basins go to 1.0.
        for basin in range(0,len(jobData.gages)):
            try:
                spinupMod.runModel(jobData,staticData,db,jobData.gageIDs[basin],jobData.gages[basin],keySlot,basin,pbsJobId)
            except:
                errMod.errOut(jobData)
            
            # TEMPORARY FOR CHEYENNE
            # Check to make sure program hasn't passed a prescribed time limit. If it has,
            # exit gracefully.
            #timeCheckStamp = datetime.datetime.now()
            #programDtCheck = timeCheckStamp - begTimeStamp
            #if programDtCheck.seconds/60.0 > 90.0: 
            #    # 90-minutes)
            #    try:
            #        fileObj = open(pyLockPath,'a')
            #        fileObj.write('WORKFLOW HAS HIT TIME LIMIT - EXITING....\n')
            #        fileObj.close()
            #    except:
            #        jobData.errMsg = "ERROR: Unable to update workflow LOCK file: " + pyLockPath
            #        errMod.errOut(jobData)
        
        # Check to see if program requirements have been met.
        if keySlot.sum() == entryValue:
            jobData.spinComplete = 1
            try:
                db.updateSpinupStatus(jobData)
            except:
                errMod.errout(jobData)
            jobData.genMsg = "SPINUP FOR JOB ID: " + str(jobData.jobID) + " COMPLETE."
            errMod.sendMsg(jobData)
            completeStatus = True
            
        # Open the Python LOCK file. Write a blank line to the file and close it.
        # This action will simply modify the file modification time while only adding
        # a blank line.
        try:
            fileObj = open(pyLockPath,'a')
            fileObj.write('\n')
            fileObj.close()
        except:
            jobData.errMsg = "ERROR: Unable to update workflow LOCK file: " + pyLockPath
            errMod.errOut(jobData)
            
    # Remove LOCK file
    os.remove(pyLockPath)
    
if __name__ == "__main__":
    main(sys.argv[1:])