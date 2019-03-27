# Main calling program to initiate a calibration of the National
# Water Model. Please see comments below on steps involved for the workflow
# to run. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu
# 303-497-2693

import sys
import argparse
import os
import pwd
import numpy as np
import datetime
import time

from core import statusMod
from core import dbMod
from core import errMod
from core import configMod
from core import calibMod
import pandas as pd

# Set the Python path to include package specific functions.
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
    parser = argparse.ArgumentParser(description='Main program to start or restart ' + \
             'calibration for WRF-Hydro')
    parser.add_argument('jobID',metavar='jobID',type=str,nargs='+',
                        help='Job ID specific to calibration calibration.')
    parser.add_argument('groupNum',metavar='groupNum',type=str,nargs='+',
                        help='Group number associated with basins to calibrate.')
    parser.add_argument('--optDbPath',type=str,nargs='?',
                        help='Optional alternative path to SQLite DB file.')
    
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
    
    # Establish the beginning timestamp for this program.
    begTimeStamp = datetime.datetime.now()
    
    # Get current user who is running this program.
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    # Initialize object to hold status and job information
    jobData = statusMod.statusMeta()
    jobData.jobID = int(args.jobID[0])
    jobData.dbPath = dbPath
    
    # Establish database connection.
    db = dbMod.Database(jobData)
    try:
        db.connect(jobData)
    except:
        print(jobData.errMsg)
        sys.exit(1)
        
    # Extract job data from database
    try:
        db.jobStatus(jobData)
    except:
        print(jobData.errMsg)
        sys.exit(1)
        
    # If the calibration flag is 0, simply exit gracefully as the user specified
    # not to run calibration.
    if jobData.calibFlag != 1:
        print("ERROR: Calibration flag was set to 0 for this workflow.")
        sys.exit(1)
        
    # Establish LOCK file to secure this Python program to make sure
    # no other instances over-step here. This is mostly designed to deal
    # with nohup processes being kicked off Yellowstone/Cheyenne/Crontabs arbitrarily.
    # Just another check/balance here.
    #lockPath = str(jobData.jobDir) + "/PYTHON.LOCK"
    #if os.path.isfile(lockPath):
    #    # Either a job is still running, or was running
    #    # and was killed.

    #    print('LOCK FILE FOUND.')
    #    # Read in to get PID number
    #    pidObj = pd.read_csv(lockPath)
    #    pidCheck = int(pidObj.PID[0])
    #    if errMod.check_pid(pidCheck):
    #            print("JOB: " + str(pidCheck) + \
    #                  " Is still running.")
    #            sys.exit(0)
    #    else:
    #            print("JOB: " + str(pidCheck) + \
    #                  " Has Failed. Removing LOCK " + \
    #                  " file.")
    #            os.remove(lockPath)
    #            fileObj = open(lockPath,'w')
    #            fileObj.write('\"PID\"\n')
    #            fileObj.write(str(os.getpid()))
    #            fileObj.close()
    #    # TEMPORARY FOR CHEYENNE. Since all cron jobs are launched
    #    # from an administrative node, we cannot monitor the process at
    #    # all, which is an inconvenience. So.... we will check the last
    #    # modified time. If it's more than 30 minutes old, we will assume
    #    # the process is no longer running and can continue on with calibration.
    #    #dtRunCheck = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(lockPath))
    #    #if dtRunCheck.seconds/60.0 < 15.0:
    #    #    # We are going to assume a previous process is still running on the system.
    #    #    # exit gracefully.
    #    #    print 'ASSUMING PROCESS STILL RUNNING'
    #    #    sys.exit(0)
    #    #else:
    #    #    # We are assuming the process is no longer running on the system. Alow
    #    #    # the workflow to continue.
    #    #    print 'ALLOWING WORKFLOW TO CONINUE. REMOVING LOCK FILE'
    #    #    os.remove(lockPath)
    #    #    fileObj = open(lockPath,'w')
    #    #    fileObj.write('\"PID\"\n')
    #    #    fileObj.write(str(os.getpid()))
    #    #    fileObj.close()
    #else:
    #    print('LOCK FILE NOT FOUND.')
    #    # Write a LOCK file for this program.
    #    fileObj = open(lockPath,'w')
    #    fileObj.write('\"PID\"\n')
    #    fileObj.write(str(os.getpid()))
    #    fileObj.close()
        
    # Pull extensive meta-data describing the job from the config file.
    configPath = str(jobData.jobDir) + "/setup.config"
    if not os.path.isfile(configPath):
        print("ERROR: Configuration file: " + configPath + " not found.")
        sys.exit(1)
    try:        
        staticData = configMod.readConfig(configPath)
    except:
        print("ERROR: Failure to read configuration file: " + configPath)
        sys.exit(1)
        
    # Assign the SQL command from the config file into the jobData structure
    jobData.gSQL = staticData.gSQL
        
    # Check gages in directory to match what's in the database
    try:
        jobData.checkGages2(db)
    except:
        errMod.errOut(jobData)

    # Calculate the CPU/group layout for all basins.
    try:
        jobData.calcGroupNum()
    except:
        errMod.errOut(jobData)
    
    # Some house keeping here. If the calibration is already complete, throw an error. 
    # Also ensure the spinup has been entered as complete. This is necessary for the 
    # calibration to run.
    # also, if this is a re-initiation under a different user, require the new
    # user to enter a new contact that will be unpdated in the database. 
    if int(jobData.spinComplete) != 1:
        # Check to see if optional spinup options were enabled. If so, update the spinup status.
        if staticData.coldStart == 1 or staticData.optSpinFlag != 0:
            print("Found optional spinup alternatives")
            jobData.spinComplete = 1
            try:
                db.updateSpinupStatus(jobData)
            except:
                errMod.errOut(jobData)
        else:
            jobData.errMsg = "ERROR: Spinup for job ID: " + str(jobData.jobID) + \
                             " is NOT complete. You must complete the spinup in order" + \
                             " to run calibration."
            errMod.errOut(jobData)
        
    if int(jobData.calibComplete) == 1:
        jobData.errMsg = "ERROR: Calibration for job ID: " + str(jobData.jobID) + \
                         " has already completed."
        errMod.errOut(jobData)
        
    #if userTmp != jobData.owner:
    #    print("User: " + userTmp + " is requesting to takeover jobID: " + \
    #          str(jobData.jobID) + " from owner: " + str(jobData.owner))
    #    strTmp = "Please enter new email address. Leave blank if no email " + \
    #             "change is desired. NOTE if you leave both email and Slack " + \
    #             "information blank, no change in contact will occur. Only " + \
    #             "the owner will be modified:"
    #    newEmail = input(strTmp)
    #    #strTmp = "Please enter Slack channel:"
    #    #newSlackChannel = raw_input(strTmp)
    #    #strTmp = "Please enter Slack token:"
    #    #newSlackToken = raw_input(strTmp)
    #    #strTmp = "Please enter Slack user name:"
    #    #newSlackUName = raw_input(strTmp)
    #    # V1.2 NOTE!!!!!
    #    # Given the automation of the workflow on Yellowstone, we are simply
    #    # keeping contact information the same, but only changing the ownership
    #    # of the workflow
    #    changeFlag = 1
    #    #if len(newSlackChannel) != 0 and len(newSlackToken) == 0:
    #    #    print "ERROR: You must specify an associated Slacker API token."
    #    #    sys.exit(1)
    #    #if len(newSlackChannel) != 0 and len(newSlackUName) == 0:
    #    #    print "ERROR: You must specify an associated Slacker user name."
    #    #    sys.exit(1)
    #    #if len(newSlackToken) != 0 and len(newSlackChannel) == 0:
    #    #    print "ERROR: You must specify an associated Slacker channel name."
    #    #    sys.exit(1)
    #    #if len(newSlackToken) != 0 and len(newSlackUName) == 0:
    #    #    print "ERROR: You must specify an associated Slacker user name."
    #    #    sys.exit(1)
    #    #if len(newSlackUName) != 0 and len(newSlackChannel) == 0:
    #    #    print "ERROR: You must specify an associated Slacker channel name."
    #    #    sys.exit(1)
    #    #if len(newSlackUName) != 0 and len(newSlackToken) == 0:
    #    #    print "ERROR: You must specify an associated Slacker API token."
    #    #    sys.exit(1)
    #    #if len(newSlackChannel) != 0 and len(newEmail) != 0:
    #    #    print "ERROR: You cannot specify both email and Slack for notifications."
    #    #    sys.exit(1)
    #    #if len(newSlackChannel) == 0 and len(newEmail) == 0:
    #    #    changeFlag = 0
    #
    #    # PLACEHOLDER FOR CHECKING SLACK CREDENTIALS
    #
    #    jobData.genMsg = "MSG: User: " + userTmp + " Is Taking Over JobID: " + str(jobData.jobID) + \
    #                     " From Owner: " + str(jobData.owner)
    #    errMod.sendMsg(jobData)
    #
    #    # If a new owner takes over, simply change the owner, but keep all
    #    # other contact information the same.
    #    newEmail = jobData.email
    #    newSlackChannel = jobData.slChan
    #    newSlackToken = jobData.slToken
    #    newSlackUName = jobData.slUser
    #    if not newEmail:
    #        newEmail = ''
    #    if not newSlackChannel:
    #        newSlackChannel = ''
    #        newSlackToken = ''
    #
    #    try:
    #        db.updateJobOwner(jobData,userTmp,newEmail,newSlackChannel,newSlackToken,newSlackUName,changeFlag)
    #    except:
    #        errMod.errOut(jobData)
            
    # Begin an "infinite" do loop. This loop will continue to loop through all 
    # the basins until calibrations are complete. Basins are allowed ONE failure. A restart
    # will be attempted. If the restart fails again, a LOCK file is placed into the
    # run directory and an error email is sent to the user. Additionally, if the R calibration
    # code fails, a seperate LOCK file will be placed into the directory, and the user
    # will be notified about the failure.
    completeStatus = False
    
    # Create a "key" array. This array is of length [numBasins] and is initialized to 0.0.
    # Each array element can have the following values based on current model status:
    # 0.0 - Initial value
    # 0.25 - This is a special value for the first iteration. The initial default 
    #        parameter values specified in the parameter table by the user are being
    #        applied and entered into the DB. 
    # 0.5 - Model simulation in progress
    # 0.75 - The model simulation has completed. We are ready to run the R code to 
    #        generate the next set of parameter values and enter evaluation statistics
    #        into the DB. 
    # 0.90 - The R code is running to generate new parameters estimates. Python is
    #        also generating new files.
    # 1.0 - R/Python code is complete and param/stats have been enetered into the DB. Ready to 
    #       run the next model iteration. 
    # -0.1 - The R code to generate the initial parameter values has failed. CALIB.LOCK
    #        is put into place. 
    # -0.25 - The workflow has found the model simulation to have failed. 
    # -0.5 - Model simulation failed once and a restart is being ran. 
    # -0.75 - The R/Python code to generate new parameters/stats has failed. CALIB.LOCK
    #         is put into place. 
    # -1.0 - Model has failed twice. A RUN.LOCK file has been created.
    # Once all array elements are 1.0, then completeStatus goes to True, an entry into
    # the database occurs, and the program will complete.
    keySlot = np.empty([len(jobData.gages),int(jobData.nIter)])
    keySlot[:,:] = 0.0
    
    # Create an array to hold systme job ID values. This will only be used for
    # PBS as qstat has demonstrated slow behavior when doing a full qstat command. 
    # We will track job ID values and do a qstat <jobID> and populate this array
    # to keep track of things. 
    pbsJobId = np.empty([len(jobData.gages)],np.int64)
    pbsJobId[:] = -9999
    
    # NOTE this is different from the spinup. We have a 2D array of values to account
    # for all the iterations. 
    entryValue = float(len(jobData.gages)*int(jobData.nIter))
    
    # Pull all the status values into the keySlot array. 
    for basin in range(0,len(jobData.gages)):
        domainID = jobData.gageIDs[basin]
            
        if domainID == -9999:
            jobData.errMsg = "ERROR: Unable to locate domainID for gage: " + str(jobData.gages[basin])
            errMod.errOut(jobData)
            
        # We are going to pull all values for one basin, then place them into the array.
        # This is faster then looping over each iteration at a time. 
        statusData = db.iterationStatus(jobData,domainID,str(jobData.gages[basin]))
        statusData = [list(item) for item in statusData]
        for iteration in range(0,int(jobData.nIter)):
            for iteration2 in range(0,int(jobData.nIter)):
                if statusData[iteration2][0] == iteration+1:
                    keySlot[basin,iteration] = float(statusData[iteration2][1])
                    
                
    while not completeStatus:
        # Walk through calibration directories for each basin. Determine the status of
        # the model runs by the files available. If restarting, modify the 
        # namelist files appropriately. Then, restart the model. If anything goes wrong, notifications
        # will either be emailed per the user's info, or piped to Slack for group
        # notification. A simulation is deemed complete when all expected RESTART
        # files are present and there are no jobs running for the basin. The parameter
        # estimation is deemed complete when CALIB_ITER.COMPLETE is present and
        # no calibration jobs for this basin are running. 
        # Loop through each basin. Perform the following steps:
        # 1.) If status is -0.5,0.0, or 0.5, check to see if the model is running
        #     for this basin or if parameter estimation is occurring.
        # 2.) If the model is not running, check for expected output and perform
        #     necessary logistics. Continue to parameter estimation. Note that
        #     for the first iteration, R needs to be ran before the model to get
        #     initial default parameters. 
        # 3.) Once the model is complete, the status goes to 0.75.
        # 4.) Fire off a job to run R/Python code for parameter estimation, generation
        #     , plot generation, and generation of model eval statistics to be 
        #     entered into the DB. Status goes to 0.90.
        # 5.) Once the calibration job is complete, the status goes to 1.0 and the 
        #     workflow is ready for the next iteration. 
        # If the status goes to -1.0, a LOCK file is created and must be manually
        # removed from the user. Once the program detects this, it will restart the
        # model and the status goes back to 0.5. 
        # If the status goes to -0.75, a LOCK file is created and needs to be removed
        # manually by the user before the workflow can continue. 

        for basin in range(0,len(jobData.gages)):
            for iteration in range(0,int(jobData.nIter)):
                # Only process basins that are part of this group, per the argument passed into the
                # program.
                if jobData.gageGroup != int(args.groupNum):
                    keySlot[basin,iteration] = 1.0
                    continue
                # Holding onto the status value before the workflow iterates for checking below.
                keyStatusCheck1 = keySlot[basin,iteration]
                # If the status is already 1.0, then continue the loop as now work needs to be done.
                if keyStatusCheck1 == 1.0:
                    continue
                else:
                    try:
                        calibMod.runModel(jobData,staticData,db,jobData.gageIDs[basin],
                                          jobData.gages[basin],keySlot,basin,iteration,pbsJobId)
                    except:
                        errMod.errOut(jobData)
                # Temporary for Cheyenne to slow down the strain on PBS. 
                keyStatusCheck2 = keySlot[basin,iteration]
                # Put some spacing between launching model simulations to slow down que geting 
                # overloaded.
                if keyStatusCheck1 == 0.25 and keyStatusCheck2 == 0.5:
                    time.sleep(30)
                if keyStatusCheck1 == 0.0 and keyStatusCheck2 == 0.5:
                    time.sleep(30)
                if keyStatusCheck1 == 0.5 and keyStatusCheck2 == 0.5:
                    time.sleep(30)
                if keyStatusCheck1 == 0.0 and keyStatusCheck2 == 0.25:
                    time.sleep(30)
                if keyStatusCheck1 == 0.5 and keyStatusCheck2 == 0.75:
                    time.sleep(30)
                if keyStatusCheck1 == 0.75 and keyStatusCheck2 == 0.9:
                    time.sleep(30)
                if keyStatusCheck1 == 0.5 and keyStatusCheck2 == 0.9:
                    time.sleep(30)
                if keyStatusCheck1 == 0.9 and keyStatusCheck2 == 0.9:
                    time.sleep(30)
                if keyStatusCheck1 == 0.9 and keyStatusCheck2 == 1.0:
                    time.sleep(30)
                    
                # TEMPORARY FOR CHEYENNE
                # Check to make sure program hasn't passed a prescribed time limit. If it has,
                # exit gracefully.
                #timeCheckStamp = datetime.datetime.now()
                #programDtCheck = timeCheckStamp - begTimeStamp
                #if programDtCheck.seconds/60.0 > 90.0: 
                #    # 90-minutes)
                #    try:
                #        fileObj = open(lockPath,'a')
                #        fileObj.write('WORKFLOW HAS HIT TIME LIMIT - EXITING....\n')
                #        fileObj.close()
                #    except:
                #        jobData.errMsg = "ERROR: Unable to update workflow LOCK file: " + lockPath
                #        errMod.errOut(jobData)
                    
        # Check to see if program requirements have been met.
        if keySlot.sum() == entryValue:
            jobData.calibComplete = 1
            try:
                db.updateCalibStatus(jobData)
            except:
                errMod.errout(jobData)
            jobData.genMsg = "CALIBRATION FOR JOB ID: " + str(jobData.jobID) + " COMPLETE."
            errMod.sendMsg(jobData)
            completeStatus = True
            
        # Open the Python LOCK file. Write a blank line to the file and close it.
        # This action will simply modify the file modification time while only adding
        # a blank line.
        #try:
        #    fileObj = open(lockPath,'a')
        #    fileObj.write('\n')
        #    fileObj.close()
        #except:
        #    jobData.errMsg = "ERROR: Unable to update workflow LOCK file: " + lockPath
        #    errMod.errOut(jobData)
            
    # Remove LOCK file
    #os.remove(lockPath)

if __name__ == "__main__":
    main(sys.argv[1:])
