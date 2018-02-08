# Main calling program to initiate a sensitivity analysis of WRF-Hydro. 
# Please see comments below on steps involved for the workflow
# to run. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu
# 303-497-2693

import sys
import argparse
import getpass
import os
import time
import pwd
import numpy as np
import socket

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
import configMod
import sensitivityMod
import pandas as pd

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Main program to start or restart ' + \
             'sensitivity analysis for WRF-Hydro')
    parser.add_argument('jobID',metavar='jobID',type=str,nargs='+',
                        help='Job ID specific to your sensitivity/caliration workflow job.')
    parser.add_argument('--hostname',type=str,nargs='?',
                        help='Optional hostname Postgres DB resides on. Will use localhost if not passed.')
    parser.add_argument('--portNumber',type=int,nargs='?',
                        help='Optional port number to connect. Default is 5432.')
    
    args = parser.parse_args()
    
    # Get current user who is running this program.
    userTmp = pwd.getpwuid(os.getuid()).pw_name
    
    # Initialize object to hold status and job information
    jobData = statusMod.statusMeta()
    jobData.jobID = int(args.jobID[0])
    
    # Lookup database username/login credentials based on username
    # running program.
    #try:
    #    pwdTmp = getpass.getpass('Enter Database Password: ')
    #    jobData.dbPwd = str(pwdTmp)
    #except:
    #    print "ERROR: Unable to authenticate credentials for database."
    #    sys.exit(1)
    jobData.dbPwd = 'IJustWannaCalibrate'
    
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
        
    # If the sensitivity flag is 0, simply exit gracefully as the user specified
    # not to run calibration.
    if jobData.sensFlag != 1:
        print "ERROR: Sensitivity flag was set to 0 for this workflow."
        sys.exit(1)
        
    # Establish LOCK file to secure this Python program to make sure
    # no other instances over-step here. This is mostly designed to deal
    # with nohup processes being kicked off Yellowstone/Cheyenne/Crontabs arbitrarily.
    # Just another check/balance here.
    lockPath = str(jobData.jobDir) + "/PYTHON.LOCK"
    if os.path.isfile(lockPath):
        # Either a job is still running, or was running
        # and was killed.

        # Read in to get PID number
        pidObj = pd.read_csv(lockPath)
        pidCheck = int(pidObj.PID[0])
        
        # Establish the host this is being ran on.
        hostTmp = socket.gethostname()
        if errMod.check_pid(pidCheck):
                print "JOB: " + str(pidCheck) + \
                      " Is still running on host: " + str(hostTmp)
                sys.exit(0)
        else:
                print "JOB: " + str(pidCheck) + \
                      " Has Failed. Removing LOCK " + \
                      " file."
                os.remove(lockPath)
                fileObj = open(lockPath,'w')
                fileObj.write('\"PID\"\n')
                fileObj.write(str(os.getpid()))
                fileObj.close()
    else:
        # Write a LOCK file for this program.
        fileObj = open(lockPath,'w')
        fileObj.write('\"PID\"\n')
        fileObj.write(str(os.getpid()))
        fileObj.close()
        
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
        
    # Check gages in directory to match what's in the database
    try:
        jobData.checkGages(db)
    except:
        errMod.errOut(jobData)
        
    # Some house keeping here. If the sensitivity is already complete, throw an error. 
    # Also ensure the spinup has been entered as complete. This is necessary for the 
    # sensitivity to run.
    # also, if this is a re-initiation under a different user, require the new
    # user to enter a new contact that will be unpdated in the database. 
    if int(jobData.spinComplete) != 1:
        jobData.errMsg = "ERROR: Spinup for job ID: " + str(jobData.jobID) + \
                         " is NOT complete. You must complete the spinup in order" + \
                         " to run calibration."
        errMod.errOut(jobData)
        
    if int(jobData.sensComplete) == 1:
        jobData.errMsg = "ERROR: Sensitivity for job ID: " + str(jobData.jobID) + \
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
        # V1.2 NOTE!!!!!
        # Given the automation of the workflow on Yellowstone, we are simply 
        # keeping contact information the same, but only changing the ownership
        # of the workflow
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
            
        jobData.genMsg = "MSG: User: " + userTmp + " Is Taking Over JobID: " + str(jobData.jobID) + \
                         " From Owner: " + str(jobData.owner)
        errMod.sendMsg(jobData)
        
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
            
        try:
            db.updateJobOwner(jobData,userTmp,newEmail,newSlackChannel,newSlackToken,newSlackUName,changeFlag)
        except:
            errMod.errOut(jobData)
            
    # Begin an "infinite" do loop. This loop will continue to loop through all 
    # the basins until sensitivity jobs are complete. Basins are allowed ONE failure. A restart
    # will be attempted. If the restart fails again, a LOCK file is placed into the
    # run directory and an error email is sent to the user.
    completeStatus = False
    
    # Create a "key" array. This array is of length [numBasins] and is initialized to 0.0.
    # Each array element can have the following values based on current model status:
    # 0.0 - Initial value
    # 0.10 - Job to generate parameter grids for each model job is being ran.
    # 0.25 - Job to generate parameter grids is complete. Ready to run models.....
    # 0.5 - Model simulationa are in progress
    # 0.75 - Job to read in model output and run sensitivity analysis is ready to be ran. 
    # 0.90 - Job to read in model output and run sensitivity analysis is running. 
    # 1.0 - Sensitivity analysis complete
    # -0.1 - Parameter generation failed. A LOCK file has been created. 
    # -0.5 - Model simulation failed once and a restart has been attempted
    # -0.90 - Sensitivity analysis job has failed. A LOCK file has been created. 
    # -1.0 - Model has failed twice. A LOCK file has been created.
    # Once all array elements are 1.0, then completeStatus goes to True, an entry into
    # the database occurs, and the program will complete.
    keySlot = np.empty([len(jobData.gages),int(jobData.nSensIter)])
    keySlot[:,:] = 0.0
    entryValue = float(len(jobData.gages)*int(jobData.nSensIter)*2.0)
    
    # Pull all the status values into the keySlot array. 
    for basin in range(0,len(jobData.gages)):
        try:
            domainID = db.getDomainID(jobData,str(jobData.gages[basin]))
        except:
            errMod.errOut(jobData)
            
        if domainID == -9999:
            jobData.errMsg = "ERROR: Unable to locate domainID for gage: " + str(jobData.gages[basin])
            errMod.errOut(jobData)
            
        # We are going to pull all values for one basin, then place them into the array.
        # This is faster then looping over each iteration at a time. 
        statusData = db.sensIterationStatus(jobData,domainID,str(jobData.gages[basin]))
        statusData = [list(item) for item in statusData]
        for iteration in range(0,int(jobData.nSensIter)):
            for iteration2 in range(0,int(jobData.nSensIter)):
                if statusData[iteration2][0] == iteration+1:
                    keySlot[basin,iteration] = float(statusData[iteration2][1])
            
    if len(np.where(keySlot != 0.0)[0]) == 0:
    #if keySlot.sum() == 0.0:
        # We need to either check to see if pre-processing has taken place, or
        # run it.
        preProcStatus = False
        
    while not completeStatus:
        # Walk through each basin undergoing sensitivity analysis. 
        for basin in range(0,len(jobData.gages)):
            print "GAGE: " + jobData.gages[basin]
            # Establish a status value for pre-processing the parameter values from R/Python code. 
            preProcStatus = False 
    
            # Establish a status value for post-processing the model output and running sensitivity
            # analysis.
            postProcStatus = False
            
            # Calculate the number of "batches" we are going to run
            nBatches = int(jobData.nSensIter/jobData.nSensBatch)
            entryValueBatch = float(jobData.nSensBatch)
            
            # If we have a pre-processing complete file, set our pre-proc status to True. 
            # Also, log parameter values generated if the log file hasn't been created. 
            preProcComplete = jobData.jobDir + "/" + jobData.gages[basin] + "/RUN.SENSITIVITY/preProc.COMPLETE"
            parmsLogged =  jobData.jobDir + "/" + jobData.gages[basin] + "/RUN.SENSITIVITY/PARAMS_LOGGED.COMPLETE"
            parmTxtFile = jobData.jobDir + "/" + jobData.gages[basin] + "/RUN.SENSITIVITY/params_new.txt"
            sensLogged = jobData.jobDir + "/" + jobData.gages[basin] + "/RUN.SENSITIVITY/SENS_LOGGED.COMPLETE"
            sensStats = jobData.jobDir + "/" + jobData.gages[basin] + "/RUN.SENSITIVITY/stat_sensitivity.txt"
            missingFlag = jobData.jobDir + "/" + jobData.gages[basin] + "/RUN.SENSITIVITY/CALC_STATS_MISSING"
            if os.path.isfile(preProcComplete):
                preProcStatus = True
                print "PRE PROCESSING COMPLETE!"
                if not os.path.isfile(parmsLogged):
                    # Log parameter values generated by pre-processing.
                    print "LOGGING PRE-PROC PARAM FILES."
                    try:
                        db.insertSensParms(jobData,parmsLogged,parmTxtFile,jobData.gageIDs[basin])
                    except:
                        jobData.errMsg = ("WARNING: Unable to log sensitivity parameters for basin: " + str(basin) + \
                                          " Job: " + str(jobData.jobID))
                        errMod.errOut(jobData)
            if not preProcStatus:
                #sensitivityMod.preProc(preProcStatus,jobData,staticData,db,jobData.gageIDs[basin],jobData.gages[basin])
                try:
                    sensitivityMod.preProc(preProcStatus,jobData,staticData,db,jobData.gageIDs[basin],jobData.gages[basin])
                except:
                    errMod.errOut(jobData)
            else:
                # The goal here is to only operate on a fixed number of model runs at a time.
                # If you have a large parameter sample size, it's possible to have hundreds,
                # if not thousands of model permuatations. This worflow allows for 
                # only batches of model runs to be ran at a time as to not bog down the system. 
                for batchIter in range(0,nBatches):
                    time.sleep(1)
                    batchCheck = keySlot[basin,(batchIter*jobData.nSensBatch):((batchIter+1)*jobData.nSensBatch)]
                    if batchIter == 0:
                        batchCheckPrev = entryValueBatch
                    else:
                        batchCheckPrev = keySlot[basin,((batchIter-1)*jobData.nSensBatch):(batchIter*jobData.nSensBatch)]
                        batchCheckPrev = batchCheckPrev.sum()
                    if batchCheck.sum() != entryValueBatch and batchCheckPrev == entryValueBatch:
                        for iterTmp in range(0,jobData.nSensBatch):
                            iteration = batchIter*jobData.nSensBatch + iterTmp
                            keyCheck1 = keySlot[basin,iteration]
                            if keyCheck1 < 1:
                                # This model iteration has not completed. 
                                try:
                                    sensitivityMod.runModel(jobData,staticData,db,jobData.gageIDs[basin],jobData.gages[basin],keySlot,basin,iteration)
                                except:
                                    errMod.errOut(jobData)
                                
                                if keySlot[basin,iteration] == 0.0 and keyCheck1 == 0.5:
                                    # Put some spacing between launching model simulations to slow down que geting 
                                    # overloaded.
                                    time.sleep(3)
                                    
                                # Update the temporary status array as it will be checked for this batch of model runs.
                                batchCheck[iterTmp] = keySlot[basin,iteration]
                                
            # Run post-processing ONLY when all model simulations are finished.
            if not postProcStatus and preProcStatus and len(np.where(batchCheck != 1.0)[0]) == 0:
                print "READY FOR POST PROCESSING"
                try:
                    sensitivityMod.postProc(postProcStatus,jobData,staticData,db,jobData.gageIDs[basin],jobData.gages[basin])
                except:
                    errMod.errOut(jobData)
                                
            postProcComplete = jobData.jobDir + "/" + jobData.gages[basin] + "/RUN.SENSITIVITY/postProc.COMPLETE"
            if os.path.isfile(postProcComplete):
                if not os.path.isfile(sensLogged) and not os.path.isfile(missingFlag):
                    # Log sensitivity statistics into the database.
                    if not os.path.isfile(sensStats):
                        jobData.errMsg = "ERROR: Expected to find: " + sensStats + " after post-processing. Not found."
                        errMod.errOut(jobData)
                    else:
                        try:
                            db.logSensStats(jobData,sensStats,jobData.gageIDs[basin],sensLogged)
                        except:
                            errMod.errOut(jobData)
                    # Check for complete flag on logging sensitivity statistics. 
                    if os.path.isfile(sensLogged):
                        postProcStatus = True
                        # Upgrade key status values as necessary
                        for iterTmp in range(0,jobData.nSensIter):
                            keySlot[basin,iterTmp] = 2.0
                elif os.path.isfile(sensLogged):
                    # Post-processing complete and statistics were sucessfully logged.
                    postProcStatus = True
                    # Upgrade key status values as necessary
                    for iterTmp in range(0,jobData.nSensIter):
                        keySlot[basin,iterTmp] = 2.0
                elif os.path.isfile(missingFlag):
                    # Missing obs were found. We will default to making this basin complete.
                    for iterTmp in range(0,jobData.nSensIter):
                        keySlot[basin,iterTmp] = 2.0
            
        # Check to see if program requirements have been met.
        if keySlot.sum() == entryValue and postProcStatus:
            jobData.sensComplete = 1
            try:
                db.updateSensStatus(jobData)
            except:
                errMod.errOut(jobData)
            jobData.genMsg = "SENSITIVITY FOR JOB ID: " + str(jobData.jobID) + " COMPLETE."
            errMod.sendMsg(jobData)
            completeStatus = True
            
    # Remove LOCK file
    os.remove(lockPath)
        
if __name__ == "__main__":
    main(sys.argv[1:])