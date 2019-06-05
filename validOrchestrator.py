# This is a high-level Python program for executing the calibration workflow
# on a distributed HPC environment where a scheduler is in place. The
# need for this arose out of NWM v2.1 activities. Under this scenario,
# the model needs to have specific CPUs on a compute node dedicated
# to a specific basin. Prior to this, one basin took up an entire
# node, which is computationally inefficient.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# 303-497-2693
# karsten@ucar.edu

import os
import sys
import argparse
import pwd
import numpy as np
from core import statusMod
from core import dbMod
from core import configMod
from core import errMod
from core import calibIoMod
import time

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
    parser = argparse.ArgumentParser(description='Main orchestrator to start or restart ' + \
                                                 'calibration for WRF-Hydro')
    parser.add_argument('jobID', metavar='jobID', type=str, nargs='+',
                        help='Job ID specific to calibration experiment.')
    parser.add_argument('--optDbPath', type=str, nargs='?',
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

    # Some house keeping here. If the validation is already complete, throw an error.
    # also, if this is a re-initiation under a different user, require the new
    # user to enter a new contact that will be unpdated in the database.
    # Also require that both the spinup and calibrations have been entered into
    # the database as complete.
    if int(jobData.validComplete) == 1:
        jobData.errMsg = "ERROR: Validation for job ID: " + str(jobData.jobID) + \
                         " has already completed."
        errMod.errOut(jobData)
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

    if int(jobData.calibComplete) != 1:
        jobData.errMsg = "ERROR: Calibration for job ID: " + str(jobData.jobID) + \
                         " has NOT completed. Please complete calibration before " + \
                         " proceeding."
        errMod.errOut(jobData)

    if userTmp != jobData.owner:
        print("User: " + userTmp + " is requesting to takeover jobID: " + \
              str(jobData.jobID) + " from owner: " + str(jobData.owner))
        strTmp = "Please enter new email address. Leave blank if no email " + \
                 "change is desired. NOTE if you leave both email and Slack " + \
                 "information blank, no change in contact will occur. Only " + \
                 "the owner will be modified:"
        newEmail = input(strTmp)
        # strTmp = "Please enter Slack channel:"
        # newSlackChannel = raw_input(strTmp)
        # strTmp = "Please enter Slack token:"
        # newSlackToken = raw_input(strTmp)
        # strTmp = "Please enter Slack user name:"
        # newSlackUName = raw_input(strTmp)
        # V1.2 NOTE!!!!!
        # Given the automation of the workflow on Yellowstone, we are simply
        # keeping contact information the same, but only changing the ownership
        # of the workflow
        changeFlag = 1
        # if len(newSlackChannel) != 0 and len(newSlackToken) == 0:
        #    print "ERROR: You must specify an associated Slacker API token."
        #    sys.exit(1)
        # if len(newSlackChannel) != 0 and len(newSlackUName) == 0:
        #    print "ERROR: You must specify an associated Slacker user name."
        #    sys.exit(1)
        # if len(newSlackToken) != 0 and len(newSlackChannel) == 0:
        #    print "ERROR: You must specify an associated Slacker channel name."
        #    sys.exit(1)
        # if len(newSlackToken) != 0 and len(newSlackUName) == 0:
        #    print "ERROR: You must specify an associated Slacker user name."
        #    sys.exit(1)
        # if len(newSlackUName) != 0 and len(newSlackChannel) == 0:
        #    print "ERROR: You must specify an associated Slacker channel name."
        #    sys.exit(1)
        # if len(newSlackUName) != 0 and len(newSlackToken) == 0:
        #    print "ERROR: You must specify an associated Slacker API token."
        #    sys.exit(1)
        # if len(newSlackChannel) != 0 and len(newEmail) != 0:
        #    print "ERROR: You cannot specify both email and Slack for notifications."
        #    sys.exit(1)
        # if len(newSlackChannel) == 0 and len(newEmail) == 0:
        #    changeFlag = 0

        # PLACEHOLDER FOR CHECKING SLACK CREDENTIALS

        jobData.genMsg = "MSG: User: " + userTmp + " Is Taking Over JobID: " + str(jobData.jobID) + \
                         " From Owner: " + str(jobData.owner)
        errMod.sendMsg(jobData)

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
            db.updateJobOwner(jobData, userTmp, newEmail, newSlackChannel, newSlackToken, newSlackUName, changeFlag)
        except:
            errMod.errOut(jobData)

    # Create an array to hold systme job ID values. This will only be used for
    # PBS as qstat has demonstrated slow behavior when doing a full qstat command.
    # We will track job ID values and do a qstat <jobID> and populate this array
    # to keep track of things.
    pbsJobId = np.empty([jobData.nGroups], np.int64)
    pbsJobId[:] = -9999

    # Initialize a complete status flag to be false. The orchestrator program
    # will loop over each group, checking for a COMPLETE flag. Once all COMPLETE
    # flags are present, the requirements for completetion have been met, and the
    # program will exit successfully.
    completeStatus = False

    while not completeStatus:
        # Loop over each basin group. This program will check to see if a group job is running
        # which is an instance of the calib.py program looping over basins for a group.
        for basinGroup in range(0,jobData.nGroups):
            print("WORKING ON GROUP: " + str(basinGroup))
            # Compose a complete flag for this specific group of basins. If this complete flag is present,
            # that means these basins are complete.
            basinCompleteFlag = str(jobData.jobDir) + "/VALID_GROUP_" + str(basinGroup) + ".COMPLETE"

            if os.path.isfile(basinCompleteFlag):
                jobData.groupComplete[basinGroup] = 1
                continue

            # Setup a job script that will execute the calibration program, passing in the group number
            # to instruct the workflow on which basins to process.
            if jobData.jobRunType == 4:
                # This is for MPI scripts as we don't have a scheduler.
                runScript = jobData.jobDir + "/WVG_" + str(jobData.jobID) + "_" + \
                            str(basinGroup)
            else:
                runScript = jobData.jobDir + "/run_group_" + str(basinGroup) + ".sh"
            if not os.path.isfile(runScript):
                try:
                    calibIoMod.generateValidGroupScript(jobData, basinGroup, runScript, topDir)
                except:
                    errMod.errOut(jobData)
            else:
                # We will regenerate the run script each time to ensure no previous group
                # scripts are not being used.
                try:
                    os.remove(runScript)
                except:
                    jobData.errMsg = "Unable to remove old run script file: " + runScript
                    errMod.errOut(jobData)

            # Check to see if this group is currently running.
            groupStatus = statusMod.checkBasGroupJob(jobData,basinGroup,pbsJobId,'WVG')

            print('GROUP STATUS = ' + str(groupStatus))
            if not groupStatus:
                # Check to see if the complete flag was generated.
                if os.path.isfile(basinCompleteFlag):
                    jobData.groupComplete[basinGroup] = 1
                    continue
                else:
                    # We need to fire off a new group job.
                    print('SUBMITTING GROUP JOB')
                    try:
                        statusMod.submitGroupCalibration(jobData,runScript,pbsJobId,basinGroup)
                    except:
                        errMod.errOut(jobData)
                    print(pbsJobId)

            # Allow for some time in-between groups.
            time.sleep(5)

        # Check to see if the program requirements have been met.
        if sum(jobData.groupComplete) == jobData.nGroups:
            jobData.validComplete = 1
            try:
                db.updateValidationStatus(jobData)
            except:
                errMod.errout(jobData)
            jobData.genMsg = "VALIDATION FOR JOB ID: " + str(jobData.jobID) + " COMPLETE."
            errMod.sendMsg(jobData)

            completeStatus = True

if __name__ == "__main__":
    main(sys.argv[1:])