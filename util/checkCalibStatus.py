# Utility function to check to see if a job is running. The program
# will ping the database for current information on the calibration, 
# then check to see if current jobs are running. If no jobs are 
# running on Yellowstone, but the database says things should be 
# running, an error message is sent to the user. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laborator
# 303-497-2693
# karsten@ucare.edu

import argparse
import sys
import os
import getpass

import warnings
warnings.filterwarnings("ignore")

# Set the Python path to include package specific functions.
prPath = os.path.realpath(__file__)
pathSplit = prPath.split('/')
libPath = '/'
for j in range(1,len(pathSplit)-2):
    libPath = libPath + pathSplit[j] + '/'
libPath = libPath + 'lib/Python'
sys.path.insert(0,libPath)

import statusMod
import dbMod
import errMod

def main(argv):
    # Parse arguments. User must input a job name and directory.
    parser = argparse.ArgumentParser(description='Utility program to check to' + \
                                     ' make sure a given job is running.')
    parser.add_argument('jobID',metavar='jobID',type=str,nargs='+',
                        help='Job ID specific to calibration spinup.')
    parser.add_argument('--hostname',type=str,nargs='?',
                        help='Optional hostname MySQL DB resides on. Will use localhost if not passed.')
    
    args = parser.parse_args()
    
    # Initialize object to hold status and job information
    jobData = statusMod.statusMeta()
    jobData.jobID = int(args.jobID[0])

    # Lookup database username/login credentials based on username
    # running program.
    try:
        pwdTmp = getpass.getpass('Enter Database Password: ')
        jobData.dbUName= 'WH_Calib_rw'
        jobData.dbPwd = str(pwdTmp)
    except:
        print "ERROR: Unable to authenticate credentials for database."
        sys.exit(1)
        
    if not args.hostname:
        # We will assume localhost for Postgres DB
        hostTmp = 'localhost'
    else:
        hostTmp = str(args.hostname)
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
        
    # Check to see if spinup or calibration logged as complete in the DB.
    if int(jobData.spinComplete) != 1:
        jobData.errMsg = "ERROR: You must complete the spinup in order to run statuses on calibration."
        errMod.errOut(jobData)
        
    if int(jobData.calibComplete) == 1:
        # calibration complete, exit gracefully.
        sys.exit(0)
        
    # Check gages in directory to match what's in the database
    try:
        jobData.checkGages(db)
    except:
        errMod.errOut(jobData)
        
    # For version 1.2 calibration, we are goingt to assume there is ALWAYS a job running.
    # Testing reveals that there are always several dozen jobs running at any given time due
    # to the scope of the simulations. Loop through basins and check for a job running.
    # as soon as we find one job running, we can exit knowing something is running, which
    # means the workflow is still running.
    for basin in range(0,len(jobData.gages)):
        # First pull the unique ID for the basin. 
        calibStatus = statusMod.checkCalibJob(jobData,basin)
        modelStatus = statusMod.checkBasJob(jobData,basin)
        if calibStatus or modelStatus:
            print "BASIN: " + str(jobData.gages[basin])
            print "MODEL STATUS: " + str(modelStatus)
            print "CALIB STATUS: " + str(calibStatus)
            print "WORKFLOW IS RUNNING."
            sys.exit(0)
            
    # If this loop exited without finding something running, this implies the 
    # workflow has crashed. Error out.
    #jobData.errMsg = "ERROR: Calibration Workflow for JOB: " + str(jobData.jobID) + \
    #                " Appears to Have Crashed for user: " + str(jobData.owner)
    #errMod.errOut(jobData)
    
    # Loop through each basin being calibrated. Obtain the calibration status
    # for each iteration. If the calibration status is listed as having either
    # parameter estimation running, or a model simulation running, then there
    # NEEDS to be a corresponding calibration or simulation job attached to 
    # this basin running. There will be four tries to check this before 
    # the program will error out via the contact information stored in the 
    # database.
    #keyStatus = 0.0
    #for basin in range(0,len(jobData.gages)):
    #    # First pull the unique ID for the basin. 
    #    try:
    #        domainID = db.getDomainID(jobData,str(jobData.gages[basin]))
    #    except:
    #        errMod.errOut(jobData)
    #    for iteration in range(0,int(jobData.nIter)):
    #        # Pull job status for this iteration
    #        keyStatus = db.iterationStatus(jobData,domainID,iteration,str(jobData.gages[basin]))
    #        # if calibration job is running....
    #        if keyStatus == 0.25 or keyStatus == 0.90:
    #            # Check for calibration job running.
    #            for checkTick in range(1,6):
    #                calibStatus = statusMod.checkCalibJob(jobData,basin)
    #                if not calibStatus:
    #                    print "CHECK " + str(checkTick) + " SHOWS NO CALIBRATION RUNNING."
    #                    print "GAGE: " + str(jobData.gages[basin])
    #                    # Ping DB again to see if things have updated....
    #                    keyStatus = db.iterationStatus(jobData,domainID,iteration,str(jobData.gages[basin]))
    #                    if keyStatus == 0.25 or keyStatus == 0.90:
    #                        # Status remains unchanged.
    #                        if checkTick == 5:
    #                            # We have reached the limit of testing, and we are assuming the workflow 
    #                            # has crashed. 
    #                            jobData.errMsg = "ERROR: Calibration Workflow for JOB: " + str(jobData.jobID) + \
    #                                             " Appears to Have Crashed for user: " + str(jobData.owner)
    #                            errMod.errOut(jobData)
    #                    if keyStatus == 0.50 or keyStatus == 1.00:
    #                        # Calibration program completed, and job has moed on.
    #                        print "FOUND RUNNING CALIBRATION: " + str(jobData.jobID)
    #                        sys.exit(0)
    #                time.sleep(40)
    #        if keyStatus == 0.5:
    #            # Check for model running
    #            for checkTick in range(1,6):
    #                modelStatus = statusMod.checkBasJob(jobData,basin)
    #                if not modelStatus:
    #                    print "CHECK " + str(checkTick) + " SHOWS NO MODEL RUNNING."
    #                    print "GAGE: " + str(jobData.gages[basin])
    #                    # Ping DB again to see if things have updated....
    #                    keyStatus = db.iterationStatus(jobData,domainID,iteration,str(jobData.gages[basin]))
    #                    if keyStatus == 0.5:
    #                        if checkTick == 5:
    #                            # We have reached the limit of testing, and we are assuming the workflow 
    #                            # has crashed. 
    #                            jobData.errMsg = "ERROR: Calibration Workflow for JOB: " + str(jobData.jobID) + \
    #                                             " Appears to Have Crashed for user: " + str(jobData.owner)
    #                            errMod.errOut(jobData)
    #                    if keyStatus == 0.75 or keyStatus == 0.90:
    #                        # Model simulation has completed and moved on.
    #                        print "FOUND RUNNING JOBID: " + str(jobData.jobID)
    #                        sys.exit(0)
if __name__ == "__main__":
    main(sys.argv[1:])