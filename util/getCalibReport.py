# This is a utiltiy program for the user to do a quick dump of where
# their calibration job is at for each basin. The user has the option
# to either print to the screen, or send the output to the job 
# contact information. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# 303-497-2693
# karsten@ucar.edu

import argparse
import sys
import os
import getpass
import numpy as np

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
import configMod

def main(argv):
    # Parse arguments. User must input a job name and directory.
    parser = argparse.ArgumentParser(description='Utility program to report the position' + \
                                     ' of a calibration job.')
    parser.add_argument('jobID',metavar='jobID',type=str,nargs='+',
                        help='Job ID specific to calibration spinup.')
    parser.add_argument('contactFlag',metavar='ctFlag',type=int,nargs='+',
                        help='1 = send to job contact, 0 = print to screen.')
    parser.add_argument('--email',nargs='?',help='Optional email to pipe output to.')
    parser.add_argument('--hostname',type=str,nargs='?',
                        help='Optional hostname MySQL DB resides on. Will use localhost if not passed.')
                        
    args = parser.parse_args()
    
    # Create dictionary of specified status messages.
    msgDict = {'-1.0':'MODEL RUN LOCKED.','-0.75':'MAIN CALIBRATION PROGRAM LOCKED',
               '-0.5':'MODEL FAILED ONCE - RUNNING AGAIN','-0.25':'MODEL FAILED ONCE - WAITING',
               '-0.1':'CALIBRATON PROGRAM FOR DEFAULT PARAMETERS LOCKED',
               '0.0':'NOT STARTED','0.25':'CALIBRATION PROGRAM FOR DEFAULT PROGRAM RUNNING',
               '0.5':'MODEL CURRENTLY RUNNING','0.75':'MODEL COMPLETE READY FOR PARAMETER ESTIMATION',
               '0.9':'PARAMETER ESTIMATION OCCURRING','1.0':'MODEL ITERATION COMPLETE'}
    
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
    jobData.dbUName= 'WH_Calib_rw'
    jobData.port = 5432    
    
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
        
    # Pull extensive meta-data describing the job from the config file.
    configPath = str(jobData.jobDir) + "/setup.config"
    if not os.path.isfile(configPath):
        print "ERROR: Configuration file: " + configPath + " not found."
        sys.exit(1)
    staticData = configMod.readConfig(configPath)
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
       
    # If an optional email was passed to the program, update the job object to 
    # reflect this for information dissemination.
    if args.email:
        jobData.slackObj = None
        jobData.email = str(args.email)

    # Loop through each basin. Determine if which iteration we are on, then report the status
    # of the job for this basin.
    msgOut = ''
    iterArray = np.empty([int(jobData.nIter)],np.int)
    completeArray = np.empty([int(jobData.nIter)],np.float)

    meanSum = 0.0
    for basin in range(0,len(jobData.gages)):
        iterArray[:] = 0
        completeArray[:] = 0.0
        keyStatus = 0.0
        keyStatusPrev = 0.0
        domainID = jobData.gageIDs[basin]
        iterComplete = 1 
        statusData = db.iterationStatus(jobData,domainID,str(jobData.gages[basin]))
        for iteration in range(0,int(jobData.nIter)):
            keyStatus = float(statusData[iteration][1])
            iterationTmp = int(statusData[iteration][0])
	    iterArray[iteration] = iterationTmp
            completeArray[iteration] = keyStatus

	indComplete = np.where(completeArray == 1)
	indCheck1 = np.where(completeArray != 1.0)
	indCheck2 = np.where(completeArray == 0.0)
	meanSum = meanSum + len(indComplete[0])
	if len(indComplete[0]) == int(jobData.nIter):
	    msgOut = msgOut + "BASIN: " + str(jobData.gages[basin]) + \
                     ": CALIBRATION COMPLETE.\n"
	else:
            if len(indCheck2[0]) == int(jobData.nIter):
                msgOut = msgOut + "BASIN: " + str(jobData.gages[basin]) + \
                         " - HAS NOT BEGUN CALIBRATION.\n"
            else:
                iterLastComplete = len(indComplete[0])
		iterCurrent = iterLastComplete + 1
		indCurrent = np.where(iterArray == iterCurrent)
		statusCurrent = completeArray[indCurrent[0][0]]
                ind2 = np.where(completeArray != 0.0) and np.where(completeArray != 1.0)
                iterTmp = iterArray[ind2[0][0]]
		msgOut = msgOut + "BASIN: " + str(jobData.gages[basin]) + \
		         ": " + str(msgDict[str(statusCurrent)]) + \
		         " - ITERATION: " + str(iterCurrent) + "\n"
                  
    print "MEAN COMPLETENESS = " + str(float(meanSum)/len(jobData.gages))
    jobData.genMsg = msgOut
    if int(args.contactFlag[0]) == 0:
        print jobData.genMsg
    else:
        errMod.sendMsg(jobData)
                
                
if __name__ == "__main__":
    main(sys.argv[1:])
