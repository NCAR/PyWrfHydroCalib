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
from core import statusMod
from core import dbMod
from core import configMod
from core import errMod

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

    # Loop over each basin group. This program will check to see if a group job is running
    # which is an instance of the calib.py program looping over basins for a group.
    # If a group job is not running, this program will check the database file to see
    # if the basins in the group have a complete status for all calibration iterations.
    for basinGroup in range(0,jobData.nGroups):
        # If all groups are complete, the calibration experiment is complete.
        if sum(jobData.groupComplete) == jobData.nGroups:
            print("COMPLETE")
            # STUB FOR EXITING PROGRAM WITH COMPLETION.

        # Compose a complete flag for this specific group of basins. If this complete flag is present,
        # that means these basins are complete.
        basinCompleteFlag = str(jobData.jobDir) + "/CALIB_GROUP_" + str(basinGroup) + ".COMPLETE"

        if os.path.isfile(basinCompleteFlag):
            jobData.groupComplete[basinGroup] = 1
            continue

        # Setup a job script that will execute the calibration program, passing in the group number
        # to instruct the workflow on which basins to process.
        runScript = jobData.jobDir + "/run_group_" + str(basinGroup) + ".sh"
        if not os.path.isfile(runScript):
            # CALL ROUTINE TO COMPOSE FILE.

if __name__ == "__main__":
    main(sys.argv[1:])