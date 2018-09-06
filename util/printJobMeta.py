# Utility program for printing job information from the DB file for
# all jobs, or specific job given an optional ID value passed in 
# by the user. The user must provide a path the sqllite3 DB file
# created during the initialization process. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import sqlite3
import argparse
import os
import sys

def main(argv):
    # Parse arguments. User must input a path to the sqllite DB file.
    parser = argparse.ArgumentParser(description='Utility for printing metadata ' + \
             ' on a specific job, or all jobs in the DB')
    parser.add_argument('inDB',metavar='inDB',type=str,nargs='+',
                        help='Required path to sqllite3 DB file.')
    parser.add_argument('--optJobID',type=str,nargs='?',
                        help='Optional job ID for metadata on a specific job.')

    args = parser.parse_args()
    
    # If the sqllite DB file does not exist, throw an error to the user.
    if not os.path.isfile(args.inDB[0]):
        print "ERROR: Unable to locate DB file: " + args.inDB[0]
        sys.exit(1)

    dbPath = args.inDB[0]

    # Open the SQLite DB file
    try:
        conn = sqlite3.connect(dbPath)
    except:
        print "ERROR: Unable to connect to: " + dbPath + ". Please intiialize the DB file."
        sys.exit(1)

    try:
        dbCursor = conn.cursor()
    except:
        print "ERROR: Unable to establish cursor object for: " + dbPath
        sys.exit(1)
        
    if args.optJobID:
        sqlCmd = "SELECT * from \"Job_Meta\" where \"jobID\"=%s;" % args.optJobID
    else:
        sqlCmd = "SELECT * from \"Job_Meta\";"
        
    try:
        dbCursor.execute(sqlCmd)
        results = dbCursor.fetchall()
    except:
        print "ERROR: Unable to execute SQL command: " + sqlCmd
        sys.exit(1)
        
    numJobs = len(results)
    
    if numJobs == 0:
        print "No jobs have been found in the DB tables."
        sys.exit(0)
        
    # Loop through the various jobs and print out information to the screen on
    # metadata for each job. 
    for job in range(0,numJobs):
        print "--------------------------------------------------"
        print "Job ID: " + str(results[job][0])
        print "Job Directory: " + str(results[job][1])
        print "Spinup Start Date: " + results[job][2]
        print "Spinup End Date: " + str(results[job][3])
        print "Spinup Complete: " + str(results[job][4])
        print "Sensitivity Flag: " + str(results[job][5])
        print "Sensitivity Table: " + str(results[job][6])
        print "Sample Size for Sensitivity: " + str(results[job][7])
        print "Number of Sensitivity Iterations: " + str(results[job][8])
        print "Sensitivity Batch Size: " + str(results[job][9])
        print "Sensitivity Start Date: " + str(results[job][10])
        print "Sensitivity End Date: " + str(results[job][11])
        print "Sensitivity Beginning Analysis Date: " + str(results[job][12])
        print "Sensitivity Complete Flag: " + str(results[job][13])
        print "Calibration Flag: " + str(results[job][14])
        print "Calibration Table: " + str(results[job][15])
        print "Calibration Start Date: " + str(results[job][16])
        print "Calibration End Date: " + str(results[job][17])
        print "Calibration Beginning Analysis Date: " + str(results[job][18])
        print "Number of Calibration Iterations: " + str(results[job][19])
        print "Calibration Complete Flag: " + str(results[job][20])
        print "Validation Start Date: " + str(results[job][21])
        print "Validation End Date: " + str(results[job][22])
        print "Validation Beginning Analysis Date: " + str(results[job][23])
        print "Validation Complete Flag: " + str(results[job][24])
        print "Account Key: " + str(results[job][25])
        print "Que Name: " + str(results[job][26])
        print "Number of Cores for the Model: " + str(results[job][27])
        print "Number of Nodes for the Model: " + str(results[job][28])
        print "Number of Cores for R: " + str(results[job][29])
        print "Number of Nodes for R: " + str(results[job][30])
        print "Job Run Type: " + str(results[job][31])
        print "Executable Path: " + str(results[job][32])
        print "Number of Gages: " + str(results[job][33])
        print "Owner: " + str(results[job][34])
        print "Email: " + str(results[job][35])
        print "Slack Channel: " + str(results[job][36])
        print "Slack Token: " + str(results[job][37])
        print "Slack User: " + str(results[job][38])
        print "Analysis Run Type: " + str(results[job][39])
        print "Analysis Que Name: " + str(results[job][40])
        print "--------------------------------------------------"
        
    # Close connection to DB
    try:
        conn.close()
    except:
        print "ERROR: Unable to close DB connection."
        sys.exit(1)

    
if __name__ == "__main__":
    main(sys.argv[1:])