# Utility program for backing up database files containing
# database tables of calibration statistics, along with
# parameter values, and basin information. This was created for
# NCAR-specific activities, but can be used by anyone who wishes
# to backup their calibration DB files to a Postgress DB.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu

import sys
import os
import argparse
import sqlite3
 import psycopg2

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Utility program for backing up database files to an '
                                                 'external SQL database')
    parser.add_argument('dbPath', metavar='dbPath', type=str, nargs='+',
                        help='Path to database file to backup.')
    parser.add_argument('host', metavar='host', type=str, nargs='+',
                        help='Name of external SQL host to back up to.')
    parser.add_argument('dbName', metavar='dbName', type=str, nargs='+',
                        help='Name of external SQL database to backup to.')
    parser.add_argument('dbUserName', metavar='dbUserName', type=str, nargs='+',
                        help='Name of database user name.')
    parser.add_argument('dbPwd', metavar='dbPwd', type=str, nargs='+',
                        help='Database password.')

    args = parser.parse_args()

    dbInPath = args.dbPath[0]

    # Connect to the input sqlite database file.
    if not os.path.isfile(dbInPath):
        print('Unable to locate specified input file: ' + dbInPath)
        sys.exit(1)

    # Open the DB file.
    try:
        connIn = sqlite3.connect(dbInPath)
    except:
        print('Unable to connect to sqlite file: ' + dbInPath)
        sys.exit(1)

    # Establish a cursor object.
    try:
        dbCursorIn = connIn.cursor()
    except:
        print('Unable to establish cursor object for: ' + dbInPath)
        sys.exit(1)

    # Connect to the external postgress DB.
    strTmp = "dbname='" + args.dbName[0] + "' user='" + args.dbUserName[0] + "' host='" + args.host[0] + \
             "' password='" + args.dbPwd[0] + "'"
    try:
        connExt = psycopg2.connect(strTmp)
    except:
        print("Unable to connect to external postgres database.")
        sys.exit(1)

    # Establish a cursor object for the external postgres database.
    try:
        dbCursorExt = connExt.cursor()
    except:
        print("Unable to extablish cursor object for external postgres database.")
        sys.exit(1)

    # Pull out the local basin ID values for this database file.
    cmd = "select \"domainID\" from \"Domain_Meta\";"
    try:
        dbCursorIn.execute(cmd)
        resultsBasins = dbCursorIn.fetchall()
    except:
        print('Unable to query: ' + dbCursorIn + ' for basin ID values.')
        sys.exit(1)

    if len(resultsBasins) == 0:
        print("No basins found in database file: " + dbInPath)
        sys.exit(1)

    # Grab the experiment ID values associated with this database file.
    cmd = "select \"jobID\" from \"Job_Meta\";"
    try:
        dbCursorIn.execute(cmd)
        resultsExp = dbCursorIn.fetchall()
    except:
        print('Unable to query: ' + dbCursorIn + ' for calibration experiment ID values.')
        sys.exit(1)

    if len(resultsExp) == 0:
        print("No job data found in database file: " + dbInPath)
        sys.exit(1)

    # Loop through each of the basins in this database file. Domain ID values
    # listed on the external database will utilize the domainID value in the
    # database file, along with the first jobID found in this database file.
    # It is up to the user to ensure unique job ID values exist for different
    # files if multiple files are being backed up.
    expRef = resultsExp[0][0]
    for bsnTmp in resultsBasins:
        bsnIdUnique = int(str(expRef) + str(bsnTmp[0]))
        cmd = "select \"domainID\" from \"Domain_Meta\" where \"domainID\"='" + str(bsnIdUnique) + "';"
        try:
            dbCursorExt.execute(cmd)
            resultsTmp = dbCursorExt.fetchone()
        except:
            print("Unable to query external database for unique basin ID:" + str(bsnIdUnique))
            sys.exit(1)

        if len(resultsTmp) == 0:
            # We need to insert domain information into the external database.
            cmd = "select * from \"Domain_Meta\" where \"domainID\"='" + str(bsnTmp[0]) + "';"
            try:
                dbCursorIn.execute(cmd)
                resultsTmp = dbCursorIn.fetchall()
            except:
                print("Unable to query database file for domain ID: " + str(bsnTmp[0]))
                sys.exit(1)

    # Loop through each of the calibration experiments listed in the sqlite file.
    # 1.) First check to see if the metadata has been entered in for this experiment.
    # 2.) Check to see if the basin information for this sql file has been entered.
    # 3.) Loop through the calibration tables and either enter information, or update
    #     fields that need to be updated.
    for expTmp in resultsExp:
        cmd = "select \"jobID\" from \"Job_Meta\" where \"jobID\"='" + str(expTmp) + "';"
        try:
            dbCursorIn.execute(cmd)
            results = dbCursorIn.fetchone()
        except:
            print("Unable to query job ID: " + expTmp + " from external postgres database.")
            sys.exit(1)

        if len(results) == 0:
            # We haven't entered in job meta information.
            cmd = "select * from \"Job_Meta\" where \"jobID\"='" + str(expTmp) + "';"
            try:
                dbCursorIn.execute(cmd)
                results = dbCursorIn.fetchone()
            except:
                print("Unable to query job ID: " + str(expTmp) + " from: " + dbInPath)
                sys.exit(1)

    # Close the sqlite connection.
    try:
        connIn.close()
    except:
        print('Unable to close connection to: ' + dbInPath)

    # Close the connection to the postgres database.
    try:
        connExt.close()
    except:
        print("Unable to to close connection to external postgres database.")
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv[1:])