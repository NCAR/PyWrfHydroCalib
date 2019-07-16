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
#import psycopg2

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

    # First grab the experiment ID values associated with this
    cmd = "select \"jobID\" from \"Job_Meta\";"
    try:
        dbCursorIn.execute(cmd)
        results = dbCursorIn.fetchall()
    except:
        print('Unable to query: ' + dbCursorIn + ' for calibration experiment ID values.')
        sys.exit(1)

    if results == None:
        print("No job data found in database file: " + dbInPath)
        sys.exit(1)

    # Loop through each of the calibration experiments listed in the sqlite file.
    # 1.) First check to see if the metadata has been entered in for this experiment.
    # 2.) Check to see if the basin information for this sql file has been entered.
    # 3.) Loop through the calibration tables and either enter information, or update
    #     fields that need to be updated.
    for expTmp in results:
        cmd = "select \"jobID\" from \"Job_Meta\" where \"jobID\"='" + str(expTmp) + "';"
        try:
            dbCursorIn.execute(cmd)
            results = dbCursorIn.fetchone()
        except:
            print("Unable to query job ID: " + expTmp + " from external postgres database.")
            sys.exit(1)

        if results == None:
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