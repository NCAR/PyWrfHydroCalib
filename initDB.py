# This is a top-level, self-contained Python program that will create the 
# necessary database tables necessary to run calibrations. It's up to the 
# user to establish a Postgres server on their end that the workflow can
# communicate with. Please see the documentation for more detailed information.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory.

import psycopg2
import sys
import os
import getpass
import argparse

# Set the Python path to include package specific functions included with this 
# package.
prPath = os.path.realpath(__file__)
pathSplit = prPath.split('/')
libPath = '/'
for j in range(1,len(pathSplit)-1):
    libPath = libPath + pathSplit[j] + '/'
schemaFile2 = libPath + 'setup_files/psql_schema2.sql'
libPathTop = libPath + 'lib'
libPath = libPath + 'lib/Python'
sys.path.insert(0,libPath)

import warnings
warnings.filterwarnings("ignore")

def main(argv):
    # Optional hostname for the user to pass.
    parser = argparse.ArgumentParser(description='Main program to initialize the wrfHydroCalib_DB')
    parser.add_argument('--hostname',type=str,nargs='?',
                        help='Optional hostname MySQL DB resides on. Will use localhost if not passed.')
    args = parser.parse_args()
                    
    if not args.hostname:
        # We will assume localhost for Postgres DB
        hostTmp = 'localhost'
    else:
        hostTmp = str(args.hostname)
                        
    # Obtain root password from user for the MySQL DB. This is necessary to 
    # create the necessary DB and associated tables.
    try:
        pwdTmp = getpass.getpass('Enter Database Root Password: ')
    except:
        print "ERROR: Unable to authenticate credentials for database."
        sys.exit(1)
    
    if not pwdTmp:
        print "ERROR: Improper postgres root password provided."
        sys.exit(1)
        
    # Check to see if this DB has already been created. If it has, throw an 
    # error back to the user. 
    try:
        #strTmp = "dbname=postgres user=postgres password=" + pwdTmp + " port=5432 host=localhost"
        strTmp = "dbname=postgres user=postgres password=" + pwdTmp + " port=5432 host=" + hostTmp
        db = psycopg2.connect(strTmp)
    except:
        print "ERROR: Unable to connect to postgres as user root. It's possible you entered an incorrect password."
        sys.exit(1)
    db.autocommit = True
    conn = db.cursor()
    sqlCmd = 'SELECT datname FROM pg_database;'
    conn.execute(sqlCmd)
    qResult = conn.fetchall()
    nResults = len(qResult)
    for i in range(0,nResults):
        if qResult[i][0] == 'wrfHydroCalib_DB':
            conn.close()
            print "ERROR: wrfHydroCalib_DB Database already exists. Please remove before re-running this program."
            sys.exit(1)
    sqlCmd = 'SELECT usename FROM pg_user;'
    conn.execute(sqlCmd)
    qResult = conn.fetchall()
    nResults = len(qResult)
    for i in range(0,nResults):
        if qResult[i][0] == 'WH_Calib_rw':
            conn.close()
            print "ERROR: WH_Calib_rw User already exists. Please remove before re-running this program."
            sys.exit(1)
            
    # Prompt user to enter in password for read-write access to DB being created. 
    # Be sure to let user know that they need to keep the password handy for future
    # access and calibration.
    try:
        pwdUser1 = getpass.getpass('Create New Database Password: ')
    except:
        print "ERROR: Error in parsing password."
        sys.exit(1)
        
    if not pwdUser1:
        print "ERROR: Improper User-Created Password."
        sys.exit(1)
        
    try:
        pwdUser2 = getpass.getpass('Re-Enter New Database Password: ')
    except:
        print "ERROR: Error in parsing password."
        sys.exit(1)
        
    if not pwdUser2:
        print "ERROR: Improper User-Created Re-Entered Password."
        sys.exit(1)
        
    if pwdUser1 != pwdUser2:
        print "ERROR: Password Re-Entered Does Not Match First Password Created."
        sys.exit(1)
        
    # STEP 1: Create proper database and user per user input.
    cmd = "CREATE USER \"WH_Calib_rw\" WITH PASSWORD \'" + pwdUser1 + "\';"
    try:
        conn.execute(cmd)
    except:
        print "ERROR: Failure to create DB User WH_Calib_rw."
        sys.exit(1)
    cmd = "CREATE DATABASE \"wrfHydroCalib_DB\" OWNER \'WH_Calib_rw\' CONNECTION_LIMIT 5;"
    try:
        conn.execute(cmd)
    except:
        print "ERROR: Failure to create DB wrfHydroCalib_DB."
        sys.exit(1)
        
    try:
        conn.close()
    except:
        print "ERROR: Unable to disconnect from postgres as user postgres."
        sys.exit(1)
    
    # Re-connect to the new database created with the user-provided password.
    # Then create the necessary tables. 
    #strTmp = "dbname=wrfHydroCalib_DB user=WH_Calib_rw password=" + pwdUser1 + " port=5432 host=localhost"
    strTmp = "dbname=wrfHydroCalib_DB user=WH_Calib_rw password=" + pwdUser1 + " port=5432 host=" + hostTmp
    try:
        db = psycopg2.connect(strTmp)
    except:
        print "ERROR; Failure to establish connection with newly created wrfHydroCalib_DB."
        sys.exit(1)
    db.autocommit = True
    conn = db.cursor()
    
    # Load schema file in to create database tables.
    conn.execute(open(schemaFile2,'r').read())
    #try:
    #    conn.execute(open(schemaFile2,'r').read())
    #except:
    #    print "ERROR: Unable to load schema table to create tables for wrfHydroCalib_DB."
    #    sys.exit(1)
    
    try:
        conn.close()
    except:
        print "ERROR: Unable to disconnect from postgres as user postgres."
        sys.exit(1)
    
if __name__ == "__main__":
    main(sys.argv[1:])