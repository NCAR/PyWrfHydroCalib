# This is a top-level, self-contained Python program that will create the 
# necessary database tables necessary to run calibrations. It's up to the 
# user to establish a MySQL server on their end that the workflow can
# communicate with. Please see the documentation for more detailed information.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory.

import MySQLdb
import subprocess
import sys
import os
import getpass
import fileinput
import shutil

# Set the Python path to include package specific functions included with this 
# package.
prPath = os.path.realpath(__file__)
pathSplit = prPath.split('/')
libPath = '/'
for j in range(1,len(pathSplit)-1):
    libPath = libPath + pathSplit[j] + '/'
schemaFile = libPath + 'setup_files/schema.sql'
schemaPathTmp = libPath + "setup_files/schemaTmp.sql"
libPathTop = libPath + 'lib'
libPath = libPath + 'lib/Python'
sys.path.insert(0,libPath)

import warnings
warnings.filterwarnings("ignore")

def main(argv):
    # Obtain root password from user for the MySQL DB. This is necessary to 
    # create the necessary DB and associated tables.
    try:
        pwdTmp = getpass.getpass('Enter Database Root Password: ')
    except:
        print "ERROR: Unable to authenticate credentials for database."
        sys.exit(1)
    
    if not pwdTmp:
        print "ERROR: Improper MySQL root password provided."
        sys.exit(1)
        
    # Check to see if this DB has already been created. If it has, throw an 
    # error back to the user. 
    try:
        db = MySQLdb.connect('localhost','root',pwdTmp)
    except:
        print "ERROR: Unable to connect to MySQL as user root. It's possible you entered an incorrect password."
        sys.exit(1)
    conn = db.cursor()
    sqlCmd = 'show databases;'
    conn.execute(sqlCmd)
    qResult = conn.fetchall()
    nResults = len(qResult)
    for i in range(0,nResults):
        if qResult[i][0] == 'NWM_Calib_DB':
            conn.close()
            print "ERROR: NWM_Calib_DB Database already exists. Please remove before re-running this program."
            sys.exit(1)
    sqlCmd = 'select user from mysql.user;'
    conn.execute(sqlCmd)
    qResult = conn.fetchall()
    nResults = len(qResult)
    for i in range(0,nResults):
        if qResult[i][0] == 'NWM_Calib_rw':
            conn.close()
            print "ERROR: NWM_Calib_rw User already exists. Please remove before re-running this program."
            sys.exit(1)
    try:
        conn.close()
    except:
        print "ERROR: Unable to disconnect from MySQL as user root."
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
        
    if not os.path.isfile(schemaFile):
        print "ERROR: The necessary input schema file: " + schemaFile + " not found."
        sys.exit(1)

    # Make a temporary copy of the schema file. Replace the temporary password
    # in the temporary file with the password provided by the user.      
    if os.path.isfile(schemaPathTmp):
        os.remove(schemaPathTmp)
    searchStr = "PWD_TMP"
    replaceStr = "'" + pwdUser1 + "'"
    shutil.copyfile(schemaFile,schemaPathTmp)
    check = 0
    for line in fileinput.input(schemaPathTmp,inplace=1):
        if searchStr in line:
            if check == 0:
                line = line.replace(searchStr,replaceStr)
                check = check + 1
            else:
                check = check + 1
        sys.stdout.write(line)
        
    cmd = "mysql -u root -p'" + pwdTmp + "' < " + schemaPathTmp
     
    subprocess.call(cmd,shell=True)
    
    # Remove temporary schema file
    os.remove(schemaPathTmp)
    
if __name__ == "__main__":
    main(sys.argv[1:])