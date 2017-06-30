# This is a top-level, self-contained Python program that will create the 
# necessary database tables necessary to run calibrations. It's up to the 
# user to establish a MySQL server on their end that the workflow can
# communicate with. Please see the documentation for more detailed information.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory.

#import MySQLdb
import argparse
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
libPathTop = libPath + 'lib'
libPath = libPath + 'lib/Python'
sys.path.insert(0,libPath)

import warnings
warnings.filterwarnings("ignore")

def main(argv):
    # Parse arguments. User must input a root password to initialize the proper
    # database, etc using the schema file.
    parser = argparse.ArgumentParser(description='DB Initialization Program')
    parser.add_argument('--hostName',metavar='hostName',type=str,nargs='?',
                        help='Optional hostname if MySQL server on different machine.')    

    args = parser.parse_args()  
    
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
        
    # Prompt user to enter in password for read-write access to DB being created. 
    # Be sure to let user know that they need to keep the password handy for future
    # access and calibration.
    print "PLEASE ENTER A PASSWORD FOR ACCESS TO THE CALIBRATION DATABASE."
    try:
        pwdUser1 = getpass.getpass('Create Database Password: ')
    except:
        print "ERROR: Error in parsing password."
        sys.exit(1)
        
    if not pwdUser1:
        print "ERROR: Improper User-Created Password."
        sys.exit(1)
        
    try:
        pwdUser2 = getpass.getpass('Re-Enter Database Password: ')
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
    schemaPathTmp = './schemaTmp.sql'
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
        
    if args.hostName:
        # An optional hostname was passed from the user, indicating we are 
        # initializing the DB off another system.
        cmd = "mysql -h " + args.hostName + " -u root -p'" + pwdTmp + "' < " + schemaPathTmp
    else:
        cmd = "mysql -u root -p'" + pwdTmp + "' < " + schemaPathTmp
     
    print cmd
    subprocess.call(cmd,shell=True)
    
    # Remove temporary schema file
    os.remove(schemaPathTmp)
    
if __name__ == "__main__":
    main(sys.argv[1:])