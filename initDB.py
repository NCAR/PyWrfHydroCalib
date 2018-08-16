# This is a top-level, self-contained Python program that will create the 
# necessary database tables necessary to run calibrations. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory.

import sys
import os
import argparse
import sqlite3

# Set the Python path to include package specific functions included with this 
# package.
prPath = os.path.realpath(__file__)
pathSplit = prPath.split('/')
libPath = '/'
for j in range(1,len(pathSplit)-1):
    libPath = libPath + pathSplit[j] + '/'
topDir = libPath
schemaFile2 = libPath + 'setup_files/psql_schema2.sql'
libPathTop = libPath + 'lib'
libPath = libPath + 'lib/Python'
sys.path.insert(0,libPath)

import warnings
warnings.filterwarnings("ignore")

def main(argv):
    # Optional hostname for the user to pass.
    parser = argparse.ArgumentParser(description='Main program to initialize the wrfHydroCalib_DB')
    parser.add_argument('--optDbPath',type=str,nargs='?',
                        help='Optional alternative path to SQLite DB file.')
    args = parser.parse_args()
                    
    # If the optional SQLite file already exists, throw an error.
    if args.optDbPath is not None:
        if os.path.isfile(args.optDbPath):
            print "ERROR: " + args.optDbPath + " Already Exists."
            sys.exit(1)
        else:
            dbPath = args.optDbPath
    else:
        dbPath = topDir + "wrfHydroCalib.db"
        if os.path.isfile(dbPath):
            print "ERROR: SQLite3 DB file: " + dbPath + " Already Exists."
            sys.exit(1)
            
    # Create a connnection object.
    dbConn = sqlite3.connect(dbPath)
    
    # Go through and create tables
    try:
        dbConn.execute('''CREATE TABLE Calib_Params
                       (jobID integer, domainID integer, iteration integer,
                       paramName text, paramValue real)''')
        dbConn.commit()
    except:
        errOut(dbConn,"Unable to create table: Calib_Params.",dbPath)
        
    try:
        dbConn.execute('''CREATE TABLE Sens_Params
                       (jobID integer, domainID integer, iteration integer,
                       paramName text, paramValue real)''')
    except:
        errOut(dbConn,"Unable to create table: Sens_Params.",dbPath)
    
    try:
        dbConn.execute('''CREATE TABLE Calib_Stats
                       (jobID integer, domainID integer, iteration integer,
                       objfnVal real, bias real, rmse real, cor real,
                       nse real, nselog real, kge real, fdcerr real,
                       msof real, best integer, complete real)''')
    except:
        errOut(dbConn,"Unable to create table: Calib_Stats.",dbPath)
        
    try:
        dbConn.execute('''CREATE TABLE Sens_Stats
                       (jobID integer, domainID integer, iteration integer,
                       objfnVal real, bias real, rmse real, cor real,
                       nse real, nselog real, kge real, fdcerr real,
                       msof real, timestep text, complete real)''')
    except:
        errOut(dbConn,"Unable to create table: Sens_Stats.",dbPath)
        
    try:
        dbConn.execute('''CREATE TABLE Domain_Meta
                       (domainID INTEGER PRIMARY KEY, gage_id text, link_id integer,
                       domain_path text, gage_agency text, geo_e integer,
                       geo_w integer, geo_s integer, geo_n integer,
                       hyd_e integer, hyd_w integer, hyd_s integer,
                       hyd_n integer, geo_file text, land_spatial_meta_file text,
                       wrfinput_file text, soil_file text, fulldom_file text,
                       rtlink_file text, spweight_file text, gw_file text,
                       gw_mask text, lake_file text, forcing_dir text, obs_file text,
                       site_name text, lat real, lon real, area_sqmi real,
                       area_sqkm real, county_cd text, state text, huc2 text,
                       huc4 text, huc6 text, huc8 text, ecol3 text, ecol4 text,
                       rfc text, dx_hydro real, agg_factor integer, hydro_tbl_spatial text)''')
    except:
        errOut(dbConn,"Unable to create table: Domain_Meta.",dbPath)
        
    try:
        dbConn.execute('''CREATE TABLE Job_Meta 
                       (jobID INTEGER PRIMARY KEY, Job_Directory text, date_su_start timestamp,
                       date_su_end timestamp, su_complete integer, sens_flag integer,
                       sens_table text, num_sens_sample integer, num_sens_iter integer,
                       sens_batch integer, date_sens_start timestamp, date_sens_end timestamp,
                       date_sens_start_eval timestamp, sens_complete integer,
                       calib_flag integer, calib_table text, date_calib_start timestamp,
                       date_calib_end timestamp, date_calib_start_eval timestamp,
                       num_iter integer, calib_complete integer, valid_start_date timestamp,
                       valid_end_date timestamp, valid_start_date_eval timestamp,
                       valid_complete integer, acct_key text, que_name text,
                       num_cores_model integer, num_nodes_model integer,
                       num_cores_R integer, num_nodes_R integer,
                       job_run_type integer, exe text, num_gages integer,
                       owner text, email text, slack_channel text, slack_token text,
                       slack_user text, analysis_run_type integer, que_name_analysis text)''')
    except:
        errOut(dbConn,"Unablet to create table: Job_Meta.",dbPath)
    
    try:
        dbConn.execute('''CREATE TABLE Job_Params
                       (jobID integer, param text, defaultValue real, min real,
                       max real, sens_flag integer, calib_flag integer)''')
    except:
        errOut(dbConn,"Unable to create table: Job_Params.",dbPath)
        
    try:
        dbConn.execute('''CREATE TABLE Valid_Stats
                        (jobID integer, domainID integer, simulation text,
                        evalPeriod text, objfnVal real, bias real, rmse real,
                        cor real, nse real, nselog real, nseWt real, kge real,
                        msof real)''')
    except:
        errOut(dbConn,"Unable to create table: Valid_Stats.",dbPath)
    
    # Close the database file
    try:
        dbConn.close()
    except:
        errOut(dbConn,'Unable to close file: ' + dbPath,dbPath)
                        
def errOut(dbConn,errMsg,dbPath):
    """"
    Quick function to close out the DB file and report an error to the user on the screen.
    """
    print "ERROR: " + errMsg
    try:
        dbConn.close()
    except:
        print "ERROR: Unable to close DB file properly. Defaulting to removing the file."
        
    try:
        os.remove(dbPath)
    except:
        print "ERROR: Unable to remove the DB file."
        sys.exit(1)
    
if __name__ == "__main__":
    main(sys.argv[1:])