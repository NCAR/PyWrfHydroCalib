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

    print("Connecting to the Sqlite file.")
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

    print('Connecting to the external Postgres database.')
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

    print("Extracting domain ID values from sqlite file.")
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

    print("Extracting job ID values from the sqlite file.")
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
    print("Entering in domain information into the external postgres DB")
    for bsnTmp in resultsBasins:
        bsnIdUnique = int(str(expRef) + str(bsnTmp[0]))
        cmd = "select \"domainID\" from \"Domain_Meta\" where \"domainID\"='" + str(bsnIdUnique) + "';"
        try:
            dbCursorExt.execute(cmd)
            resultsTmp = dbCursorExt.fetchone()
        except:
            print("Unable to query external database for unique basin ID:" + str(bsnIdUnique))
            sys.exit(1)

        if resultsTmp == None:
            # We need to insert domain information into the external database. First
            # query the database file for the domain information.
            cmd = "select * from \"Domain_Meta\" where \"domainID\"='" + str(bsnTmp[0]) + "';"
            try:
                dbCursorIn.execute(cmd)
                resultsTmp = dbCursorIn.fetchall()
            except:
                print("Unable to query database file for domain ID: " + str(bsnTmp[0]))
                sys.exit(1)

            # Sanity checking here.
            if len(resultsTmp) != 1:
                print("Unexpected multiple basin entries for ID: " + str(bsnTmp[0]) +
                      " in external database file")
                sys.exit(1)
            if len(resultsTmp[0]) != 45:
                print("Unexpected basin information for ID: " + str(bsnTmp[0]) +
                      " in external database file")
                sys.exit(1)

            # Insert this information into the external database.
            cmd = "INSERT INTO \"Domain_Meta\" (\"domainID\",gage_id,link_id,domain_path,gage_agency,geo_e," + \
                  "geo_w,geo_s,geo_n,hyd_e,hyd_w,hyd_s,hyd_n,geo_file,land_spatial_meta_file,wrfinput_file," + \
                  "soil_file,fulldom_file,rtlink_file,spweight_file," + \
                  "gw_file,gw_mask,lake_file,forcing_dir,obs_file,site_name,lat,lon," + \
                  "area_sqmi,area_sqkm,county_cd,state,huc2,huc4,huc6,huc8,ecol3,ecol4,rfc," + \
                  "dx_hydro,agg_factor,hydro_tbl_spatial,opt_spin_land_path," + \
                  "opt_spin_hydro_path,chan_parm_path) VALUES " + \
                  "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                  "'%s','%s','%s','%s');" % (bsnIdUnique,resultsTmp[0][1],resultsTmp[0][2],resultsTmp[0][3],
                                             resultsTmp[0][4],resultsTmp[0][5],resultsTmp[0][6],resultsTmp[0][7],
                                             resultsTmp[0][8],resultsTmp[0][9],resultsTmp[0][10],resultsTmp[0][11],
                                             resultsTmp[0][12],resultsTmp[0][13],resultsTmp[0][14],resultsTmp[0][15],
                                             resultsTmp[0][16],resultsTmp[0][17],resultsTmp[0][18],resultsTmp[0][19],
                                             resultsTmp[0][20],resultsTmp[0][21],resultsTmp[0][22],resultsTmp[0][23],
                                             resultsTmp[0][24],resultsTmp[0][25],resultsTmp[0][26],resultsTmp[0][27],
                                             resultsTmp[0][28],resultsTmp[0][29],resultsTmp[0][30],resultsTmp[0][31],
                                             resultsTmp[0][32],resultsTmp[0][33],resultsTmp[0][34],resultsTmp[0][35],
                                             resultsTmp[0][36],resultsTmp[0][37],resultsTmp[0][38],resultsTmp[0][39],
                                             resultsTmp[0][40],resultsTmp[0][41],resultsTmp[0][42],resultsTmp[0][43],
                                             resultsTmp[0][44])
            try:
                dbCursorExt.execute(cmd)
                connExt.commit()
            except:
                print("Unable to enter in domain information for basin ID: " + str(bsnTmp[0]) +
                      " from external database.")
                sys.exit(1)

    # Loop through each of the calibration experiments listed in the sqlite file.
    # 1.) First check to see if the metadata has been entered in for this experiment.
    # 2.) Check to see if the basin information for this sql file has been entered.
    # 3.) Loop through the calibration tables and either enter information, or update
    #     fields that need to be updated.
    for expTmp in resultsExp:
        cmd = "select \"jobID\" from \"Job_Meta\" where \"jobID\"='" + str(expTmp[0]) + "';"
        try:
            dbCursorExt.execute(cmd)
            resultsTmp = dbCursorExt.fetchone()
        except:
            print("Unable to query job ID: " + expTmp + " from external postgres database.")
            sys.exit(1)

        if resultsTmp == None:
            # We haven't entered in job meta information.
            cmd = "select * from \"Job_Meta\" where \"jobID\"='" + str(expTmp[0]) + "';"
            try:
                dbCursorIn.execute(cmd)
                resultsTmp = dbCursorIn.fetchone()
            except:
                print("Unable to query job ID: " + str(expTmp[0]) + " from: " + dbInPath)
                sys.exit(1)

            # Sanity checking here.
            if len(resultsTmp) != 40:
                print("Unexpected job information for ID: " + str(expTmp[0]) +
                      " in external database file")
                sys.exit(1)

            print("Entering in information for experiment ID: " + str(expTmp[0]))
            # Insert this information into the external Job_Meta table.
            cmd = "insert into \"Job_Meta\" (\"jobID\",\"Job_Directory\",date_su_start,date_su_end," + \
                  "su_complete,sens_flag,sens_table,num_sens_sample,num_sens_iter,sens_batch,date_sens_start," \
                  "date_sens_end,date_sens_start_eval,sens_complete,calib_flag,calib_table,date_calib_start," \
                  "date_calib_end,date_calib_start_eval,num_iter," + \
                  "calib_complete,valid_start_date,valid_end_date,valid_start_date_eval," + \
                  "valid_complete,acct_key,que_name,num_cores_model,num_nodes_model,num_cores_per_node," + \
                  "job_run_type,exe,num_gages,owner,email," + \
                  "slack_channel,slack_token,slack_user,mpi_cmd,cpu_pin_cmd) values " + \
                  "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"\
                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                  "'%s');" % (expTmp[0],resultsTmp[1],resultsTmp[2],resultsTmp[3],resultsTmp[4],
                              resultsTmp[5],resultsTmp[6],resultsTmp[7],resultsTmp[8],
                              resultsTmp[9],resultsTmp[10],resultsTmp[11],resultsTmp[12],
                              resultsTmp[13],resultsTmp[14],resultsTmp[15],resultsTmp[16],
                              resultsTmp[17],resultsTmp[18],resultsTmp[19],resultsTmp[20],resultsTmp[21],
                              resultsTmp[22],resultsTmp[23],resultsTmp[24],resultsTmp[25],resultsTmp[26],
                              resultsTmp[27],resultsTmp[28],resultsTmp[29],resultsTmp[30],resultsTmp[31],
                              resultsTmp[32],resultsTmp[33],resultsTmp[34],resultsTmp[35],resultsTmp[36],
                              resultsTmp[37],resultsTmp[38],resultsTmp[39])

            try:
                dbCursorExt.execute(cmd)
                connExt.commit()
            except:
                print("Unable to enter in job meta information for basin ID: " + str(expTmp[0]) +
                      " from external database.")
                sys.exit(1)

        # Now, either enter information for calibration/sensitivity/validation into the various tables holding
        # the data. If the entry has already been created, update it. Otherwise, create a new entry.
        print("Extracting Sens_Params from sqlite file.")
        cmd = "select * from \"Sens_Params\" where \"jobID\"='" + str(expTmp[0]) + "';"
        try:
            dbCursorIn.execute(cmd)
            resultsTmp = dbCursorIn.fetchall()
        except:
            print("Unable to query Sens_Params for job ID: " + expTmp + " from sqlite file.")
            sys.exit(1)
        if len(resultsTmp) != 0:
            # We have data to either enter into the postgres DB, or need to update.
            for entryTmp in resultsTmp:
                bsnIdUnique = int(str(entryTmp[0]) + str(entryTmp[1]))
                # First check to see if we need to run an INSERT or an UPDATE.
                cmd = "select * from \"Sens_Params\" where \"jobID\"=" + str(entryTmp[0]) + " and \"domainID\"=" + \
                      str(bsnIdUnique) + " and iteration=" + str(entryTmp[2]) + " and \"paramName\"='" + str(entryTmp[3]) + \
                      "';"
                try:
                    dbCursorExt.execute(cmd)
                    updatesTmp = dbCursorExt.fetchone()
                except:
                    print("Unable to run query on Sens_Params external.....")
                    sys.exit(1)
                if updatesTmp == None:
                    print("Inserting data into the postgres database...")
                    # This is a new entry, we need run an INSERT.
                    cmd = "insert into \"Sens_Params\" (\"jobID\",\"domainID\",iteration,\"paramName\",\"paramValue\") " \
                          "values ('%s','%s','%s','%s','%s'); " % (str(entryTmp[0]),str(bsnIdUnique),str(entryTmp[2]),
                                                                   str(entryTmp[3]),str(entryTmp[4]))
                    try:
                        dbCursorExt.execute(cmd)
                        connExt.commit()
                    except:
                        print("Unable to insert data into external Sens_Params table.")
                        sys.exit(1)
                else:
                    print("Updating entry on postgres database entry....")
                    # We are running an UPDATE on an existing entry.
                    cmd = "update \"Sens_Params\" set \"jobID\"='" + entryTmp[0] + "', \"domainID\"='" + \
                          str(bsnIdUnique) + "', iteration='" + entryTmp[2] + "', \"paramName\"='" + str(entryTmp[3]) + \
                          "', \"paramValue\"='" + str(entryTmp[4]) + "' where \"jobID\"='" + str(updatesTmp[0]) + \
                          "' and \"domainID\"='" + str(bsnIdUnique) + "' and iteration='" + str(updatesTmp[2]) + \
                          "' and \"paramName\"='" + str(updatesTmp[3]) + "' and \"paramValue\"='" + str(updatesTmp[4]) + "';"
                    try:
                        dbCursorExt.execute(cmd)
                        connExt.commit()
                    except:
                        print("Unable to update entry in external Sens_Params table.")
                        sys.exit(1)
        else:
            print("We have no information to update or enter for Sens_Params.")



        cmd = "select * from \"Sens_Stats\" where \"jobID\"='" + str(expTmp[0]) + "';"
        try:
            dbCursorIn.execute(cmd)
            resultsTmp = dbCursorIn.fetchall()
        except:
            print("Unable to query Sens_Stats for job ID: " + expTmp + " from sqlite file.")
            sys.exit(1)





        print("Extracting Calib_Params information from the sqlite file.")
        cmd = "select * from \"Calib_Params\" where \"jobID\"='" + str(expTmp[0]) + "';"
        try:
            dbCursorIn.execute(cmd)
            resultsTmp = dbCursorIn.fetchall()
        except:
            print("Unable to query Calib_Params for job ID: " + expTmp + " from sqlite file.")
            sys.exit(1)
        if len(resultsTmp) != 0:
            # We have data to either enter into the postgres DB, or need to update.
            for entryTmp in resultsTmp:
                bsnIdUnique = int(str(entryTmp[0]) + str(entryTmp[1]))
                # First check to see if we need to run an INSERT or an UPDATE.
                cmd = "select * from \"Calib_Params\" where \"jobID\"=" + str(entryTmp[0]) + " and \"domainID\"=" + \
                      str(bsnIdUnique) + " and iteration=" + str(entryTmp[2]) + " and \"paramName\"='" + str(entryTmp[3]) + \
                      "';"
                try:
                    dbCursorExt.execute(cmd)
                    updatesTmp = dbCursorExt.fetchone()
                except:
                    print("Unable to run query on Calib_Params external.....")
                    sys.exit(1)
                if updatesTmp == None:
                    # This is a new entry, we need run an INSERT.
                    print("Inserting data into the postgres database...")
                    cmd = "insert into \"Calib_Params\" (\"jobID\",\"domainID\",iteration,\"paramName\",\"paramValue\") " \
                          "values ('%s','%s','%s','%s','%s'); " % (str(entryTmp[0]),str(bsnIdUnique),str(entryTmp[2]),
                                                                   str(entryTmp[3]),str(entryTmp[4]))
                    try:
                        dbCursorExt.execute(cmd)
                        connExt.commit()
                    except:
                        print("Unable to insert data into external Calib_Params table.")
                        sys.exit(1)
                else:
                    print("Updating entry on postgres database entry....")
                    # We are running an UPDATE on an existing entry.
                    cmd = "update \"Calib_Params\" set \"jobID\"='" + str(entryTmp[0]) + "', \"domainID\"='" + \
                          str(bsnIdUnique) + "', iteration='" + str(entryTmp[2]) + "', \"paramName\"='" + str(entryTmp[3]) + \
                          "', \"paramValue\"='" + str(entryTmp[4]) + "' where \"jobID\"='" + str(updatesTmp[0]) + \
                          "' and \"domainID\"='" + str(bsnIdUnique) + "' and iteration='" + str(updatesTmp[2]) + \
                          "' and \"paramName\"='" + str(updatesTmp[3]) + "' and \"paramValue\"='" + str(updatesTmp[4]) + "';"
                    try:
                        dbCursorExt.execute(cmd)
                        connExt.commit()
                    except:
                        print("Unable to update entry in external Calib_Params table.")
                        sys.exit(1)
        else:
            print("We have no information to update or enter for Sens_Params.")




        cmd = "select * from \"Calib_Stats\" where \"jobID\"='" + str(expTmp[0]) + "';"
        try:
            dbCursorIn.execute(cmd)
            resultsTmp = dbCursorIn.fetchall()
        except:
            print("Unable to query Calib_Stats for job ID: " + expTmp + " from sqlite file.")
            sys.exit(1)






        cmd = "select * from \"Valid_Stats\" where \"jobID\"='" + str(expTmp[0]) + "';"
        try:
            dbCursorIn.execute(cmd)
            resultsTmp = dbCursorIn.fetchall()
        except:
            print("Unable to query Valid_Stats for job ID: " + expTmp + " from sqlite file.")
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