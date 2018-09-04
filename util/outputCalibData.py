# Utility program to dumping calibration/validation parameters/statistics
# for a given job experiment ID value to a NetCDF file.

# Logan Karsten
# Natinonal Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu

import sqlite3
import argparse
import os
import sys
from netCDF4 import Dataset

def main(argv):
    # Parse arguments. User must input a jobID, the DB file to pull from. Optionally,
    # the user may provide an alternative output path for the NetCDF file.
    parser = argparse.ArgumentParser(description='Utility for dumping calibration ' + \
                                     ' and validation data to a NetCDF file.')
    parser.add_argument('jobID',metavar='jobID',type=int,nargs='+',
                        help='Required job ID to pull from')
    parser.add_argument('inDB',metavar='inDB',type=str,nargs='+',
                        help='Required path to sqllite3 DB file.')
    parser.add_argument('--optOutPath',type=str,nargs='?',
                        help='Optional output path for NetCDF file.')

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
        
    # First, pull the number of gages and iterations from the Job_Meta table. This will
    # help determine how many data entries to expect.
    sqlCmd = "SELECT num_iter from \"Job_Meta\" where \"jobID\"=%s;" % args.jobID[0]
    try:
        dbCursor.execute(sqlCmd)
        results = dbCursor.fetchone()
    except:
        print "ERROR: Unable to execute SQL command: " + sqlCmd
        sys.exit(1)
        
    if len(results) == 0:
        print "ERROR: User-provided Job ID: " + str(args.jobID[0]) + " returned no results. " + \
              " Please confirm job has been initialized."
        sys.exit(1)
        
    numIter = int(results[0])
    
    sqlCmd = "SELECT num_gages from \"Job_Meta\" where \"jobID\"=%s;" % args.jobID[0]
    try:
        dbCursor.execute(sqlCmd)
        results = dbCursor.fetchone()
    except:
        print "ERROR: Unable to execute SQL command: " + sqlCmd
        sys.exit(1)
        
    if len(results) == 0:
        print "ERROR: User-provided Job ID: " + str(args.jobID[0]) + " returned no results. " + \
              " Please confirm job has been initialized."
        sys.exit(1)
    
    numGages = int(results[0])
    #numCalibStatEntries = numIter*numGages
    
    # Next, do a sample pull from Calib_Params for a given domain ID within this job ID
    # and determine how many parameters are being calibrated. We will need this number
    # for the output file. 
    sqlCmd = "SELECT \"domainID\" from \"Calib_Params\" where \"jobID\"=%s;" % args.jobID[0]
    try:
        dbCursor.execute(sqlCmd)
        results = dbCursor.fetchone()
    except:
        print "ERROR: Unable to execute SQL command: " + sqlCmd
        sys.exit(1)
        
    if len(results) == 0:
        print "ERROR: User-provided Job ID: " + str(args.jobID[0]) + " returned no results. " + \
              " Please confirm job has been initialized."
        sys.exit(1)
        
    sqlCmd = "SELECT * from \"Calib_Params\" where \"jobID\"=%s and \"domainID\"=%s and iteration=1;" % (args.jobID[0],int(results[0]))
    try:
        dbCursor.execute(sqlCmd)
        results = dbCursor.fetchall()
    except:
        print "ERROR: Unable to execute SQL command: " + sqlCmd
        sys.exit(1)
        
    if len(results) == 0:
        print "ERROR: User-provided Job ID: " + str(args.jobID[0]) + " returned no results. " + \
              " Please confirm job has been initialized."
        sys.exit(1)
        
    numCalibParams = len(results)
        
    # Create the ouptut NetCDF file that will contain output.
    if args.optOutPath:
        outPath = args.optOutPath
    else:
        outPath = "./CalibrationData_Job_" + str(args.jobID[0]) + ".nc"
        
    idOut = Dataset(outPath,'w')
    
    # Create dimensions
    idOut.createDimension('numGages',numGages)
    idOut.createDimension('numIterations',numIter)
    idOut.createDimension('numParams',numCalibParams)
    idOut.createDimension('gageStrLen',30)
    
    # Create a gage variable that will contain the gage string for each domain.
    idOut.createVariable("gage","S1",("numGages","gageStrLen"))
    
    # Loop through, extract the gage ID and place it into the gVar
    sqlCmd = "SELECT \"domainID\" from \"Calib_Stats\" where \"jobID\"=%s and iteration=1;" % args.jobID[0]
    try:
        dbCursor.execute(sqlCmd)
        jobGageIDs = dbCursor.fetchall()
    except:
        print "ERROR: Unable to execute SQL command: " + sqlCmd
        sys.exit(1)
        
    if numGages != len(jobGageIDs):
        print "ERROR: Number of gages found in Calib_Stats does not match what is in Job_Meta."
        idOut.close()
        if os.path.isfile(outPath):
            os.remove(outPath)
        sys.exit(1)
        
    for i in range(0,numGages):
        sqlCmd = "SELECT gage_id from \"Domain_Meta\" where \"domainID\"=%s;" % jobGageIDs[i][0]
        try:
            dbCursor.execute(sqlCmd)
            gageIdTmp = dbCursor.fetchone()
        except:
            print "ERROR: Unable to extract gage_id for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
        idOut.variables['gage'][i,:] = gageIdTmp[0]
        
    # Create the statistic variables that will contain data for each gage, for all iterations.
    idOut.createVariable("calibBias","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibRmse","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibCorrelation","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibNse","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibNseLog","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibKge","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibFdc","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibMsof","f8",("numGages","numIterations"),fill_value=-9999)
    idOut.createVariable("calibBest","f8",("numGages","numIterations"),fill_value=-9999)
    
    # Loop through the calibration parameters, extract information for each 
    # Close the NetCDF output file.
    for i in range(0,numGages):
        sqlCmd = "SELECT bias from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsBias = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract bias stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT rmse from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsRmse = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract rmse stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT cor from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsCor = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract correlation stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT nse from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsNse = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract nse stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT nselog from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsNseLog = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract nselog stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT kge from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsKge = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract kge stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT fdcerr from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsFdcerr = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract fdcerr stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT msof from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsMsof = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract msof stats for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        sqlCmd = "SELECT best from \"Calib_Stats\" where \"jobID\"=%s and \"domainID\"=%s;" % (args.jobID[0],jobGageIDs[i][0])
        try:
            dbCursor.execute(sqlCmd)
            resultsBest = dbCursor.fetchall()
        except:
            print "ERROR: Unable to extract bias best iteration for domainID: " + str(jobGageIDs[i][0])
            idOut.close()
            if os.path.isfile(outPath):
                os.remove(outPath)
            sys.exit(1)
            
        # Place statistics for gage into the NetCDF variables accordingly.
        for j in range(0,numIter):
            idOut.variables['calibBias'][i,j] = resultsBias[j][0]
            idOut.variables['calibRmse'][i,j] = resultsRmse[j][0]
            idOut.variables['calibCorrelation'][i,j] = resultsCor[j][0]
            idOut.variables['calibNse'][i,j] = resultsNse[j][0]
            idOut.variables['calibNseLog'][i,j] = resultsNseLog[j][0]
            idOut.variables['calibKge'][i,j] = resultsKge[j][0]
            idOut.variables['calibFdc'][i,j] = resultsFdcerr[j][0]
            idOut.variables['calibMsof'][i,j] = resultsMsof[j][0]
            idOut.variables['calibBest'][i,j] = resultsBest[j][0]
            
    idOut.close()
    
if __name__ == "__main__":
    main(sys.argv[1:])