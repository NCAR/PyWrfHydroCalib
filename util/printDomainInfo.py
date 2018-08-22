# Utility program for dumping metadata for the basin domains
# that have been entered into the DB. The user has an optional
# argument for dumping information to screen for a specific basin. 

# Logan Karsten
# National Center for Atmospheric Research 
# Research Applications Laboratory

import sqlite3
import argparse
import os
import sys

# Establish top-level paths that are used to find the DB file. 
#prPath = os.path.realpath(__file__)
#pathSplit = prPath.split('/')
#libPath = '/'
#for j in range(1,len(pathSplit)-1):
#    libPath = libPath + pathSplit[j] + '/'
#topDir = libPath

def main(argv):
    # Parse arguments. User must input a path to the sqllite DB file.
    parser = argparse.ArgumentParser(description='Utility for printing information ' + \
             ' on a specific basin, or all basins in the DB')
    parser.add_argument('inDB',metavar='inDB',type=str,nargs='+',
                        help='Required path to sqllite3 DB file.')
    parser.add_argument('--optGage',type=str,nargs='?',
                        help='Optional gage string for information on a specific basin.')
                        
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
        
    # Compose the SQL command to obtain information from the domain table.
    if args.optGage:
        sqlCmd = "SELECT * from \"Domain_Meta\" where gage_id='%s';" % args.optGage
    else:
        sqlCmd = "SELECT * from \"Domain_Meta\";"
        
    try:
        dbCursor.execute(sqlCmd)
        results = dbCursor.fetchall()
    except:
        print "ERROR: Unable to execute SQL command: " + sqlCmd
        sys.exit(1)
        
    numGages = len(results)
    
    if numGages == 0:
        print "No gages have been entered into the DB tables."
        sys.exit(0)
        
    # Loop through and print out information to the screen.
    for gage in range(0,numGages):
        print "--------------------------------------------------"
        print "DB Basin ID: " + str(results[gage][0])
        print "Gage ID: " + str(results[gage][1])
        print "Link ID in model: " + str(results[gage][2])
        print "Domain Directory: " + str(results[gage][3])
        print "Gage Agency: " + str(results[gage][4])
        print "Geogrid Index East: " + str(results[gage][5])
        print "Geogrid Index West: " + str(results[gage][6])
        print "Geogrid Index South: " + str(results[gage][7])
        print "Geogrid Index North: " + str(results[gage][8])
        print "Fulldom Index East: " + str(results[gage][9])
        print "Fulldom Index West: " + str(results[gage][10])
        print "Fulldom Index South: " + str(results[gage][11])
        print "Fulldom Index North: " + str(results[gage][12])
        print "Geogrid Path: " + str(results[gage][13])
        print "Land Spatial Meta Path: " + str(results[gage][14])
        print "Wrfinput Path: " + str(results[gage][15])
        print "Spatial Soil Path: " + str(results[gage][16])
        print "Fulldom Path: " + str(results[gage][17])
        print "Routelink Path: " + str(results[gage][18])
        print "Spatial Weight Path: " + str(results[gage][19])
        print "Groundwater Parameter file: " + str(results[gage][20])
        print "Groundwater Mask File: " + str(results[gage][21])
        print "Lake Parameter File: " + str(results[gage][22])
        print "Forcing Directory: " + str(results[gage][23])
        print "Observations File: " + str(results[gage][24])
        print "Site Name: " + str(results[gage][25])
        print "Gage Latitude: " + str(results[gage][26])
        print "Gage Longitude: " + str(results[gage][27])
        print "Basin Area (sqMi): " + str(results[gage][28])
        print "Basin Area (sqKm): " + str(results[gage][29])
        print "County: " + str(results[gage][30])
        print "State: " + str(results[gage][31])
        print "HUC2: " + str(results[gage][32])
        print "HUC4: " + str(results[gage][33])
        print "HUC6: " + str(results[gage][34])
        print "HUC8: " + str(results[gage][35])
        print "Ecological Region Level 3: " + str(results[gage][36])
        print "Ecological Region Level 4: " + str(results[gage][37])
        print "River Forecast Center Region: " + str(results[gage][38])
        print "Hydro Modeling Resolution (meters): " + str(results[gage][39])
        print "Aggregation Factor from Land Grid to Hydro Grid: " + str(results[gage][40])
        print "2D Hydro Parameter Table Path: " + str(results[gage][41])
        print "--------------------------------------------------"
        
    # Close connection to DB
    try:
        conn.close()
    except:
        print "ERROR: Unable to close DB connection."
        sys.exit(1)
    
if __name__ == "__main__":
    main(sys.argv[1:])