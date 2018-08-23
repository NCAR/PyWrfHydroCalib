# This is a top-level, self-contained Python program that will enter in basin 
# information the user provides via a CSV file on each basin they wish 
# to calibrate over. It's important to note the user will need to create
# the CSV file with the following rows filled out for the program to 
# execute probably. A sample input CSV has been included with the package
# under /setup_files/sample_domainMeta.csv:
# 1)  'site_no' - Stream-gage identifier - I.E. USGS gage number, etc
# 2)  'link' - COMid from Routelink file associating streamflow point in model output to gage.
# 3)  'hyd_w' - NWM routing domain East-West dimension index for western edge of subsetted domain.
# 4)  'hyd_e' - NWM routing domain East-West dimension index for eastern edge of subsetted domain.
# 5)  'hyd_s' - NWM routing domain South-North dimension index for southern edge of subsetted domain.
# 6)  'hyd_n' - NWM routing domain South-North dimension index for northern edge of subsetted domain.
# 7)  'geo_w' - NWM geoGrid domain East-West dimension index for western edge of subsetted domain.
# 8)  'geo_e' - NWM geoGrid domain East-West dimension index for easteern edge of subsetted domain.
# 9)  'geo_s' - NWM geoGrid domain South-North dimension index for southern edge of subsetted domain.
# 10) 'geo_n' - NWM geoGrid domain South-North dimension index for northern edge of subsetted domain.
# 11) 'dirname' - Directory containing necessary input parameter files specific to subsetted domain
#                 I.E. - geogrid, fulldom, routelink, etc.
# 12) 'agency_cd' - Which govt agency stream gage is from (USGS,CODWR,etc).
# 13) 'site_name' - Character string describing gage site.
# 14) 'lat' - Latitude of the gage site.
# 15) 'lon' - Longitude of the gage site.
# 16) 'area_sqmi' - Area in squared mile of the basin.
# 17) 'area_sqkm' - Area in squared km of the basin.
# 18) 'county_cd' - County name site gage resides in.
# 19) 'state' - State/Province site gage resides in.
# 20) 'HUC2' - Level 2 HUC identifier for this basin.
# 21) 'HUC4' - Level 4 HUC identifier for this basin.
# 22) 'HUC6' - Level 6 HUC identifier for this basin.
# 23) 'HUC8' - Level 8 HUC identifier for this basin.
# 24) 'ecol3' - Level 3 eco region identifier with this basin.
# 25) 'ecol4' - Level 4 eco region identifier with this basin.
# 26) 'rfc' - River Forecast Center basin resides in.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import argparse
import os
import sys
import pandas as pd
from netCDF4 import Dataset
import sqlite3

import warnings
warnings.filterwarnings("ignore")

# Establish top-level paths that are used to find the DB file. 
prPath = os.path.realpath(__file__)
pathSplit = prPath.split('/')
libPath = '/'
for j in range(1,len(pathSplit)-1):
    libPath = libPath + pathSplit[j] + '/'
topDir = libPath

def main(argv):
    # Parse arguments. User must input a job name and directory.
    parser = argparse.ArgumentParser(description='Utility program to enter ' + \
             'domain metadata into wrfHydroCalib_DB')
    parser.add_argument('inCSV',metavar='inCSV',type=str,nargs='+',
                        help='Input CSV file containing information on basins.')
    parser.add_argument('--optDbPath',type=str,nargs='?',
                        help='Optional alternative path to SQLite DB file.')
                        
    args = parser.parse_args()
    
    # If the SQLite file does not exist, throw an error.
    if args.optDbPath is not None:
        if not os.path.isfile(args.optDbPath):
            print "ERROR: " + args.optDbPath + " Does Not Exist."
            sys.exit(1)
        else:
            dbPath = args.optDbPath
    else:
        dbPath = topDir + "wrfHydroCalib.db"
        if not os.path.isfile(dbPath):
            print "ERROR: SQLite3 DB file: " + dbPath + " Does Not Exist."
            sys.exit(1)
    
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
    
    # Create expected dictionary of column types
    dtype_dic= {'site_no':str,'link':int,'hyd_w':int,'hyd_e':int,'hyd_s':int,'hyd_n':int,
                'geo_w':int,'geo_e':int,'geo_s':int,'geo_n':int,'dirname':str,
                'agency_cd':str,'site_name':str,'lat':float,'lon':float,'area_sqmi':float,
                'area_sqkm':float,'county_cd':str,'state':str,'HUC2':str,'HUC4':str,
                'HUC6':str,'HUC8':str,'ecol3':str,'ecol4':str,'rfc':str}
    
    # Read in CSV file containing necessary input metadata.
    try:
        metaCSV = pd.read_csv(str(args.inCSV[0]),dtype = dtype_dic)
    except:
        print "ERROR: Unable to open CSV file: " + str(args.inCSV[0])
        sys.exit(1)
        
    # Check for expected headers in input CSV file.
    colNames = list(metaCSV.columns)
    
    if len(colNames) != 26:
        print "ERROR: Improper CSV file passed to program."
        sys.exit(1)
        
    expectedNames = ['site_no', 'link', 'hyd_w', 'hyd_e', 'hyd_s', \
                     'hyd_n', 'geo_w', 'geo_e', 'geo_s', 'geo_n', 'dirname', \
                     'agency_cd', 'site_name', 'lat', 'lon', 'area_sqmi', \
                     'area_sqkm', 'county_cd', 'state', 'HUC2', 'HUC4', 'HUC6', \
                     'HUC8', 'ecol3', 'ecol4','rfc']
    for i in range(0,len(colNames)):
        nameCheck = colNames[i]
        if nameCheck != expectedNames[i]:
            print "ERROR: Unexpected column name in: " + str(args.inCSV[0])
            sys.exit(1)
        
    # Check to make sure some data has been entered.
    nSites = len(metaCSV.site_no)
    if nSites <= 0:
        print "ERROR: Zero entries detected in input CSV file."
        sys.exit(1)
        
    # Loop through basins and enter information into the DB.
    for siteNum in range(0,nSites):
        # Pull necessary data from CSV file
        siteNo = metaCSV.site_no[siteNum]
        link = metaCSV.link[siteNum]
        hydW = metaCSV.hyd_w[siteNum]
        hydE = metaCSV.hyd_e[siteNum]
        hydS = metaCSV.hyd_s[siteNum]
        hydN = metaCSV.hyd_n[siteNum]
        geoW = metaCSV.geo_w[siteNum]
        geoE = metaCSV.geo_e[siteNum]
        geoS = metaCSV.geo_s[siteNum]
        geoN = metaCSV.geo_n[siteNum]
        dirBasin = metaCSV.dirname[siteNum]
        agency = metaCSV.agency_cd[siteNum]
        sName = metaCSV.site_name[siteNum]
        lat = metaCSV.lat[siteNum]
        lon = metaCSV.lon[siteNum]
        sqMi = metaCSV.area_sqmi[siteNum]
        sqKm = metaCSV.area_sqkm[siteNum]
        county = metaCSV.county_cd[siteNum]
        state = metaCSV.state[siteNum]
        huc2 = metaCSV.HUC2[siteNum]
        huc4 = metaCSV.HUC4[siteNum]
        huc6 = metaCSV.HUC6[siteNum]
        huc8 = metaCSV.HUC8[siteNum]
        eco3 = metaCSV.ecol3[siteNum]
        eco4 = metaCSV.ecol4[siteNum]
        rfc = metaCSV.rfc[siteNum]
        
        if not os.path.isdir(dirBasin):
            print "ERROR: Directory: " + dirBasin + " not found."
            sys.exit(1)
            
        # Compose paths to input files and check for existence of files.
        geoPath = dirBasin + "/geo_em.nc"
        landSpatialMetaPath = dirBasin + "/GEOGRID_LDASOUT_Spatial_Metadata.nc"
        fullDomPath = dirBasin + "/Fulldom.nc"
        gwPath = dirBasin + "/GWBUCKPARM.nc"
        gwMskPath = dirBasin + "/GWBASINS.nc"
        lakePath1 = dirBasin + "/LAKEPARM.nc"
        lakePath2 = dirBasin + "/LAKEPARM.TBL"
        routePath = dirBasin + "/RouteLink.nc"
        soilPath = dirBasin + "/soil_properties.nc"
        hydro2d = dirBasin + "/HYDRO_TBL_2D.nc"
        wghtPath = dirBasin + "/spatialweights.nc"
        wrfInPath = dirBasin + "/wrfinput.nc"
        forceDir = dirBasin + "/FORCING"
        obsFile = dirBasin + "/OBS/obsStrData.Rdata"
        
        # Double check to make sure input files exist
        if not os.path.isfile(geoPath):
            print "ERROR: " + geoPath + " not found."
            sys.exit(1)
        if not os.path.isfile(landSpatialMetaPath):
            print "WARNING: " + landSpatialMetaPath + " not found. Output will not be CF-Compliant."
            landSpatialMetaPath = "-9999"
        if not os.path.isfile(fullDomPath):
            print "ERROR: " + fullDomPath + " not found."
            sys.exit(1)
        if not os.path.isfile(gwPath):
            print "ERROR: " + gwPath + " not found."
            sys.exit(1)
        if not os.path.isfile(lakePath1) and not os.path.isfile(lakePath2):
            print "WARNING: No lake parameter files found. Assuming you have setup a domain with no lakes."
            lakePath = '-9999'
        if not os.path.isfile(routePath):
            print "WARNING: " + routePath + " not found. Assuming this is for gridded routing....."
            routePath = "-9999"
        if not os.path.isfile(soilPath):
            print "ERROR: " + soilPath + " not found."
            sys.exit(1)
        if not os.path.isfile(hydro2d):
            print "ERROR: " + hydro2d + " not found."
            sys.exit(1)
        if not os.path.isfile(wghtPath):
            print "WARNING: " + wghtPath + " not found. Assuming you are running a non NWM routing...."
            wghtPath = "-9999"
        if not os.path.isfile(wrfInPath):
            print "ERROR: " + wrfInPath + " not found."
            sys.exit(1)
        if not os.path.isdir(forceDir):
            print "ERROR: " + forceDir + " not found."
            sys.exit(1)
        if not os.path.isfile(obsFile):
            print "ERROR: " + obsFile + " not found."
            sys.exit(1)
        if not os.path.isfile(gwMskPath):
            print "WARNING: " + gwMskPath + " not found. Assuming you are running NWM routing...."
            gwMskPath = "-9999"
            
        # Look for a NetCDF lake parameter file first, and use it. If not, use the ASCII table instead.
        if os.path.isfile(lakePath1):
            lakePath = lakePath1
        else:
            if os.path.isfile(lakePath2):
                lakePath = lakePath2
            
        # Calculate grid spacing and aggregation factors from geogrid and Fulldom files...
        dxrt,aggFactor = calcSpacing(geoPath,fullDomPath)
        
        # Compose Postgres command
        cmd = "INSERT INTO \"Domain_Meta\" (gage_id,link_id,domain_path,gage_agency,geo_e," + \
              "geo_w,geo_s,geo_n,hyd_e,hyd_w,hyd_s,hyd_n,geo_file,land_spatial_meta_file,wrfinput_file," + \
              "soil_file,fulldom_file,rtlink_file,spweight_file," + \
              "gw_file,gw_mask,lake_file,forcing_dir,obs_file,site_name,lat,lon," + \
              "area_sqmi,area_sqkm,county_cd,state,huc2,huc4,huc6,huc8,ecol3,ecol4,rfc,dx_hydro,agg_factor,hydro_tbl_spatial) VALUES " + \
              "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (siteNo,\
              link,dirBasin,agency,geoE,geoW,geoS,geoN,hydE,hydW,\
              hydS,hydN,geoPath,landSpatialMetaPath,wrfInPath,soilPath,fullDomPath,routePath,wghtPath,gwPath,\
              gwMskPath,lakePath,forceDir,obsFile,sName,lat,lon,sqMi,sqKm,\
              county,state,huc2,huc4,huc6,huc8,eco3,eco4,rfc,dxrt,aggFactor,hydro2d)
              
        # Make entry into DB
        try:
            #conn.execute(cmd)
            dbCursor.execute(cmd)
        except:
            print "ERROR: Unable to execute postgres command: " + cmd
            sys.exit(1)
            
        try:
            #db.commit()
            conn.commit()
        except:
            print "ERROR: Unable to commit postgres command: " + cmd
            sys.exit(1)
            
    # Close connection to DB
    try:
        conn.close()
    except:
        print "ERROR: Unable to close DB connection."
        sys.exit(1)
        
def calcSpacing(geoPath,fullDomPath):
    """ Generic function to calculate the high resolution grid spacing and 
        aggregation factor between the land and hydro grid. 
    """
    
    # Open the geogrid file and pull the DX attribute (meters) from the file. 
    try:
        idGeo = Dataset(geoPath,'r')
        dxLand = float(idGeo.DX)
        nRowLand = float(idGeo.variables['XLAT_M'].shape[1])
    except:
        print "ERROR: Unable to open: " + geoPath + " and extract DX global attribute along with number of rows."
        sys.exit(1)
        
    try:
        idGeo.close()
    except:
        print "ERROR: Unable to close: " + geoPath
        sys.exit(1)
        
    # Open the Fulldom file and extract the resolution from teh "x" coordinate variable.
    try:
        idFullDom = Dataset(fullDomPath,'r')
    except:
        print "ERROR: Unable to open: " + fullDomPath
        sys.exit(1)
    
    try:
        nRowHydro = float(idFullDom.variables['y'].shape[0])
    except:
        print "ERROR: Unable to extract Fulldom number of rows from y coordinate variable."
        sys.exit(1)
        
    try:
        idFullDom.close()
    except:
        print "ERROR: Unable to close: " + fullDomPath
    
    aggFactor = int(nRowHydro/nRowLand)
    dxHydro = dxLand/aggFactor
    
    return dxHydro,aggFactor
        
if __name__ == "__main__":
    main(sys.argv[1:])