# Program to adjust NWM parameter files per output from R calibration 
# evaluations and adjustments. The following files will be adjusted:
# 1.) Fulldom.nc
# 2.) HYDRO.TBL
# 3.) soil_properties.nc

# Program is contingent on specific COMPLETE flag begin generated 
# from R. If this file is not created, then this program will exit
# gracefully without creating it's own COMPLETE flag. Without the 
# Python-generated COMPLETE flag, the workflow will generate an error 
# message. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu
# 303-497-2693

import argparse
import sys
from netCDF4 import Dataset
import os
import shutil

def main(argv):
    # Parse arguments. Only input necessary is the run directory.
    parser = argparse.ArgumentParser(description='Main program to adjust input ' + \
             'parameters for the National Water Model')
    parser.add_argument('workDir',metavar='workDir',type=str,nargs='+',
                        help='Directory containing inputs necessary for adjustments.')
    parser.add_argument('runDir',metavar='runDir',type=str,nargs='+',
                        help='Directory containing model output where final parameter' + \
                             ' files will reside')
                        
    args = parser.parse_args()
    workDir = str(args.workDir[0])
    runDir = str(args.runDir[1])
    
    # Compose input file paths.
    fullDomOrig = workDir + "/BASELINE_PARAMETERS/Fulldom.nc"
    hydroOrig = workDir + "/BASELINE_PARAMETERS/HYDRO.TBL"
    soilOrig = workDir + "/BASELINE_PARAMETERS/soil_properties.nc"
    rCompletePath = workDir + "/R_COMPLETE"
    adjTbl = workDir + "/params_new.txt"
    
    # Compose output file paths.
    fullDomOut = runDir + "/Fulldom.nc"
    hydroOut = runDir + "/HYDRO.TBL"
    soilOut = runDir + "/soil_properties.nc"
    outFlag = workDir + "/CALIB_ITER.COMPLETE"
    
    # If R COMPLETE flag not present, this implies the R code didn't run
    # to completion.
    if not os.path.isfile(rCompletePath):
        sys.exit(1)
        
    os.remove(rCompletePath)
    
    try:
        shutil.copy(fullDomOrig,fullDomOut)
        shutil.copy(hydroOrig,hydroOut)
        shutil.copy(soilOrig,soilOut)
    except:
        sys.exit(1)
        
    # Adjust HYDRO.TBL
    
    # Adjust Fulldom.nc
    idAdj = Dataset(fullDomOut,'a')
    
    idAdj.close()
    
    # Adjust soil_properties.nc
    idAdj = Dataset(soilOut,'a')
    
    idAdj.close()
        
    # Touch empty COMPLETE flag file. This will be seen by workflow, demonstrating
    # calibration iteration is complete.
    try:
        open(outFlag,'a').close()
    except:
        sys.exit(1)
        
if __name__ == "__main__":
    main(sys.argv[1:])