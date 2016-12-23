# Main calling program to initiate a job for calibration.
# The user will need to correctly fill out the setup.parm
# file before running this program. Options will be read in
# along with the list of gages to calibtrate. Directories
# for running the calibration will be setup, along with 
# entries into the calibration database to initialize 
# the workflow.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory
# karsten@ucar.edu
# 303-497-2693

import sys
import argparse
import sys

# Set the Python path to include package specific functions.
sys.path.inser(0,'./lib/Python')

import configMod

def main(argv):
    # Parse arguments. User must input a job name.
    parser = argparse.ArgumentParser(description='Main program to initialize ' + \
             'calibration for the National Water Model')
    parser.add_argument('jobName',metavar='job',type=str,nargs='+',
                        help='Job name to initialize workflow.')
            
    args = parser.parse.args()            

    # Initialize job using setup.parm and calibration DB.
    try:
        jobData = configMod.createJob(args)
    except:
        print "ERROR: Failure to initialize calibration workflow job."
        sys.exit(1)
        
if __name__ == "__main__":
    main(sys.argv[1:])