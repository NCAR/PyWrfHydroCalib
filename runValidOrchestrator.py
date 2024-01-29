import os
import sys
import argparse

def main():

    parser = argparse.ArgumentParser(description='Main orchestrator to start or restart ' + \
                                                 'calibration for WRF-Hydro')
    parser.add_argument('script_path', metavar='script_path', type=str, nargs='+',
                        help='Path of the PyWrfHydroCalib directory.')
    parser.add_argument('jobID', metavar='jobID', type=str, nargs='+',
                        help='Job ID specific to calibration experiment.')
    parser.add_argument('--optDbPath', type=str, nargs='?',
                        help='Optional alternative path to SQLite DB file.')

    args = parser.parse_args()

    script_path = args.script_path[0]
    jobID = args.jobID[0]
    try:
        opt_path = args.optDbPath
    except:
        opt_path = ""

    for valid_type in ['CTRL', 'BEST']:
        cmd = "python %s/validOrchestrator.py %s %s --optDbPath %s" %(script_path, jobID, valid_type, opt_path)
        print(cmd)
        ret = os.system(cmd)
        if(ret!=0):
            print("Error running %s" %cmd)

if __name__ == "__main__" :
    main()
