# Module file for containing functions for executing WRF-Hydro model runs.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

#import datetime
import os
import calibIoMod
import namelistMod
import statusMod
import errMod
import subprocess

import warnings
warnings.filterwarnings("ignore")

def runModel(statusData,staticData,db,gageID,gage,keySlot,basinNum,iteration):
    """
    Function for running the sensitivity analysis for a given basin. 
    This function will loop through all model iterations specified for
    sensitivity. This code will only iterate over blocks of model runs at 
    a time, as specified in the setup.parm file. This is to prevent too many
    model simulations being ran all at once. All model iterations must be 
    complete before the sensitivity analysis can be ran. Additionally, 
    an initial program to generate the necessary input parameter files
    will be ran to generate parmaeter files for all model iterations. If
    any model iterations fail, the analysis workflow will lock up and report
    an error back to the user. 
    """
    # First check to make sure previous iteration's status is 1.0 (unless iteration 0).
    # This is to prevent the program from doing unecessary work. 
    if iteration > 0:
        if keySlot[basinNum,iteration-1] < 1.0:
            return
    
    