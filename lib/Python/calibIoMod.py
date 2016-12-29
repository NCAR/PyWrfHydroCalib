# Generate IO library for handling high level IO.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import pandas as pd

def getGageList(jobData):
    # Function for extracting list of gages 
    # based on either the CSV file, or an SQL
    # command to extract gages based on a user 
    # specified command.

    if len(jobData.gList) > 0:
        # User provided CSV file with list of gages.
        gListTmp = pd.read_csv(jobData.gList)
        
        # PLACEHOLDER FOR CHECKING DB TO ENSURE
        # ALL GAGE NAMES EXIST IN META TABLE
        jobData.gages = gListTmp.Gage[:]        