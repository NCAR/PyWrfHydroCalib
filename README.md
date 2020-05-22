# PyNWMCalib

## Disclaimer:
This software is preliminary and is subject to revision. The software is provided in Beta status and under the condition that NCAR/UCAR will not be held liable for any damages resulting from the authorized or unauthorized use of the software. Until the official public release, this software is unsupported and “use at your own risk”.

## Introduction
This package provides tools for calibration of WRF-Hydro. All the scripts here are either in Python or R. The user manual has the full description of the underlying concepts and technical details on what is included in the database and how to set a calibration run from start to finish. 

## Content of package
The main workflow management scripts are in python which are placed in the main directory. The backend R and Python scripts which do the parameter adjustment, statistics calculations, updating the parameters for the next version (thought DDS calibration technique), etc are in the core[/core]. util[/util] has few utilities that the important ones are described in the manual. 

# dependencies
R required packages 
* data.table
* ncdf4
* ggplot2
* gridExtra
* plyr
* hydroGOF

Python required packages 
* netCDF4 for Python
* pandas
* numpy
* psycopg2
* psutil


