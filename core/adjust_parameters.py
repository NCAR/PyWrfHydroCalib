# Program to adjust wrfHydro parameter files per output from R calibration 
# evaluations and adjustments. The following files will be adjusted:
# 1.) Fulldom.nc
# 2.) HYDRO_TBL_2D.nc
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
import pandas as pd
import time
import subprocess
import numpy as np
import xarray as xr  


def main(argv):
    # Parse arguments. Only input necessary is the run directory.
    parser = argparse.ArgumentParser(description='Main program to adjust input ' + \
             'parameters for the National Water Model')
    parser.add_argument('workDir',metavar='workDir',type=str,nargs='+',
                        help='Directory containing inputs necessary for adjustments.')
    parser.add_argument('runDir',metavar='runDir',type=str,nargs='+',
                        help='Directory containing model output where final parameter' + \
                             ' files will reside')
    parser.add_argument('gwFlag',metavar='gwFlag',type=int,nargs='+',
                        help='Flag to indicate if groundwater bucket model is being used.')
    parser.add_argument('chRtFlag',metavar='chRtFlag',type=int,nargs='+',
                        help='Flag to indicate the type of channel routing.') 
    parser.add_argument('enableMask',metavar='enableMask',type=int,nargs='+',
                        help='Flag to indicate the use of mask.')
                        
    args = parser.parse_args()
    workDir = str(args.workDir[0])
    runDir = str(args.runDir[0])
    
    # Compose input file paths.
    fullDomOrig = workDir + "/BASELINE_PARAMETERS/Fulldom.nc"
    hydroOrig = workDir + "/BASELINE_PARAMETERS/HYDRO_TBL_2D.nc"
    soilOrig = workDir + "/BASELINE_PARAMETERS/soil_properties.nc"
    gwOrig = workDir + "/BASELINE_PARAMETERS/GWBUCKPARM.nc"
    chanParmOrig = workDir + "/BASELINE_PARAMETERS/CHANPARM.TBL"
    rCompletePath = workDir + "/R_COMPLETE"
    adjTbl = workDir + "/params_new.txt"
    
    # Compose output file paths.
    fullDomOut = runDir + "/Fulldom.nc"
    hydroOut = runDir + "/HYDRO_TBL_2D.nc"
    soilOut = runDir + "/soil_properties.nc"
    gwOut = runDir + '/GWBUCKPARM.nc'
    chanParmOut = runDir + "/CHANPARM.TBL"
    outFlag = workDir + "/CALIB_ITER.COMPLETE"
    
    # If R COMPLETE flag not present, this implies the R code didn't run
    # to completion.
    if not os.path.isfile(rCompletePath):
        sys.exit(1)
        
    # Sleep for a few seconds in case R is still touching R_COMPLETE, or
    # from lingering parallel processes.
    time.sleep(10)
    
    os.remove(rCompletePath)
    
    # If the params_new file is not present, but the R Complete path was,
    # we are going to assume this was the last iteration and no parameters
    # need to be produced.
    if not os.path.isfile(adjTbl):
        # Touch empty COMPLETE flag file. This will be seen by workflow, demonstrating
        # calibration iteration is complete.
        try:
            open(outFlag,'a').close()
            sys.exit(0)
        except:
            sys.exit(2)
    
    try:
        shutil.copy(fullDomOrig,fullDomOut)
        shutil.copy(hydroOrig,hydroOut)
        shutil.copy(soilOrig,soilOut)
        if args.gwFlag[0] == 1 or args.gwFlag[0] == 4:
            shutil.copy(gwOrig,gwOut)
        if args.chRtFlag[0] == 3:
            # Gridded routing
            shutil.copy(chanParmOrig,chanParmOut)
    except:
        sys.exit(3)
        
    # Read in new parameters table.
    newParams = pd.read_csv(adjTbl,sep=' ')
    paramNames = list(newParams.columns.values)
    
    if args.chRtFlag[0] == 3:
        # Open the original CHANPARM.TBL
        chanParmTblDataOrig = open(chanParmOrig,'w')
        
        # Open the new CHANPARM.TBL for writing
        chanParmOutObj = open(chanParmOut,'w')
        countTmp = 1
        for line in chanParmTblDataOrig:
            if countTmp < 4:
                chanParmOutObj.write(line)
            else:
                lineTmp = line
                lineSplit = lineTmp.split(',')
                if "Bw" in paramNames:
                    bwValue = float(lineSplit[1])*float(newParams.Bw[0])
                else:
                    bwValue = float(lineSplit[1])
                if "HLINK" in paramNames:
                    hlinkValue = float(lineSplit[2])*float(newParams.HLINK[0])
                else:
                    hlinkValue = float(lineSplit[2])
                if "ChSSlp" in paramNames:
                    chsslpValue = float(lineSplit[3])*float(newParams.ChSSlp[0])
                else:
                    chsslpValue = float(lineSplit[3])
                if "MannN" in paramNames:
                    mannValue = float(lineSplit[4])*float(newParams.MannN[0])
                else:
                    mannValue = float(lineSplit[4])
                outStr = lineSplit[0] + ", " + str(bwValue) + ", " + str(hlinkValue) + \
                         ", " + str(chsslpValue) + ", " + str(mannValue) + "\n"
                chanParmOutObj.write(outStr)
            countTmp = countTmp + 1
        chanParmOutObj.close()
        
    # Open NetCDF parameter files for adjustment.
    idFullDom = Dataset(fullDomOut,'a')
    idSoil2D = Dataset(soilOut,'a')
    if args.gwFlag[0] == 1 or args.gwFlag[0] == 4:
        idGw = Dataset(gwOut,'a')
    idHydroTbl = Dataset(hydroOut,'a')
    
    # if we are going to use the mask, read in the file
    if args.enableMask[0] == 1:
       maskFile = workDir + "/mask.coarse.tif"
       mask = xr.open_rasterio(maskFile)
       maskFile = workDir + "/mask.fine.tif"
       maskFine= xr.open_rasterio(maskFile) 
       maskFile = workDir + "/mask.GWBUCKET.csv"
       maskGW = pd.read_csv(maskFile,dtype={0: int})

    for param in paramNames:
        if args.enableMask[0] == 0:
            if param == "bexp":
                idSoil2D.variables['bexp'][:,:,:,:] = idSoil2D.variables['bexp'][:,:,:,:]*float(newParams.bexp[0])

            if param == "smcmax":
                idSoil2D.variables['smcmax'][:,:,:,:] = idSoil2D.variables['smcmax'][:,:,:,:]*float(newParams.smcmax[0])

            if param == "slope":
                idSoil2D.variables['slope'][:,:,:] = float(newParams.slope[0])

            if param == "lksatfac":
                idFullDom.variables['LKSATFAC'][:,:] = float(newParams.lksatfac[0])

            if param == "cwpvt":
                idSoil2D.variables['cwpvt'][:,:,:] = idSoil2D.variables['cwpvt'][:,:,:]*float(newParams.cwpvt[0])

            if param == "vcmx25":
                idSoil2D.variables['vcmx25'][:,:,:] = idSoil2D.variables['vcmx25'][:,:,:]*float(newParams.vcmx25[0])

            if param == "mp":
                idSoil2D.variables['mp'][:,:,:] = idSoil2D.variables['mp'][:,:,:]*float(newParams.mp[0])

            if param == "hvt":
                idSoil2D.variables['hvt'][:,:,:] = idSoil2D.variables['hvt'][:,:,:]*float(newParams.hvt[0])

            if param == "mfsno":
                idSoil2D.variables['mfsno'][:,:,:] = idSoil2D.variables['mfsno'][:,:,:]*float(newParams.mfsno[0])

            if param == "refkdt":
                idSoil2D.variables['refkdt'][:,:,:] = float(newParams.refkdt[0])

            if param == "dksat":
                idSoil2D.variables['dksat'][:,:,:,:] = idSoil2D.variables['dksat'][:,:,:,:]*float(newParams.dksat[0])

            if param == "retdeprtfac":
                idFullDom.variables['RETDEPRTFAC'][:,:] = float(newParams.retdeprtfac[0])

            if param == "ovroughrtfac":
                idFullDom.variables['OVROUGHRTFAC'][:,:] = float(newParams.ovroughrtfac[0])

            if param == "dksat":
                idHydroTbl.variables['LKSAT'][:,:] = idHydroTbl.variables['LKSAT'][:,:]*float(newParams.dksat[0])

            if param == "smcmax":
                idHydroTbl.variables['SMCMAX1'][:,:] = idHydroTbl.variables['SMCMAX1'][:,:]*float(newParams.smcmax[0])

            if param == "rsurfexp":
                idSoil2D.variables['rsurfexp'][:,:,:] = float(newParams.rsurfexp[0])

            if param == "z0sno":
                idSoil2D.variables['z0sno'][:,:,:] = float(newParams.z0sno[0])

            if param == "ssi":
                idSoil2D.variables['ssi'][:,:,:] = float(newParams.ssi[0])

            if param == "snowretfac":
                idSoil2D.variables['snowretfac'][:,:,:] = float(newParams.snowretfac[0])

            if param == "swemx":
                idSoil2D.variables['swemx'][:,:,:] = float(newParams.swemx[0])

            if param == "tau0":
                idSoil2D.variables['tau0'][:,:,:] = float(newParams.tau0[0])

            if param == "graingrowth":
                idSoil2D.variables['graingrowth'][:,:,:] = float(newParams.graingrowth[0])

            if param == "extragrowth":
                idSoil2D.variables['extragrowth'][:,:,:] = float(newParams.extragrowth[0])

            if param == "dirtsoot":
                idSoil2D.variables['dirtsoot'][:,:,:] = float(newParams.dirtsoot[0])

            if param == "bats_cosz":
                idSoil2D.variables['bats_cosz'][:,:,:] = float(newParams.bats_cosz[0])

            if param == "bats_visnew":
                idSoil2D.variables['bats_visnew'][:,:,:] = float(newParams.bats_visnew[0])

            if param == "bats_nirnew":
                idSoil2D.variables['bats_nirnew'][:,:,:] = float(newParams.bats_nirnew[0])

            if param == "bats_visage":
                idSoil2D.variables['bats_visage'][:,:,:] = float(newParams.bats_visage[0])

            if param == "bats_nirage":
                idSoil2D.variables['bats_nirage'][:,:,:] = float(newParams.bats_nirage[0])

            if param == "bats_visdir":
                idSoil2D.variables['bats_visdir'][:,:,:] = float(newParams.bats_visdir[0])

            if param == "bats_nirdir":
                idSoil2D.variables['bats_nirdir'][:,:,:] = float(newParams.bats_nirdir[0])

            if param == "rsurfsnow":
                idSoil2D.variables['rsurfsnow'][:,:,:] = float(newParams.rsurfsnow[0])

            if param == "refsnowdens":
                idSoil2D.variables['refsnowdens'][:,:,:] = float(newParams.refsnowdens[0])

            if param == "frac_direct":
                idSoil2D.variables['frac_direct'][:,:,:] = float(newParams.frac_direct[0])

            if param == "frac_visible":
                idSoil2D.variables['frac_visible'][:,:,:] = float(newParams.frac_visible[0])

            if param == "scamax":
                idSoil2D.variables['scamax'][:,:,:] = float(newParams.scamax[0])

            if param == "unload_temp":
                idSoil2D.variables['unload_temp'][:,:,:] = float(newParams.unload_temp[0])

            if param == "unload_wind":
                idSoil2D.variables['unload_wind'][:,:,:] = float(newParams.unload_wind[0])

            if param == "maxsno_sp":
                idSoil2D.variables['maxsno_sp'][:,:,:] = float(newParams.maxsno_sp[0])

            if args.gwFlag[0] == 1 or args.gwFlag[0] == 4:
                if param == "zmax":
                    idGw.variables['Zmax'][:] = float(newParams.zmax[0])

                if param == "expon":
                    idGw.variables['Expon'][:] = float(newParams.expon[0])

                if param == "Loss":
                    idGw.variables['Loss'][:] = float(newParams.Loss[0])

                if param == 'Coeff':
                    idGw.variables['Coeff'][:] = float(newParams.Coeff[0])


        if args.enableMask[0] == 1:
            if param == "bexp":
                idSoil2D.variables['bexp'][:,:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['bexp'][:,:,:,:],idSoil2D.variables['bexp'][:,:,:,:]*float(newParams.bexp[0]))

            if param == "smcmax":
                idSoil2D.variables['smcmax'][:,:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['smcmax'][:,:,:,:],idSoil2D.variables['smcmax'][:,:,:,:]*float(newParams.smcmax[0]))

            if param == "slope":
                idSoil2D.variables['slope'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['slope'][:,:,:],idSoil2D.variables['slope'][:,:,:]*0+float(newParams.slope[0]))

            if param == "lksatfac":
                idFullDom.variables['LKSATFAC'][:,:] = np.where(maskFine[0]==1,idFullDom.variables['LKSATFAC'][:,:],idFullDom.variables['LKSATFAC'][:,:]*0+float(newParams.lksatfac[0]))

            if param == "cwpvt":
                idSoil2D.variables['cwpvt'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['cwpvt'][:,:,:],idSoil2D.variables['cwpvt'][:,:,:]*float(newParams.cwpvt[0]))

            if param == "vcmx25":
                idSoil2D.variables['vcmx25'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['vcmx25'][:,:,:],idSoil2D.variables['vcmx25'][:,:,:]*float(newParams.vcmx25[0]))

            if param == "mp":
                idSoil2D.variables['mp'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['mp'][:,:,:],idSoil2D.variables['mp'][:,:,:]*float(newParams.mp[0]))

            if param == "hvt":
                idSoil2D.variables['hvt'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['hvt'][:,:,:],idSoil2D.variables['hvt'][:,:,:]*float(newParams.hvt[0]))

            if param == "mfsno":
                idSoil2D.variables['mfsno'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['mfsno'][:,:,:],idSoil2D.variables['mfsno'][:,:,:]*float(newParams.mfsno[0]))

            if param == "refkdt":
                idSoil2D.variables['refkdt'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['refkdt'][:,:,:],idSoil2D.variables['refkdt'][:,:,:]*0+float(newParams.refkdt[0]))

            if param == "dksat":
                idSoil2D.variables['dksat'][:,:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['dksat'][:,:,:,:],idSoil2D.variables['dksat'][:,:,:,:]*float(newParams.dksat[0]))

            if param == "retdeprtfac":
                idFullDom.variables['RETDEPRTFAC'][:,:] = np.where(maskFine[0]==1,idFullDom.variables['RETDEPRTFAC'][:,:],idFullDom.variables['RETDEPRTFAC'][:,:]*0+float(newParams.retdeprtfac[0]))

            if param == "ovroughrtfac":
                idFullDom.variables['OVROUGHRTFAC'][:,:] = np.where(maskFine[0]==1,idFullDom.variables['OVROUGHRTFAC'][:,:],idFullDom.variables['OVROUGHRTFAC'][:,:]*0+float(newParams.ovroughrtfac[0]))

            if param == "dksat":
                idHydroTbl.variables['LKSAT'][:,:] = np.where(np.flipud(mask[0])==1,idHydroTbl.variables['LKSAT'][:,:],idHydroTbl.variables['LKSAT'][:,:]*float(newParams.dksat[0]))

            if param == "smcmax":
                idHydroTbl.variables['SMCMAX1'][:,:] = np.where(np.flipud(mask[0])==1,idHydroTbl.variables['SMCMAX1'][:,:],idHydroTbl.variables['SMCMAX1'][:,:]*float(newParams.smcmax[0]))

            if param == "rsurfexp":
                idSoil2D.variables['rsurfexp'][:,:,:] = np.where(np.flipud(mask[0])==1,idSoil2D.variables['rsurfexp'][:,:,:],idSoil2D.variables['rsurfexp'][:,:,:]*0+float(newParams.rsurfexp[0]))

            if param == "z0sno":
                idSoil2D.variables['z0sno'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['z0sno'][:,:,:],idSoil2D.variables['z0sno'][:,:,:]*0+float(newParams.z0sno[0]))

            if param == "ssi":
                idSoil2D.variables['ssi'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['ssi'][:,:,:],idSoil2D.variables['ssi'][:,:,:]*0+float(newParams.ssi[0]))

            if param == "snowretfac":
                idSoil2D.variables['snowretfac'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['snowretfac'][:,:,:],idSoil2D.variables['snowretfac'][:,:,:]*0+float(newParams.snowretfac[0]))

            if param == "swemx":
                idSoil2D.variables['swemx'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['swemx'][:,:,:],idSoil2D.variables['swemx'][:,:,:]*0+float(newParams.swemx[0]))

            if param == "tau0":
                idSoil2D.variables['tau0'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['tau0'][:,:,:],idSoil2D.variables['tau0'][:,:,:]*0+float(newParams.tau0[0]))

            if param == "graingrowth":
                idSoil2D.variables['graingrowth'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['graingrowth'][:,:,:],idSoil2D.variables['graingrowth'][:,:,:]*0+float(newParams.graingrowth[0]))

            if param == "extragrowth":
                idSoil2D.variables['extragrowth'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['extragrowth'][:,:,:],idSoil2D.variables['extragrowth'][:,:,:]*0+float(newParams.extragrowth[0]))

            if param == "dirtsoot":
                idSoil2D.variables['dirtsoot'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['dirtsoot'][:,:,:],idSoil2D.variables['dirtsoot'][:,:,:]*0+float(newParams.dirtsoot[0]))

            if param == "bats_cosz":
                idSoil2D.variables['bats_cosz'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['bats_cosz'][:,:,:],idSoil2D.variables['bats_cosz'][:,:,:]*0+float(newParams.bats_cosz[0]))

            if param == "bats_visnew":
                idSoil2D.variables['bats_visnew'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['bats_visnew'][:,:,:],idSoil2D.variables['bats_visnew'][:,:,:]*0+float(newParams.bats_visnew[0]))

            if param == "bats_nirnew":
                idSoil2D.variables['bats_nirnew'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['bats_nirnew'][:,:,:],idSoil2D.variables['bats_nirnew'][:,:,:]*0+float(newParams.bats_nirnew[0]))

            if param == "bats_visage":
                idSoil2D.variables['bats_visage'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['bats_visage'][:,:,:],idSoil2D.variables['bats_visage'][:,:,:]*0+float(newParams.bats_visage[0]))

            if param == "bats_nirage":
                idSoil2D.variables['bats_nirage'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['bats_nirage'][:,:,:],idSoil2D.variables['bats_nirage'][:,:,:]*0+float(newParams.bats_nirage[0]))

            if param == "bats_visdir":
                idSoil2D.variables['bats_visdir'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['bats_visdir'][:,:,:],idSoil2D.variables['bats_visdir'][:,:,:]*0+float(newParams.bats_visdir[0]))

            if param == "bats_nirdir":
                idSoil2D.variables['bats_nirdir'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['bats_nirdir'][:,:,:],idSoil2D.variables['bats_nirdir'][:,:,:]*0+float(newParams.bats_nirdir[0]))

            if param == "rsurfsnow":
                idSoil2D.variables['rsurfsnow'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['rsurfsnow'][:,:,:],idSoil2D.variables['rsurfsnow'][:,:,:]*0+float(newParams.rsurfsnow[0]))

            if param == "refsnowdens":
                idSoil2D.variables['refsnowdens'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['refsnowdens'][:,:,:],idSoil2D.variables['refsnowdens'][:,:,:]*0+float(newParams.refsnowdens[0]))

            if param == "frac_direct":
                idSoil2D.variables['frac_direct'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['frac_direct'][:,:,:],idSoil2D.variables['frac_direct'][:,:,:]*0+float(newParams.frac_direct[0]))

            if param == "frac_visible":
                idSoil2D.variables['frac_visible'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['frac_visible'][:,:,:],idSoil2D.variables['frac_visible'][:,:,:]*0+float(newParams.frac_visible[0]))

            if param == "scamax":
                idSoil2D.variables['scamax'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['scamax'][:,:,:],idSoil2D.variables['scamax'][:,:,:]*0+float(newParams.scamax[0]))

            if param == "unload_temp":
                idSoil2D.variables['unload_temp'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['unload_temp'][:,:,:],idSoil2D.variables['unload_temp'][:,:,:]*0+float(newParams.unload_temp[0]))

            if param == "unload_wind":
                idSoil2D.variables['unload_wind'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['unload_wind'][:,:,:],idSoil2D.variables['unload_wind'][:,:,:]*0+float(newParams.unload_wind[0]))

            if param == "maxsno_sp":
                idSoil2D.variables['maxsno_sp'][:,:,:] =  np.where(np.flipud(mask[0])==1,idSoil2D.variables['maxsno_sp'][:,:,:],idSoil2D.variables['maxsno_sp'][:,:,:]*0+float(newParams.maxsno_sp[0]))
		
            if args.gwFlag[0] == 1 or args.gwFlag[0] == 4:   
                if param == "zmax":
                    idGw.variables['Zmax'][np.isin(idGw.variables['ComID'][:], maskGW, invert = True)] = float(newParams.zmax[0])

                if param == "expon":
                    idGw.variables['Expon'][np.isin(idGw.variables['ComID'][:], maskGW, invert = True)] = float(newParams.expon[0])

                if param == "Loss":
                    idGw.variables['Loss'][np.isin(idGw.variables['ComID'][:], maskGW, invert = True)] = float(newParams.Loss[0])

                if param == 'Coeff':
                    idGw.variables['Coeff'][np.isin(idGw.variables['ComID'][:], maskGW, invert = True)] = float(newParams.Coeff[0])
           
    # Close NetCDF files
    idFullDom.close()
    idSoil2D.close()
    if args.gwFlag[0] == 1 or args.gwFlag[0] == 4:
        idGw.close()
    idHydroTbl.close()
    
    # Remove restart files. All other files will be overwritten by the next
    # model iteration. 
    cmd = 'rm -rf ' + runDir + '/*.err'
    try:
        subprocess.call(cmd,shell=True)
    except:
        sys.exit(1)
    cmd = 'rm -rf ' + runDir + '/*.out'
    try:
        subprocess.call(cmd,shell=True)
    except:
        sys.exit(1)
    cmd = 'rm -rf ' + runDir + '/HYDRO_RST.*'
    try:
        subprocess.call(cmd,shell=True)
    except:
        sys.exit(4)
    cmd = 'rm -rf ' + runDir + '/RESTART.*'
    try:
        subprocess.call(cmd,shell=True)
    except:
        sys.exit(5)

    # Touch empty COMPLETE flag file. This will be seen by workflow, demonstrating
    # calibration iteration is complete.
    try:
        open(outFlag,'a').close()
    except:
        sys.exit(6)
            
if __name__ == "__main__":
    main(sys.argv[1:])
