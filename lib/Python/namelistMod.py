# General function library for creating namelist.hrldas and hydro.namelist 
# files using information the user provided, and basin-specific information.

# Logan Karsten
# National Center for Atmospheric Research 
# Research Applications Laboratory

import os

import warnings
warnings.filterwarnings("ignore")

def createHrldasNL(statusData,gageData,jobData,outDir,typeFlag,bDate,eDate,genFlag):
    # General function for creation of a namelist.hrldas file.
    
    # NOTE: typeFlag = 1 indicates cold start.
    #       typeFlag = 2 indicates restart.
    #       typeFlag = 3 indicates this is the initialization of a new model
    #                    simulation for either a calibration iteration, or 
    #                    sensitivity simulation, or a validation simulation. 
    # NOTE: genFlag = 0 indicates a spinup - pull all parameter files from 
    #                   gageData
    #       genFlag = 1 indicartes a calibration - pull HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from the run directory.
    #       genFlag = 2 Indicates validation CTRL - pull HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from calibration output.
    #       genFlag = 3 Indicates validation BEST - pull HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from calibration output.
    #       genFlag = 4 Indicates sensitivy analysis - HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from run directory
    # Create path for the namelist file
    pathOut = outDir + "/namelist.hrldas"
    if os.path.isfile(pathOut):
        os.remove(pathOut)
    
    # If this is a calibration simulation, we need to check to see if we are 
    # past the date of minimal outputs. If the user specified to turn this optional
    # feature off, then by default, all simulations will produce full outputs
    # for the entire calibration iteration simulation.
    if jobData.optCalStripFlag == 1:
        minOutFlag = 1
        if bDate >= jobData.bCalibFullOutputs:
            minOutFlag = 0
        else:
            minOutFlag = 1
    else:
        minOutFlag = 0
        
    # Write each line of the expected hrldas.namelist file.
    try:
        fileObj = open(pathOut,'w')
        fileObj.write('&NOAHLSM_OFFLINE\n')
        fileObj.write('\n')
        inStr = ' HRLDAS_SETUP_FILE = "' + str(gageData.wrfInput) + '"' + '\n'
        fileObj.write(inStr)
        inStr = ' INDIR = "' + str(gageData.forceDir) + '"' + '\n'
        fileObj.write(inStr)
        if genFlag == 0:
            inStr = ' SPATIAL_FILENAME = "' + str(gageData.soilFile) + '"' + '\n'
        if genFlag == 1:
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.CALIB/OUTPUT/soil_properties.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' SPATIAL_FILENAME = "' + pthTmp + '"' + '\n'
        if genFlag == 2:
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.VALID/OUTPUT/CTRL/soil_properties.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' SPATIAL_FILENAME = "' + pthTmp + '"' + '\n'
        if genFlag == 3:
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.VALID/OUTPUT/BEST/soil_properties.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' SPATIAL_FILENAME = "' + pthTmp + '"' + '\n'
        if genFlag == 4:
            pthTmp = outDir + "/soil_properties.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' SPATIAL_FILENAME = "' + pthTmp + '"' + '\n'
        fileObj.write(inStr)
        inStr = ' OUTDIR = "' + outDir + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        if minOutFlag == 1:
            # Only run simulation to the end of the mimimal output time period. 
            # The workflow will then restart the model with full output after 
            # it has seen it's passed this point.
            dt = jobData.bCalibFullOutputs - bDate
        else:
            dt = eDate - bDate
        inStr = ' START_YEAR = ' + bDate.strftime('%Y') + '\n'
        fileObj.write(inStr)
        inStr = ' START_MONTH = ' + bDate.strftime('%m') + '\n'
        fileObj.write(inStr)
        inStr = ' START_DAY = ' + bDate.strftime('%d') + '\n'
        fileObj.write(inStr)
        fileObj.write(' START_HOUR = 00\n')
        fileObj.write(' START_MIN = 00\n')
        fileObj.write('\n')
        if typeFlag == 2:
            # We are restarting a model simulation that either failed, or was killed. 
            rstFile = outDir + "/RESTART." + bDate.strftime('%Y%m%d') + "00_DOMAIN1"
            if not os.path.isfile(rstFile):
                statusData.errMsg = "ERROR: Failure to find: " + rstFile
                raise Exception()
            inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + rstFile + "'" + '\n'
        if typeFlag == 1:
            # We are cold-starting this simulation. This will most likely be for
            # spinup purposes.
            inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + "'" + '\n' 
        if typeFlag == 3:
            # This is the beginning of a new sensitivity/calibration/validation
            # simulation.
            if jobData.coldStart == 1:
                inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + "'" + '\n'
            if jobData.optSpinFlag == 1:
                inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + gageData.optLandRstFile + "'\n"
            if jobData.optSpinFlag == 0 and jobData.coldStart == 0:
                rstFile = outDir + "/RESTART." + bDate.strftime('%Y%m%d') + "00_DOMAIN1"
                inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + rstFile + "'" + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' ! Specification of simulation length in days OR hours\n')
        inStr = ' KHOUR = ' + str(dt.days*24 + int(dt.seconds/3600.0)) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' ! Physics options (see the documentation for details)\n')
        inStr = ' DYNAMIC_VEG_OPTION = ' + str(jobData.dynVegOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' CANOPY_STOMATAL_RESISTANCE_OPTION = ' + str(jobData.canStomOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' BTR_OPTION = ' + str(jobData.btrOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' RUNOFF_OPTION = ' + str(jobData.runOffOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' SURFACE_DRAG_OPTION = ' + str(jobData.sfcDragOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' FROZEN_SOIL_OPTION = ' + str(jobData.frzSoilOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' SUPERCOOLED_WATER_OPTION = ' + str(jobData.supCoolOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' RADIATIVE_TRANSFER_OPTION = ' + str(jobData.radTOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' SNOW_ALBEDO_OPTION = ' + str(jobData.snAlbOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' PCP_PARTITION_OPTION = ' + str(jobData.pcpPartOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' TBOT_OPTION = ' + str(jobData.tbotOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' TEMP_TIME_SCHEME_OPTION = ' + str(jobData.timeSchmOpt) + '\n'
        fileObj.write(inStr)
        inStr = ' GLACIER_OPTION = ' + str(jobData.glacier) + '\n'
        fileObj.write(inStr)
        inStr = ' SURFACE_RESISTANCE_OPTION = ' + str(jobData.sfcResOpt) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' ! Timestep in units of seconds\n')
        inStr = ' FORCING_TIMESTEP = ' + str(jobData.fDT) + '\n'
        fileObj.write(inStr)
        inStr = ' NOAH_TIMESTEP = ' + str(jobData.lsmDt) + '\n'
        fileObj.write(inStr)
        if minOutFlag == 1:
            # Produce minimal monthly outputs
            inStr = ' OUTPUT_TIMESTEP = 2592000\n'
        else:
            inStr = ' OUTPUT_TIMESTEP = ' + str(jobData.lsmOutDt) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' ! Land surface model restart file write frequency\n')
        if minOutFlag == 1:
            inStr = ' RESTART_FREQUENCY_HOURS = -9999\n'
        else:
            if int(jobData.lsmRstFreq) == -9999:
                inStr = ' RESTART_FREQUENCY_HOURS = -9999\n'
            else:
                inStr = ' RESTART_FREQUENCY_HOURS = ' + str(int(jobData.lsmRstFreq/3600.0)) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' ! Split output after split_output_count output times\n')
        fileObj.write(' SPLIT_OUTPUT_COUNT = 1\n')
        fileObj.write('\n')
        fileObj.write(' ! Soil layer specification\n')
        fileObj.write(' NSOIL = 4\n')
        inStr = ' soil_thick_input(1) = ' + str(jobData.soilThick[0]) + '\n'
        fileObj.write(inStr)
        inStr = ' soil_thick_input(2) = ' + str(jobData.soilThick[1]) + '\n'
        fileObj.write(inStr)
        inStr = ' soil_thick_input(3) = ' + str(jobData.soilThick[2]) + '\n'
        fileObj.write(inStr)
        inStr = ' soil_thick_input(4) = ' + str(jobData.soilThick[3]) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' ! Forcing data measurement height for winds, temp, humidity\n')
        inStr = ' ZLVL = ' + str(jobData.zLvl) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' ! Restart file format options\n')
        fileObj.write(' rst_bi_in = 0\n')
        fileObj.write(' rst_bi_out = 0\n')
        fileObj.write('\n')
        fileObj.write('/\n')
        fileObj.write('\n')
        fileObj.write('&WRF_HYDRO_OFFLINE\n')
        inStr = ' !Specifications of forcing data: 1=HRLDAS-hr format, 2=HRLDAS-min format ' + \
                '3=WRF, 4=Idealized, 5=Ideal w/ Spec.Precip., 6=HRLDAS-hrl y format w/ Spec. Precip.\n'
        fileObj.write(inStr)
        inStr = ' FORC_TYP = ' + str(jobData.fType) + '\n'
        fileObj.write(inStr)
        fileObj.write('/\n')
        fileObj.close
    except:
        if len(statusData.errMsg) == 0:
            statusData.errMsg = "ERROR: Failure to create: " + pathOut
        raise
    
def createHydroNL(statusData,gageData,jobData,outDir,typeFlag,bDate,eDate,genFlag):
    # General function for creation of a hydro.namelist file.
    # NOTE: typeFlag = 1 indicates cold start.
    #       typeFlag = 2 indicates restart.
    # NOTE: genFlag = 0 indicates a spinup - pull all parameter files from 
    #                   gageData
    #       genFlag = 1 indicartes a calibration - pull HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from the run directory.
    #       genFlag = 2 Indicates validation CTRL - pull HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from calibration output.
    #       genFlag = 3 Indicates validation BEST - pull HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from calibration output.
    #       genFlag = 4 Indicates sensitivy analysis - HYDRO_TBL_2D.nc, Fulldom.nc,
    #                   GWBUCKPARM.nc, and soil_properties.nc from run directory
    # Create path for the namelist file.
    pathOut = outDir + "/hydro.namelist"
    if os.path.isfile(pathOut):
        os.remove(pathOut)
        
    # If this is a calibration simulation, we need to check to see if we are 
    # past the date of minimal outputs. If the user specified to turn this optional
    # feature off, then by default, all simulations will produce full outputs
    # for the entire calibration iteration simulation.
    if jobData.optCalStripFlag == 1:
        minOutFlag = 1
        if bDate >= jobData.bCalibFullOutputs:
            minOutFlag = 0
        else:
            minOutFlag = 1
    else:
        minOutFlag = 0
        
    # Write each line of the hydro namelist file.
    try:
        fileObj = open(pathOut,'w')
        fileObj.write('&HYDRO_nlist\n')
        fileObj.write('!!!! ---------------------- SYSTEM COUPLING ---------------------- !!!!\n')
        fileObj.write('!Specify what is being coupled: 1=HRLDAS (offline Noah-LSM), 2=WRF, 3=NASA/LIS, 4=CLM\n')
        fileObj.write('\n')
        fileObj.write(' sys_cpl = 1\n')
        fileObj.write('\n')
        fileObj.write('!!!! ---------------------- MODEL INPUT DATA FILES ---------------------- !!!!\n')
        fileObj.write('\n')
        fileObj.write('!Specify land surface model gridded input data file...(e.g.: "geo_em.d01.nc")\n')
        inStr = ' GEO_STATIC_FLNM = "' + str(gageData.geoFile) + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the high-resolution routing terrain input data file...(e.g.: "Fulldom_hires.nc")\n')
        if genFlag == 0:
            inStr = ' GEO_FINEGRID_FLNM = "' + str(gageData.fullDom) + '"' + '\n'
        if genFlag == 1:
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.CALIB/OUTPUT/Fulldom.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' GEO_FINEGRID_FLNM = "' + pthTmp + '"\n'
        if genFlag == 2:
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.VALID/OUTPUT/CTRL/Fulldom.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' GEO_FINEGRID_FLNM = "' + pthTmp + '"\n'
        if genFlag == 3:
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.VALID/OUTPUT/BEST/Fulldom.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' GEO_FINEGRID_FLNM = "' + pthTmp + '"\n'
        if genFlag == 4:
            pthTmp = outDir + "/Fulldom.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' GEO_FINEGRID_FLNM = "' + pthTmp + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Specify the spatial hydro parameters file (e.g.: "hydro2dtbl.nc")\n')
        fileObj.write('! If you specify a filename and the file does not exist, it will be created for you.\n')
        if genFlag == 0:
            # Spinup
            inStr = ' HYDROTBL_F = "' + str(gageData.hydroSpatial) + '"' + '\n'
        if genFlag == 1:
            # Calibration run with updated parameter dataset
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.CALIB/OUTPUT/HYDRO_TBL_2D.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' HYDROTBL_F = "' + pthTmp + '"\n'
        if genFlag == 2:
            # Control validation simulation
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.VALID/OUTPUT/CTRL/HYDRO_TBL_2D.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' HYDROTBL_F = "' + pthTmp + '"\n'
        if genFlag == 3:
            # Best validation simulation
            pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                     str(gageData.gage) + "/RUN.VALID/OUTPUT/BEST/HYDRO_TBL_2D.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' HYDROTBL_F = "' + pthTmp + '"\n'
        if genFlag == 4:
            pthTmp = outDir + "/HYDRO_TBL_2D.nc"
            if not os.path.isfile(pthTmp):
                statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                raise Exception()
            inStr = ' HYDROTBL_F = "' + pthTmp + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Specify spatial metadata file for land surface grid. (e.g. "GEOGRID_LDASOUT_Spatial_Metadata.nc")\n')
        if str(gageData.landSpatialMeta) == '-9999':
            inStr = 'LAND_SPATIAL_META_FLNM = \'\'' + '\n'
        else:
            inStr = 'LAND_SPATIAL_META_FLNM = "' + str(gageData.landSpatialMeta) + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the name of the restart file if starting from restart... comment out with ! if not...\n')
        if typeFlag == 1:
            # We are cold-starting this simulation. This will most likely be for spinup
            # purposes.
            inStr = ' !RESTART_FILE = ""' + '\n'
        if typeFlag == 2:
            # We are restarting a model simulation that has either failed, or was killed.
            restartFile = outDir + "/HYDRO_RST." + bDate.strftime('%Y-%m-%d') + "_00:00_DOMAIN1"
            if not os.path.isfile(restartFile):
                statusData.errMsg = "ERROR: Failure to find: " + restartFile
                raise Exception()
            inStr = ' RESTART_FILE = "' + restartFile + '"' + '\n'
        if typeFlag == 3:
            # This is the beginning of a new sensitivity/calibration/validation
            # simulation.
            if jobData.coldStart == 1:
                inStr = ' !RESTART_FILE = ""' + '\n'
            if jobData.optSpinFlag == 1:
                inStr = ' RESTART_FILE = \"' + gageData.optHydroRstFile + '\"\n'
            if jobData.optSpinFlag == 0 and jobData.coldStart == 0:
                restartFile = outDir + "/HYDRO_RST." + bDate.strftime('%Y-%m-%d') + "_00:00_DOMAIN1"
                if not os.path.isfile(restartFile):
                    statusData.errMsg = "ERROR: Failure to find: " + restartFile
                    raise Exception()
                inStr = ' RESTART_FILE = "' + restartFile + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!!!! ---------------------- MODEL SETUP OPTIONS ---------------------- !!!!\n')
        fileObj.write('\n')
        fileObj.write('!Specify the domain or nest number identifier...(integer)\n')
        fileObj.write(' IGRID = 1\n')
        fileObj.write('\n')
        fileObj.write('!Specify the restart file write frequency...(minutes)\n')
        fileObj.write(' ! A value of -99999 will output restarts on the first day of the month only.\n')
        if minOutFlag == 1:
            inStr = ' rst_dt = -99999\n'
        else:
            if int(jobData.hydroRstFreq) == -99999:
                inStr = ' rst_dt = -99999\n'
            else:
                inStr = ' rst_dt = ' + str(int(jobData.hydroRstFreq/60.0)) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n') 
        fileObj.write('! Reset the LSM soil states from the high-res routing restart file (1=overwrite, 0 = no overwrite)\n')
        fileObj.write('! NOTE: Only turn this option on if overland or subsurface routing is active!\n')
        inStr = ' rst_typ = ' + str(jobData.rstType)  + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Restart file format control\n')
        fileObj.write(' rst_bi_in = 0  !0: use netcdf input restart file (default)\n')
        fileObj.write('                !1: use parallel io for reading multiple restart files, 1 per core\n')
        fileObj.write(' rst_bi_out = 0 !0: use netcdf output restart file (default)\n')
        fileObj.write('                !1: use paralle io for outputting multiple restart files, 1 per core\n')
        fileObj.write('\n')
        fileObj.write('!Restart switch to set restart accumulation variables = 0 (0-no reset, 1-yes reset to 0.0)\n')
        inStr = ' RSTRT_SWC = ' + str(jobData.resetHydro) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Specify baseflow/bucket model initialization...(0=cold start from table, 1=restart file)\n')
        if typeFlag == 1:
            # For cold-start spinups
            inStr = "GW_RESTART = 0\n"
        else:
            if jobData.gwBaseFlag == 0:
                # If the user has turned off the ground water buckets, 
                # turn this option off.
                inStr = "GW_RESTART = 0\n"
            else:
                # For all other runs that are not cold-start spinups. 
                inStr = "GW_RESTART = 1\n"
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!!!!------------ MODEL OUTPUT CONTROL ---------------!!!\n')
        fileObj.write('\n')
        fileObj.write('!Specify the output file write frequency...(minutes)\n')
        inStr = ' out_dt = ' + str(int(jobData.hydroOutDt/60.0)) + ' ! minutes' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the number of output times to be contained within each output history file...(integer)\n')
        fileObj.write('!  SET = 1 WHEN RUNNING CHANNEL ROUTING ONLY/CALIBRATION SIMS!!!!\n')
        fileObj.write('!  SET = 1 WHEN RUNNING COUPLED TO WRF!!!\n')
        fileObj.write(' SPLIT_OUTPUT_COUNT = 1\n')
        fileObj.write('\n')
        fileObj.write('!Specify the minimum stream order to output to netcdf point file...(integer)\n')
        fileObj.write('!Note: lower value of stream order produces more output\n')
        inStr = ' order_to_write = ' + str(jobData.strOrder) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Flag to turn configure output routines: 1 = with scale/offset/compression\n')
        fileObj.write('! 2 = with scale/offset/NO compression, 3 = compression only, 4 = no scale/offset/compression (default)\n')
        inStr = ' io_form_outputs = ' + str(jobData.ioFormOutputs) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Realtime run configuration option:\n')
        fileObj.write('! 0=all (default), 1=analysis, 2=short-range, 3=medium-range, 4=long-range, 5=retrospective\n')
        inStr = ' io_config_outputs = ' + str(jobData.ioConfigOutputs) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Option to write output files at time 0: 0=no, 1=yes (default)\n')
        fileObj.write(' t0OutputFlag = 0\n')
        fileObj.write('\n')
        fileObj.write('! Options to output channel & bucket influxes. Only active for UDMP_OPT=1.\n')
        fileObj.write('! Nonzero choice requires that out_dt above matches NOAH_TIMESTEP in namelist.hrldas.\n')
        fileObj.write('! 0=None (default), 1=channel influxes (qSfcLatRunoff,qBucket)\n')
        fileObj.write('! 2=channel+bucket fluxes (qSfcLatRunoff,qBucket,qBtmVertRunoff_toBucket)\n')
        fileObj.write('! 3=channel accumulations (accSfcLatRunoff, accBucket) *** NOT TESTED ***\n')
        fileObj.write(' output_channelBucket_influx = 0\n')
        fileObj.write('\n')
        fileObj.write('!Output netcdf file control\n')
        if minOutFlag == 1:
            # Turn all outputs off
            inStr = ' CHRTOUT_DOMAIN = 0 ! Netcdf point timeseries output at all channel points (1d)\n'
            fileObj.write(inStr)
            fileObj.write('                   ! 0 = no output, 1 = output\n')
            inStr = ' CHANOBS_DOMAIN = 0 ! Netcdf point timeseries at forecast points or gage points (defined in Routelink)\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output at forecast points or gage points\n')
            inStr = ' CHRTOUT_GRID = 0 ! Netcdf grid of channel streamflow values (2d)\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            fileObj.write('              ! NOTE: Not available with reach-based routing\n')
            inStr = ' LSMOUT_DOMAIN = 0 ! Netcdf grid of variables passed between LSM and routing components\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            fileObj.write('              ! NOTE: No scale_factor/add_offset available\n')
            inStr = ' RTOUT_DOMAIN = 0 ! Netcdf grid of terrain routing variables on routing grid\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            inStr = ' output_gw = 0 ! Netcdf point of GW buckets\n'
            fileObj.write('              ! 0 = no output, 1 = output\n')
            fileObj.write(inStr)
            inStr = ' outlake = 0 ! Netcdf point file of lakes (1d)\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            inStr = ' frxst_pts_out = 0 ! ASCII text file of forecast points or gage points (defined in Routelink)\n'
            fileObj.write(inStr)
        else:
            inStr = ' CHRTOUT_DOMAIN = ' + str(jobData.chrtoutDomain) + ' ! Netcdf point timeseries output at all channel points (1d)\n'
            fileObj.write(inStr)
            fileObj.write('                   ! 0 = no output, 1 = output\n')
            inStr = ' CHANOBS_DOMAIN = ' + str(jobData.chanObs) + ' ! Netcdf point timeseries at forecast points or gage points (defined in Routelink)\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output at forecast points or gage points\n')
            inStr = ' CHRTOUT_GRID = ' + str(jobData.chrtoutGrid) + ' ! Netcdf grid of channel streamflow values (2d)' + '\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            fileObj.write('              ! NOTE: Not available with reach-based routing\n')
            inStr = ' LSMOUT_DOMAIN = ' + str(jobData.lsmDomain) + ' ! Netcdf grid of variables passed between LSM and routing components\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            fileObj.write('              ! NOTE: No scale_factor/add_offset available\n')
            inStr = ' RTOUT_DOMAIN = ' + str(jobData.rtoutDomain) + ' ! Netcdf grid of terrain routing variables on routing grid\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            inStr = ' output_gw = ' + str(jobData.gwOut) + ' ! Netcdf point of GW buckets\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            inStr = ' outlake = ' + str(jobData.lakeOut) + ' ! Netcdf point file of lakes (1d)\n'
            fileObj.write(inStr)
            fileObj.write('              ! 0 = no output, 1 = output\n')
            inStr = ' frxst_pts_out = ' + str(jobData.frxstPts) + ' ! ASCII text file of forecast points or gage points (defined in Routelink)\n'
            fileObj.write(inStr)
        fileObj.write('              ! 0 = no output, 1 = output\n')
        fileObj.write('\n')
        fileObj.write('!!!! ---------------------- PHYSICS OPTIONS AND RELATED SETTINGS ---------------------- !!!!\n')
        fileObj.write('\n')
        fileObj.write('!Specify the number of soil layers (integer) and the depth of the bottom of of each layer (meters)...\n')
        fileObj.write('! Notes: In Version 1 of WRF-Hydro these must be the same as in the namelist.input file\n')
        fileObj.write('! Future versions will permit this to be different.\n')
        fileObj.write(' NSOIL=4\n')
        inStr = ' ZSOIL8(1) = ' + str((0.0 - jobData.soilThick[0])) + '\n'
        fileObj.write(inStr)
        inStr = ' ZSOIL8(2) = ' + str((0.0 - jobData.soilThick[0] - jobData.soilThick[1])) + '\n'
        fileObj.write(inStr)
        inStr = ' ZSOIL8(3) = ' + str((0.0 - jobData.soilThick[0] - jobData.soilThick[1] - jobData.soilThick[2])) + '\n'
        fileObj.write(inStr)
        inStr = ' ZSOIL8(4) = ' + str((0.0 - jobData.soilThick[0] - jobData.soilThick[1] - jobData.soilThick[2] - jobData.soilThick[3])) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the grid spacing of the terrain routing grid...(meters)\n')
        inStr = ' DXRT = ' + str(float(gageData.dxHydro)) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the integer multiple between the land model grid and the terrain routing grid...(integer)\n')
        inStr = ' AGGFACTRT = ' + str(int(gageData.aggFact)) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Specify the routing model timestep...(seconds)\n')
        inStr = ' DTRT_CH = ' + str(jobData.dtChRt) + '\n'
        fileObj.write(inStr)
        inStr = ' DTRT_TER = ' + str(jobData.dtTerRt) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Switch activate sucsurface routing...(0=no, 1=yes)\n')
        inStr = ' SUBRTSWCRT = ' + str(jobData.subRtFlag) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Switch activate surface overland flow routing...(0=no, 1=yes)\n')
        inStr = ' OVRTSWCRT = ' + str(jobData.ovrRtFlag) + '\n'
        fileObj.write(inStr)
        fileObj.write('!Specify overland flow routing option: 1=Steepest Descent(D8) 2=CASC2D\n')
        fileObj.write(' ! NOTE: Currently subsurface flow is only steepest descent\n')
        inStr = ' rt_option = ' + str(jobData.rtOpt) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Switch to activate channel routing:\n')
        inStr = ' CHANRTSWCRT = ' + str(jobData.chnRtFlag) + '\n'
        fileObj.write(inStr)
        fileObj.write('!Specify channel routing option: 1=Muskingam-reach, 2=Musk.-Cunge-reach, 3=Diff.Wave-gridded\n')
        inStr = ' channel_option = ' + str(jobData.chnRtOpt) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the reach file for reach-based routing options...\n')
        if str(gageData.rtLnk) == '-9999':
            inStr = ' route_link_f = \'\'' + '\n'
        else:
            inStr = ' route_link_f = "' + str(gageData.rtLnk) + '"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! If using channel_option=2, activate the compound channel formulation? (Default=.FALSE.)\n')
        if jobData.cmpdChan == 1:
            fileObj.write('compound_channel = .TRUE.\n')
        else:
            fileObj.write('compound_channel = .FALSE.\n')
        fileObj.write('! Specify the simulated lakes for NHDPlus reach-based routing\n')
        if str(gageData.lkFile) == '-9999':
            inStr = ' route_lake_f = \'\'' + '\n'
        else:
            inStr = ' route_lake_f = "' + str(gageData.lkFile) + '"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Switch to activate baseflow bucket model...(0=none, 1=exp. bucket, 2=pass-through\n')
        inStr = ' GWBASESWCRT = ' + str(jobData.gwBaseFlag) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Groundwater/baseflow mask specified on land surface model grid...\n')
        fileObj.write('!Note: Only required in baseflow bucket model is active\n')
        fileObj.write('!gwbasmskfil will not be used if UDMP_OPT = 1\n')
        if str(gageData.gwMask) == '-9999':
            inStr = ' gwbasmskfil = \'\'' + '\n'
        else:
            inStr = ' gwbasmskfil = "' + str(gageData.gwMask) + '"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Groundwater bucket parameter file (e.g.: "GWBUCKPARM.nc")\n')
        if jobData.gwBaseFlag == 1:
            if genFlag == 0:
                inStr = ' GWBUCKPARM_file = "' + str(gageData.gwFile) + '"\n'
            if genFlag == 1:
                pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                         str(gageData.gage) + "/RUN.CALIB/OUTPUT/GWBUCKPARM.nc"
                if not os.path.isfile(pthTmp):
                    statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                    raise Exception()
                inStr = ' GWBUCKPARM_file = "' + pthTmp + '"\n'
            if genFlag == 2:
                pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                         str(gageData.gage) + "/RUN.VALID/OUTPUT/CTRL/GWBUCKPARM.nc"
                if not os.path.isfile(pthTmp):
                    statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                    raise Exception()
                inStr = ' GWBUCKPARM_file = "' + pthTmp + '"\n'
            if genFlag == 3:
                pthTmp = str(jobData.outDir) + "/" + str(jobData.jobName) + "/" + \
                         str(gageData.gage) + "/RUN.VALID/OUTPUT/BEST/GWBUCKPARM.nc"
                if not os.path.isfile(pthTmp):
                    statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                    raise Exception()
                inStr = ' GWBUCKPARM_file = "' + pthTmp + '"\n'
            if genFlag == 4:
                pthTmp = outDir + "/GWBUCKPARM.nc"
                if not os.path.isfile(pthTmp):
                    statusData.errMsg = "ERROR: Failure to find: " + pthTmp
                    raise Exception()
                inStr = ' GWBUCKPARM_file = "' + pthTmp + '"' + '\n'
        else:
            # We aren't running with the ground water bucket model.
            inStr = ' GWBUCKPARM_file = "-9999"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! User defined mapping, such NHDPlus\n')
        fileObj.write('!0: default none. 1: yes\n')
        inStr = ' UDMP_OPT = ' + str(jobData.udmpOpt) + '\n'
        fileObj.write(inStr)
        fileObj.write('! If on, specify the user-defined mapping file (e.g.: "spatialWeights.nc")\n')
        if jobData.udmpOpt == 1:
            inStr = ' udmap_file = "' + str(gageData.udMap) + '"\n'
        else:
            inStr = '!udmap_file = "./DOMAIN/spatialweights.nc"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('/')
        fileObj.write('\n')
        fileObj.write('&NUDGING_nlist\n')
        fileObj.write('\n')
        fileObj.write(' ! Path to the "timeslice" observation files.\n')
        fileObj.write('timeSlicePath = "./nudgingTimeSliceObs"\n')
        fileObj.write('\n')
        fileObj.write('nudgingParamFile  = "foo"\n')
        fileObj.write('\n')
        fileObj.write(' ! Nudging restart file = "nudgingLastObsFile"\n')
        fileObj.write(' ! nudgingLastObsFile defaults to "", which will look for nudgingLastObs.YYYY-mm-dd_HH:MM:SS.nc\n')
        fileObj.write(' !  **AT THE INITIALIZATION TIME OF THE RUN**. Set to a missing file to use no restart.\n')
        fileObj.write(' !nudgingLastObsFile = "foo"\n')
        fileObj.write('\n')
        fileObj.write('!! Parallel input of nudging timeslice observation files?\n')
        fileObj.write(' readTimeSliceParallel = .TRUE.\n')
        fileObj.write('\n')
        fileObj.write('! TemporalPersistence defaults to true, only runs if necessary params present.\n')
        fileObj.write(' temporalPersistence = .TRUE.\n')
        fileObj.write('\n')
        fileObj.write(' ! The total number of last (obs,modeled) pairs to save in nudgingLastObs for\n')
        fileObj.write(' ! removal of bias. This is the maximum array length. (This option is active when persistBias=FALSE)\n')
        fileObj.write(' ! (Default=960=10days @15 min obs resolution, if all the obs are present and no longer if not.)\n')
        fileObj.write(' nLastObs = 960\n')
        fileObj.write('\n')
        fileObj.write(' ! If using temporalPersistence the last observation persists by default.\n')
        fileObj.write(' ! This option instead persists the bias after the last observation.\n')
        fileObj.write(' persistBias = .FALSE.\n')
        fileObj.write('\n')
        fileObj.write(' ! AnA (FALSE) vs Forecast (TRUE) bias persistence.\n')
        fileObj.write(' ! If persistBias: Does the window for calculating the bias end at\n')
        fileObj.write(' ! model init time (=t0)?\n')
        fileObj.write(' ! FALSE = window ends at model time (moving),\n')
        fileObj.write(' ! TRUE = window ends at init=t0(fcst) time.\n')
        fileObj.write(' ! (If commented out, Default=FALSE) time.\n')
        fileObj.write(' ! Note: Perfect restart tests require this option to be .FALSE.\n')
        fileObj.write(' biasWindowBeforeT0 = .FALSE.\n')
        fileObj.write('\n')
        fileObj.write(' ! If persistBias: Only use this many last (obs,modeled) pairs. (If Commented out, Default=-1*nLastObs)\n')
        fileObj.write(' ! > 0: apply an age-based filter, units=hours.\n')
        fileObj.write(' ! = 0: apply no additional filter, use all available/usable obs.\n')
        fileObj.write(' ! < 0: apply an count-based filter, units=count\n')
        fileObj.write(' maxAgePairsBiasPersist = -960\n')
        fileObj.write('\n')
        fileObj.write(' ! If persistBias: The minimum number of last (obs,modeled) pairs, with age less than\n')
        fileObj.write(' ! maxAgePairsBiasPersist, required to apply a bias correction. (default=8)\n')
        fileObj.write(' minNumPairsBiasPersist = 8\n')
        fileObj.write('\n')
        fileObj.write(' ! If persistBias: give more weight to observations closer in time? (default=FALSE)\n')
        fileObj.write(' invDistTimeWeightBias = .TRUE.\n')
        fileObj.write('\n')
        fileObj.write(' ! If persistBias: "No constructive interference in bias correction?", Reduce the bias adjustment\n')
        fileObj.write(' ! when the model and the bias adjustment have the same sign relative to the modeled flow at t0?\n')
        fileObj.write(' (default=FALSE)\n')
        fileObj.write(' ! Note: Perfect restart tests require this option to be .FALSE.\n')
        fileObj.write(' noConstInterBias = .FALSE.\n')
        fileObj.write('/')
        fileObj.close
    except:
        if len(statusData.errMsg) == 0:
            statusData.errMsg = "ERROR: Failure to create " + pathOut
        raise
