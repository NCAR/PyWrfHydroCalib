# General function library for creating namelist.hrldas and hydro.namelist 
# files using information the user provided, and basin-specific information.

# Logan Karsten
# National Center for Atmospheric Research 
# Research Applications Laboratory

import os

def createHrldasNL(gageData,jobData,outDir,typeFlag,bDate,eDate):
    # General function for creation of a namelist.hrldas file.
    
    # NOTE: typeFlag = 1 indicates cold start.
    #       typeFlag = 2 indicates restart.
    # Create path for the namelist file
    pathOut = outDir + "/namelist.hrldas"
    if os.path.isfile(pathOut):
        jobData.errMsg = "ERROR: " + pathOut + " already exists."
        raise Exception()
    
    # Write each line of the expected hrldas.namelist file.
    try:
        fileObj = open(pathOut,'w')
        fileObj.write('&NOAHLSM_OFFLINE\n')
        fileObj.write('\n')
        inStr = ' HRLDAS_SETUP_FILE = "' + str(gageData.wrfInput) + '"' + '\n'
        fileObj.write(inStr)
        inStr = ' INDIR = "' + str(gageData.forceDir) + '"' + '\n'
        fileObj.write(inStr)
        inStr = ' SPATIAL_FILENAME = "' + str(gageData.soilFile) + '"' + '\n'
        fileObj.write(inStr)
        inStr = ' OUTDIR = "' + outDir + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
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
        if typeFlag == 1:
            inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + "'" + '\n' 
        else:
            rstFile = outDir + "/RESTART." + bDate.strftime('%Y%m%d%H') + "_DOMAIN1"
            inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + rstFile + "'" + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = ' KDAY = ' + str(dt.days) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
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
        fileObj.write('\n')
        inStr = ' FORCING_TIMESTEP = ' + str(jobData.fDT) + '\n'
        fileObj.write(inStr)
        inStr = ' NOAH_TIMESTEP = ' + str(jobData.lsmDt) + '\n'
        fileObj.write(inStr)
        inStr = ' OUTPUT_TIMESTEP = ' + str(jobData.lsmOutDt) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = ' RESTART_FREQUENCY_HOURS = ' + str(int(jobData.lsmRstFreq/3600.0)) + '\n'
        fileObj.write(inStr)
        fileObj.write(' ! Split output after split_output_count output times\n')
        fileObj.write(' SPLIT_OUTPUT_COUNT = 1\n')
        fileObj.write('\n')
        fileObj.write('\n')
        fileObj.write(' ! XSTART = 1\n')
        fileObj.write(' ! XEND = 1\n')
        fileObj.write(' ! YSTART = 1\n')
        fileObj.write(' ! YEND = 1\n')
        fileObj.write('\n')
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
        inStr = ' ZLVL = ' + str(jobData.zLvl) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
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
        inStr = ' !HRLDAS_ini_typ 1: initial and parameters from forcing; 0: from wrfinput\n'
        fileObj.write(inStr)
        inStr = ' HRLDAS_ini_typ = 1' + '\n'
        fileObj.write(inStr)
        fileObj.write(' ! for extract greenfrac\n')
        inStr = ' GEO_STATIC_FLNM = "' + str(gageData.geoFile) + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('/')
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create: " + pathOut
        raise
    
def createHydroNL(gageData,jobData,outDir,typeFlag,bDate,eDate):
    # General function for creation of a hydro.namelist file.

    # Create path for the namelist file.
    pathOut = outDir + "/hydro.namelist"
    if os.path.isfile(pathOut):
        jobData.errMsg = "ERROR: " + pathOut + " already exists."
        raise Exception()
        
    # Write each line of the hydro namelist file.
    try:
        fileObj = open(pathOut,'w')
        fileObj.write('&HYDRO_nlist\n')
        fileObj.write('\n')
        fileObj.write('!!!! SYSTEM COUPLING !!!!\n')
        fileObj.write('!Specify what is being coupled: 1=HRLDAS (offline Noah-LSM), 2=WRF, 3=NASA/LIS, 4=CLM\n')
        fileObj.write(' sys_cpl = 1\n')
        fileObj.write('\n')
        fileObj.write('!!!! MODEL INPUT DATA FILES !!!!\n')
        fileObj.write('!Specify land surface model gridded input data file...(e.g.: "geo_em.d03.nc")\n')
        inStr = ' GEO_STATIC_FLNM = "' + str(gageData.geoFile) + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the high-resolution routing terrain input data file...(e.g.: "Fulldom_hires_hydrofile.nc")\n')
        inStr = ' GEO_FINEGRID_FLNM = "' + str(gageData.fullDom) + '"' + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the name of the restart file if starting from restart... comment out with ! if not...\n')
        if typeFlag == 1: # Spinup
            inStr = ' !RESTART_FILE = ""' + '\n'
            fileObj.write(inStr)
        elif typeFlag == 2: # Calibration
            restartFile = outDir + "/HYDRO_RST." + bDate.strftime('%Y-%m-%d_%H') + ":00_DOMAIN2"
            inStr = ' RESTART_FILE = "' + restartFile + '"' + '\n'
            fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!!!! MODEL SETUP AND I/O CONTROL !!!!\n')
        fileObj.write('!Specify the domain or nest number identifier...(integer)\n')
        fileObj.write(' IGRID = 1\n')
        fileObj.write('\n')
        fileObj.write('!Specify the restart file write frequency...(minutes)\n')
        inStr = ' rst_dt = ' + str(int(jobData.hydroRstFreq/60.0)) + '\n'
        fileObj.write(inStr)
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
        fileObj.write(' !rst_typ = 1 : overwrite the soil variables from routing restart file\n')
        inStr = ' rst_typ = ' + str(jobData.rstType)  + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Output netcdf file control\n')
        inStr = ' CHRTOUT_DOMAIN = ' + str(jobData.chrtoutDomain) + ' ! Netcdf point timeseries output at all channel points' + \
                ':0 = no output; 1=CHANOBS and lateral inflows; 2=no CHANOBS & thinned channel output\n'
        fileObj.write(inStr)
        inStr = ' CHRTOUT_GRID = ' + str(jobData.chrtoutGrid) + ' ! Netcdf grid of channel streamflow values' + '\n'
        fileObj.write(inStr)
        inStr = ' LSMOUT_DOMAIN = ' + str(jobData.lsmDomain) + ' ! Netcdf grid of variables passed between LSM and routing components\n'
        fileObj.write(inStr)
        inStr = ' RTOUT_DOMAIN = ' + str(jobData.rtoutDomain) + ' ! Netcdf grid of terrain routing variables on routing grid\n'
        fileObj.write(inStr)
        inStr = ' output_gw = ' + str(jobData.gwOut) + ' ! Netcdf point of GW buckets\n'
        fileObj.write(inStr)
        inStr = ' outlake = ' + str(jobData.lakeOut) + ' ! Netcdf point file of lakes\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = ' IOCflag = ' + str(jobData.iocFlag) + ' ! Flag for IOC configuration outputs: 0-diagnostics; ' + \
                '1-Anal_Assim; 2-Short; 3-Medium; 4-Long\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' rst_bi_in = 0\n')
        fileObj.write(' rst_bi_out = 0\n')
        fileObj.write('\n')
        fileObj.write('!Restart switch to set restart accumulation variables = 0 (0-no reset, 1-yes reset to 0.0)\n')
        inStr = ' RSTRT_SWC = ' + str(jobData.resetHydro) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the minimum stream order to output to netcdf point file...(integer)\n')
        fileObj.write('!Note: lower value of stream order produces more output\n')
        inStr = ' order_to_write = ' + str(jobData.strOrder) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!!!! PHYSICS OPTIONS AND RELATED SETTINGS !!!!\n')
        fileObj.write('!Switch for terrain adjustment of incoming solar radiation: 0=no, 1=yes\n')
        fileObj.write('!Note: This option is not yet active in Version 1.0...\n')
        fileObj.write('!      WRF has this capability so be careful not to double apply the correction!!!!\n')
        inStr = ' TERADJ_SOLAR = ' + str(jobData.solarAdj) + '\n'
        fileObj.write(inStr)
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
        # PLACEHOLDER FOR DXRT and AGGFACRT
        fileObj.write('!Specify the grid spacing of the terrain routing grid...(meters)\n')
        fileObj.write(' DXRT = 250.0\n')
        fileObj.write('\n')
        fileObj.write('!Specify the integer multiple between the land model grid and the terrain routing grid...(integer)\n')
        fileObj.write(' AGGFACTRT = 4\n')
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
        inStr = ' rt_option = ' + str(jobData.rtOpt) + '\n'
        fileObj.write('\n')
        fileObj.write('!Switch to activate channel routing:\n')
        inStr = ' CHANRTSWCRT = ' + str(jobData.chnRtFlag) + '\n'
        fileObj.write(inStr)
        fileObj.write('!Specify channel routing option: 1=Muskingam-reach, 2=Musk.-Cunge-reach, 3=Diff.Wave-gridded\n')
        inStr = ' channel_option = ' + str(jobData.chnRtOpt) + '\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Speicy the reach file for reach-based routing options...\n')
        inStr = ' route_link_f = "' + str(gageData.rtLnk) + '"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Specify the simulated lakes for NHDPlus reach-based routing\n')
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
        inStr = ' GWBUCKPARM_file = "' + str(gageData.gwFile) + '"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! User defined mapping, such NHDPlus\n')
        fileObj.write('!0: default none. 1: yes\n')
        fileObj.write(' UDMP_OPT = 1\n')
        inStr = ' udmap_file = "' + str(gageData.udMap) + '"\n'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('/')
        fileObj.write('\n')
        fileObj.write('&NUDGING_nlist\n')
        fileObj.write('nudgingParamFile  = "foo"\n')
        fileObj.write('!netwkReExFile = "foo"\n')
        fileObj.write('\n')
        fileObj.write('!! Parallel input of nudging timeslice observation files?\n')
        fileObj.write(' readTimeSliceParallel = .TRUE.\n')
        fileObj.write('\n')
        fileObj.write('! TemporalPersistence defaults to true, only runs if necessary params present.\n')
        fileObj.write(' temporalPersistence = .TRUE.\n')
        fileObj.write('\n')
        fileObj.write('! nudgingLastObsFile defaults to '', which will look for nudgingLastObs.YYYY-mm-dd_HH:MM:SS.nc\n')
        fileObj.write('!   **AT THE INITALIZATION TIME OF THE RUN**. Set to a missing file to use no restart.\n')
        fileObj.write('!nudgingLastObsFile   = "notAFile.junk"\n')
        fileObj.write('/')
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create " + pathOut
        raise