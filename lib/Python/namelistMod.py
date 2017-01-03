# General function library for creating namelist.hrldas and hydro.namelist 
# files using information the user provided, and basin-specific information.

# Logan Karsten
# National Center for Atmospheric Research 
# Research Applications Laboratory

import os

def createHrldasNL(gageData,jobData,outDir,typeFlag):
    # General function for creation of a namelist.hrldas file.
    
    # NOTE: typeFlag = 1 indicates spinup, 2 indicates calibration.
    
    # Create path for the namelist file
    pathOut = outDir + "/namelist.hrldas"
    if os.path.isfile(pathOut):
        jobData.errMsg = "ERROR: " + pathOut + " already exists."
        raise
    
    print pathOut
    # Write each line of the expected hrldas.namelist file.
    #try:
    fileObj = open(pathOut,'w')
    fileObj.write('&NOAHLSM_OFFLINE\n')
    fileObj.write('\n')
    inStr = ' HRLDAS_SETUP_FILE = "' + str(gageData.wrfInput) + '"' + '\n'
    fileObj.write(inStr)
    inStr = ' INDIR = "' + jobData.fDir + '"' + '\n'
    fileObj.write(inStr)
    inStr = ' SPATIAL_FILENAME = "' + gageData.soilFile + '"' + '\n'
    fileObj.write(inStr)
    inStr = ' OUTDIR = "' + outDir + '"' + '\n'
    fileObj.write(inStr)
    fileObj.write('\n')
    if typeFlag == 1:  # Spinup
        dt = jobData.eSpinDate - jobData.bSpinDate
        inStr = ' START_YEAR = ' + jobData.bSpinDate.strftime('%Y') + '\n'
        fileObj.write(inStr)
        inStr = ' START_MONTH = ' + jobData.bSpinDate.strftime('%m') + '\n'
        fileObj.write(inStr)
        inStr = ' START_DAY = ' + jobData.bSpinDate.strftime('%d') + '\n'
        fileObj.write(inStr)
    else:  # Calibration
        dt = jobData.eCalibDate - jobData.bCalibDate
        inStr = ' START_YEAR = ' + jobData.bCalibDate.strftime('%Y') + '\n'
        fileObj.write(inStr)
        inStr = ' START_MONTH = ' + jobData.bCalibDate.strftime('%m') + '\n'
        fileObj.write(inStr)
        inStr = ' START_DAY = ' + jobData.bCalibDate.strftime('%d') + '\n'
        fileObj.write(inStr)
    fileObj.write(' START_HOUR = 00\n')
    fileObj.write(' START_MIN = 00\n')
    fileObj.write('\n')
    if typeFlag == 1:
        inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + "'" + '\n' 
    else:
        # PLACEHOLDER FOR CHECKING FOR SPINUP RESTART
        rstFile = ''
        inStr = ' RESTART_FILENAME_REQUESTED = ' + "'" + rstFile + "'" + '\n'
    fileObj.write(inStr)
    fileObj.write('\n')
    inStr = ' KDAY = ' + dt.days + '\n'
    fileObj.write(inStr)
    fileObj.write('\n')
    inStr = ' DYNAMIC_VEG_OPTION = ' + jobData.dynVegOpt + '\n'
    fileObj.write(inStr)
    inStr = ' CANOPY_STOMATAL_RESISTANCE_OPTION = ' + jobData.canStomOpt + '\n'
    fileObj.write(inStr)
    inStr = ' BTR_OPTION = ' + jobData.btrOpt + '\n'
    fileObj.write(inStr)
    inStr = ' RUNOFF_OPTION = ' + jobData.runOffOpt + '\n'
    fileObj.write(inStr)
    inStr = ' SURFACE_DRAG_OPTION = ' + jobData.sfcDragOpt + '\n'
    fileObj.write(inStr)
    inStr = ' FROZEN_SOIL_OPTION = ' + jobData.frzSoilOpt + '\n'
    fileObj.write(inStr)
    inStr = ' SUPERCOOLED_WATER_OPTION = ' + jobData.supCoolOpt + '\n'
    fileObj.write(inStr)
    inStr = ' RADIATIVE_TRANSFER_OPTION = ' + jobData.radTOpt + '\n'
    fileObj.write(inStr)
    inStr = ' SNOW_ALBEDO_OPTION = ' + jobData.snAlbOpt + '\n'
    fileObj.write(inStr)
    inStr = ' PCP_PARTITION_OPTION = ' + jobData.pcpPartOpt + '\n'
    fileObj.write(inStr)
    inStr = ' TBOT_OPTION = ' + jobData.tbotOpt + '\n'
    fileObj.write(inStr)
    inStr = ' TEMP_TIME_SCHEME_OPTION = ' + jobData.timeSchmOpt + '\n'
    fileObj.write(inStr)
    inStr = ' GLACIER_OPTION = ' + jobData.glacier + '\n'
    fileObj.write(inStr)
    inStr = ' SURFACE_RESISTANCE_OPTION = ' + jobData.sfcResOpt + '\n'
    fileObj.write(inStr)
    fileObj.write('\n')
    fileObj.write('\n')
    inStr = ' FORCING_TIMESTEP = ' + jobData.fDT + '\n'
    fileObj.write(inStr)
    inStr = ' NOAH_TIMESTEP = ' + jobData.lsmDt + '\n'
    fileObj.write(inStr)
    inStr = ' OUTPUT_TIMESTEP = ' + jobData.lsmOutDt + '\n'
    fileObj.write(inStr)
    fileObj.write('\n')
    inStr = ' RESTART_FREQUENCY_HOURS = ' + int(jobData.lsmRstFreq/3600.0) + '\n'
    fileObj.write(inStr)
    fileObj.write(' ! Split output after split_output_count output times\n')
    fileObj.write(' SPLIT_OUTPUT_COUNT = 1\n')
    fileObj.write('\n')
    fileObj.write('\n')
    fileObj.write(' ! XSTART = 1\n')
    fileObj.wrtie(' ! XEND = 1\n')
    fileObj.write(' ! YSTART = 1\n')
    fileObj.write(' ! YEND = 1\n')
    fileObj.write('\n')
    fileObj.write(' NSOIL = 4\n')
    inStr = ' soil_thick_input(1) = ' + jobData.soilThick[0] + '\n'
    fileObj.write(inStr)
    inStr = ' soil_thick_input(2) = ' + jobData.soilThick[1] + '\n'
    fileObj.write(inStr)
    inStr = ' soil_thick_input(3) = ' + jobData.soilThick[2] + '\n'
    fileObj.write(inStr)
    inStr = ' soil_thick_input(4) = ' + jobData.soilThick[3] + '\n'
    fileObj.write(inStr)
    fileObj.write('\n')
    inStr = ' ZLVL = ' + jobData.zLvl + '\n'
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
    inStr = ' FORC_TYP = ' + jobData.fType + '\n'
    fileObj.write(inStr)
    inStr = ' !HRLDAS_ini_typ 1: initial and parameters from forcing; 0: from wrfinput\n'
    fileObj.write(inStr)
    inStr = ' HRLDAS_ini_type = 1' + '\n'
    fileObj.write(inStr)
    fileObj.write(' ! for extract greenfrac\n')
    inStr = ' GEO_STATIC_FLNM = "' + gageData.geoFile + '"' + '\n'
    fileObj.write(inStr)
    fileObj.write('/')
    fileObj.close
    #except:
    #    jobData.errMsg = "ERROR: Failure to create: " + pathOut
    #    raise
    
def createHydroNL(gageData,jobData,outDir,typeFlag):
    # General function for creation of a hydro.namelist file.

    # Create path for the namelist file.
    pathOut = outDir + "/hydro.namelist"
    if os.path.isfile(pathOut):
        jobData.errMsg = "ERROR: " + pathOut + " already exists."
        raise
        
    # Write each line of the hydro namelist file.
    try:
        fileObj = open(pathOut,'w')
        fileObj.write('&HYDRO_nlist')
        fileObj.write('\n')
        fileObj.write('!!!! SYSTEM COUPLING !!!!')
        fileObj.write('!Specify what is being coupled: 1=HRLDAS (offline Noah-LSM), 2=WRF, 3=NASA/LIS, 4=CLM')
        fileObj.write(' sys_cpl = 1')
        fileObj.write('\n')
        fileObj.write('!!!! MODEL INPUT DATA FILES !!!!')
        fileObj.write('!Specify land surface model gridded input data file...(e.g.: "geo_em.d03.nc")')
        inStr = ' GEO_STATIC_FLNM = "' + gageData.geoFile + '"'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the high-resolution routing terrain input data file...(e.g.: "Fulldom_hires_hydrofile.nc")')
        inStr = ' GEO_FINEGRID_FLNM = "' + gageData.fullDom + '"'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the name of the restart file if starting from restart... comment out with ! if not...')
        if typeFlag == 1: # Spinup
            inStr = ' !RESTART_FILE = ""'
            fileObj.write(inStr)
        else: # Calibration
            # PLACEHOLDER FOR RESTART FILE 
            restartFile = ''
            inStr = ' RESTART_FILE = "' + restartFile + '"'
            fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!!!! MODEL SETUP AND I/O CONTROL !!!!')
        fileObj.write('!Specify the domain or nest number identifier...(integer)')
        fileObj.write(' IGRID = 1')
        fileObj.write('\n')
        fileObj.write('!Specify the restart file write frequency...(minutes)')
        inStr = ' rst_dt = ' + int(jobData.hydroRstFreq/60.0)
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the output file write frequency...(minutes)')
        inStr = ' out_dt = ' + int(jobData.hydroOutDt/60.0) + ' ! minutes'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the number of output times to be contained within each output history file...(integer)')
        fileObj.write('!  SET = 1 WHEN RUNNING CHANNEL ROUTING ONLY/CALIBRATION SIMS!!!!')
        fileObj.write('!  SET = 1 WHEN RUNNING COUPLED TO WRF!!!')
        fileObj.write(' SPLIT_OUTPUT_COUNT = 1')
        fileObj.write('\n')
        fileObj.write(' rst_typ = 1 : overwrite the soil variables from routing restart file')
        inStr = ' rst_typ = ' + jobData.rstType 
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Output netcdf file control')
        inStr = ' CHRTOUT_DOMAIN = ' + jobData.chrtoutDomain + ' ! Netcdf point timeseries output at all channel points' + \
                ':0 = no output; 1=CHANOBS and lateral inflows; 2=no CHANOBS & thinned channel output'
        fileObj.write(inStr)
        inStr = ' CHRTOUT_GRID = ' + jobData.chrtoutGrid + ' ! Netcdf grid of channel streamflow values'
        fileObj.write(inStr)
        inStr = ' LSMOUT_DOMAIN = ' + jobData.lsmDomain + ' ! Netcdf grid of variables passed between LSM and routing components'
        fileObj.write(inStr)
        inStr = ' RTOUT_DOMAIN = ' + jobData.rtoutDomain + ' ! Netcdf grid of terrain routing variables on routing grid'
        fileObj.write(inStr)
        inStr = ' output_gw = ' + jobData.gwOut + ' ! Netcdf point of GW buckets'
        fileObj.write(inStr)
        inStr = ' outlake = ' + jobData.lakeOut + ' ! Netcdf point file of lakes'
        fileObj.write(inStr)
        fileObj.write('\n')
        inStr = ' IOCflag = ' + jobData.iocFlag + ' ! Flag for IOC configuration outputs: 0-diagnostics; ' + \
                '1-Anal_Assim; 2-Short; 3-Medium; 4-Long'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write(' rst_bi_in = 0')
        fileObj.write(' rst_bi_out = 0')
        fileObj.write('\n')
        fileObj.write('!Restart switch to set restart accumulation variables = 0 (0-no reset, 1-yes reset to 0.0)')
        inStr = ' RSTRT_SWC = ' + jobData.resetHydro
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the minimum stream order to output to netcdf point file...(integer)')
        fileObj.write('!Note: lower value of stream order produces more output')
        inStr = ' order_to_write = ' + jobData.strOrder
        fileObj.write('\n')
        fileObj.write('!!!! PHYSICS OPTIONS AND RELATED SETTINGS !!!!')
        fileObj.write('!Switch for terrain adjustment of incoming solar radiation: 0=no, 1=yes')
        fileObj.write('!Note: This option is not yet active in Version 1.0...')
        fileObj.write('!      WRF has this capability so be careful not to double apply the correction!!!!')
        inStr = ' TERADJ_SOLAR = ' + jobData.solarAdj
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Specify the number of soil layers (integer) and the depth of the bottom of of each layer (meters)...')
        fileObj.write('! Notes: In Version 1 of WRF-Hydro these must be the same as in the namelist.input file')
        fileObj.write('! Future versions will permit this to be different.')
        fileObj.write(' NSOIL=4')
        inStr = ' ZSOIL8(1) = ' + (0.0 - jobData.soilThick[0])
        fileObj.write(inStr)
        inStr = ' ZSOIL8(2) = ' + (0.0 - jobData.soilThick[0] - jobData.soilThick[1])
        fileObj.write(inStr)
        inStr = ' ZSOIL8(3) = ' + (0.0 - jobData.soilThick[0] - jobData.soilThick[1] - jobData.soilThick[2])
        fileObj.write(inStr)
        inStr = ' ZSOIL8(4) = ' + (0.0 - jobData.soilThick[0] - jobData.soilThick[1] - jobData.soilThick[2] - jobData.soilThick[3])
        fileObj.write(inStr)
        fileObj.write('\n')
        # PLACEHOLDER FOR DXRT and AGGFACRT
        fileObj.write('!Specify the grid spacing of the terrain routing grid...(meters)')
        fileObj.write(' DXRT = 250.0')
        fileObj.write('\n')
        fileObj.write('!Specify the integer multiple between the land model grid and the terrain routing grid...(integer)')
        fileObj.write(' AGGFACTRT = 4')
        fileObj.write('\n')
        fileObj.write('! Specify the routing model timestep...(seconds)')
        inStr = ' DTRT_CH = ' + jobData.dtChRt
        fileObj.write(inStr)
        inStr = ' DTRT_TER = ' + jobData.dtTerRt
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Switch activate sucsurface routing...(0=no, 1=yes)')
        inStr = ' SUBRTSWCRT = ' + jobData.subRtFlag
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Switch activate surface overland flow routing...(0=no, 1=yes)')
        inStr = ' OVRTSWCRT = ' + jobData.ovrRtFlag
        fileObj.write(inStr)
        fileObj.write('!Specify overland flow routing option: 1=Steepest Descent(D8) 2=CASC2D')
        inStr = ' rt_option = ' + jobData.rtOpt
        fileObj.write('\n')
        fileObj.write('!Switch to activate channel routing:')
        inStr = ' CHANRTSWCRT = ' + jobData.chRtFlag
        fileObj.write(inStr)
        fileObj.write('!Specify channel routing option: 1=Muskingam-reach, 2=Musk.-Cunge-reach, 3=Diff.Wave-gridded')
        inStr = ' channel_option = ' + jobData.chnRtOpt
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Speicy the reach file for reach-based routing options...')
        inStr = ' route_link_f = "' + gageData.rtLnk + '"'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('! Specify the simulated lakes for NHDPlus reach-based routing')
        inStr = ' route_lake_f = "' + gageData.lkFile + '"'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Switch to activate baseflow bucket model...(0=none, 1=exp. bucket, 2=pass-through')
        inStr = ' GWBASESWCRT = ' + jobData.gwBaseFlag
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('!Groundwater/baseflow mask specified on land surface model grid...')
        fileObj.write('!Note: Only required in baseflow bucket model is active')
        fileObj.write('!gwbasmskfil will not be used if UDMP_OPT = 1')
        inStr = ' GWBUCKPARM_file = "' + gageData.gwFile + '"'
        fileObj.write('\n')
        fileObj.write('! User defined mapping, such NHDPlus')
        fileObj.write('!0: default none. 1: yes')
        fileObj.write(' UDMP_OPT = 1')
        inStr = ' udmap_file = "' + gageData.udMap + '"'
        fileObj.write(inStr)
        fileObj.write('\n')
        fileObj.write('/')
        fileObj.write('\n')
        fileObj.write('&NUDGING_nlist')
        fileObj.write('nudgingParamFile  = "foo"')
        fileObj.write('!netwkReExFile = "foo"')
        fileObj.write('\n')
        fileObj.write('!! Parallel input of nudging timeslice observation files?')
        fileObj.write(' readTimeSliceParallel = .TRUE.')
        fileObj.write('\n')
        fileObj.write('! TemporalPersistence defaults to true, only runs if necessary params present.')
        fileObj.write(' temporalPersistence = .TRUE.')
        fileObj.write('\n')
        fileObj.write('! nudgingLastObsFile defaults to '', which will look for nudgingLastObs.YYYY-mm-dd_HH:MM:SS.nc')
        fileObj.write('!   **AT THE INITALIZATION TIME OF THE RUN**. Set to a missing file to use no restart.')
        fileObj.write('!nudgingLastObsFile   = "notAFile.junk"')
        fileObj.write('/')
        fileObj.close
    except:
        jobData.errMsg = "ERROR: Failure to create " + pathOut
        raise