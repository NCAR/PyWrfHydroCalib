#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly=TRUE)
namelistFile <- args[1]
#namelistFile <- "validScript.R"

#.libPaths("/glade/u/home/adugger/system/R/Libraries/R3.2.2")
#library(rwrfhydro)
library(data.table)
library(ggplot2)
library(plyr)
library(gridExtra)
library(zoo)
library(qmap)
#########################################################
# SETUP
#########################################################

source("calib_utils.R")
source(namelistFile)
# passed in from namelist: runDir, validDir, objFn, siteId, linkId, ncores, startCalibDate, endCalibDate, startValidDate, endValidDate

attach(paste0(runDir, "/proj_data.Rdata"), name="calibdb")
#gageIndx <- gageIndx
metrics <- metrics # right now the metrics in this script is the metrics for the streamflow ... 
metrics_streamflow <- metrics
metrics_snow <- metrics_snow
metrics_soilmoisture <- metrics_soilmoisture
if (enableStreamflowCalib == 1) obsStreamData <- obsStreamData
if (enableSnowCalib == 1) obsSnowData <- obsSnowData 
if (enableSoilMoistureCalib == 1) obsSoilData <- obsSoilData
mskvar.lsm <- mskvar.lsm
window_days <- window_days
event_metrics_daily <- event_metrics_daily # Xia 20210610
detach(calibdb)

#########################################################
# MAIN CODE
#########################################################

# params for validation: x0 (control) and x_best (best)

# Read model out and calculate performance metric
outPathControl <- paste0(validDir, "/OUTPUT/CTRL/")
write(paste0("Control run output dir: ", outPathControl), stdout())

outPathValid <- paste0(validDir, "/OUTPUT/BEST/")
write(paste0("Validation run output dir: ", outPathValid), stdout())

# Setup parallel
if (ncores>1) {
     parallelFlag <- TRUE
     library(doParallel)
     #cl <- makeForkCluster(ncores)
     cl <- makePSOCKcluster(ncores)
     registerDoParallel(cl)
} else {
     parallelFlag <- FALSE
}

# Date bounds
minDate <- min(startCalibDate, startValidDate)
maxDate <- max(endCalibDate, endValidDate)
#minDate <- startValidDate # this was used for the hyper res calibration 
#maxDate <- endValidDate # this was used for the hyper res calibration

# Read files
if (enableStreamflowCalib == 1) {
if (hydro_SPLIT_OUTPUT_COUNT == 1) {
   write(paste0("Reading control run model out files. Parallel ", parallelFlag, " ncores=", ncores), stdout())
    filesList <- list.files(path = outPathControl,
                        pattern = glob2rx("*.CHANOBS_DOMAIN*"),
                        full.names = TRUE)
    filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
    whFiles <- which(filesListDate >= minDate)
    filesList <- filesList[whFiles]
    if (length(filesList) == 0) stop("No matching files in specified directory.")
    chrt <- as.data.table(plyr::ldply(filesList, ReadChFile_Multi, .parallel = parallelFlag))
    setnames(chrt, "streamflow", "q_cms")
    setnames(chrt, "feature_id", "FID")

    if (enableMultiSites == 0) {
      chrt <- subset(chrt, FID == linkId)
      chrt$site_no = siteId
    } else { # enableMultiSites = 1
      calib_sites <- read.csv(paste0(runDir, "/calib_sites.csv"), colClasses= c("integer", "character", "numeric"))
      chrt <- merge(chrt, calib_sites[, c("FID", "site_no")],  by = "FID")
    }
    chrt.cont <- copy(chrt)

} else if (hydro_SPLIT_OUTPUT_COUNT == 0) {

   write(paste0("Reading control run model out file : CHANOBS_DOMAIN1.nc"), stdout())
   chanobsFile <- list.files(outPathControl, pattern = glob2rx("CHANOBS_DOMAIN1.nc"), full.names = TRUE)
   q_cms = ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "streamflow")
   featureIdTmp <- ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "feature_id", collapse_degen = FALSE)

   if (length(dim(q_cms)) != 1) {
       q_cms <- as.data.frame(t(q_cms)) # R totate the matrix when it is reading it on
    } else {
       q_cms <- as.data.frame(q_cms)
    }

    names(q_cms) <- featureIdTmp
    chrt <- as.data.table(q_cms)

    q_cms$POSIXct<-as.POSIXct(ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "time")*60,
                        origin = "1970-01-01 00:00:00 UTC", tz = "UTC") # because the time is minutes from this origin

    if (enableMultiSites == 0) {
       q_cms <- q_cms[, c(as.character(linkId), "POSIXct")]
   } else {
       calib_sites <- read.csv(paste0(runDir, "/calib_sites.csv"), colClasses= c("integer", "character", "numeric"))
       q_cms <- q_cms[, c(as.character(calib_sites$FID), "POSIXct")]
   }
   chrt <-as.data.table(q_cms)

   # If the model crashes, then it would be append to the file after restarting the model,
   # therefore, we need to remove the duplicates in order to not double count for those.
   chrt <- unique(chrt)

   # remove the spin up part
   chrt <- chrt[POSIXct > minDate, ]

   # melt the data.frame
   chrt = melt(chrt, id.vars = c("POSIXct"), variable.factor = FALSE)
   setnames(chrt, "variable", "FID")
   setnames(chrt, "value", "q_cms")
   chrt$FID <- as.integer(chrt$FID)

   # add site_no information
   if (enableMultiSites == 0) {
       chrt$site_no <- siteId
   } else {
       chrt <- merge(chrt, calib_sites[, c("FID", "site_no")], by = "FID")
   }
    chrt.cont <- copy(chrt)
}

if (hydro_SPLIT_OUTPUT_COUNT == 1) {
   write(paste0("Reading validation run model out files. Parallel ", parallelFlag, " ncores=", ncores), stdout())
     filesList <- list.files(path = outPathValid,
                        pattern = glob2rx("*.CHANOBS_DOMAIN*"),
                        full.names = TRUE)
     filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
     whFiles <- which(filesListDate >= minDate)
     filesList <- filesList[whFiles]
     if (length(filesList) == 0) stop("No matching files in specified directory.")
      chrt <- as.data.table(plyr::ldply(filesList, ReadChFile_Multi, .parallel = parallelFlag))
      setnames(chrt, "streamflow", "q_cms")
      setnames(chrt, "feature_id", "FID")

      if (enableMultiSites == 0) {
        chrt <- subset(chrt, FID == linkId)
        chrt$site_no = siteId
      } else { # enableMultiSites = 1
        calib_sites <- read.csv(paste0(runDir, "/calib_sites.csv"), colClasses= c("integer", "character", "numeric"))
        chrt <- merge(chrt, calib_sites[, c("FID", "site_no")],  by = "FID")
      }
     chrt.valid <- copy(chrt)

} else if (hydro_SPLIT_OUTPUT_COUNT == 0) {

   write(paste0("Reading validation run model out file : CHANOBS_DOMAIN1.nc"), stdout())
   chanobsFile <- list.files(outPathValid, pattern = glob2rx("CHANOBS_DOMAIN1.nc"), full.names = TRUE)
   q_cms = ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "streamflow")
      featureIdTmp <- ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "feature_id", collapse_degen = FALSE)

      if (length(dim(q_cms)) != 1) {
         q_cms <- as.data.frame(t(q_cms)) # R totate the matrix when it is reading it on
      } else {
         q_cms <- as.data.frame(q_cms)
      }

      names(q_cms) <- featureIdTmp
      chrt <- as.data.table(q_cms)

      q_cms$POSIXct<-as.POSIXct(ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "time")*60,
                          origin = "1970-01-01 00:00:00 UTC", tz = "UTC") # because the time is minutes from this origin

     if (enableMultiSites == 0) {
         q_cms <- q_cms[, c(as.character(linkId), "POSIXct")]
     } else {
         calib_sites <- read.csv(paste0(runDir, "/calib_sites.csv"), colClasses= c("integer", "character", "numeric"))
         q_cms <- q_cms[, c(as.character(calib_sites$FID), "POSIXct")]
     }
     chrt <-as.data.table(q_cms)

     # If the model crashes, then it would be append to the file after restarting the model,
     # therefore, we need to remove the duplicates in order to not double count for those.
     chrt <- unique(chrt)

     # remove the spin up part
     chrt <- chrt[POSIXct > minDate, ]

     # melt the data.frame
     chrt = melt(chrt, id.vars = c("POSIXct"), variable.factor = FALSE)
     setnames(chrt, "variable", "FID")
     setnames(chrt, "value", "q_cms")
     chrt$FID <- as.integer(chrt$FID)

     # add site_no information
     if (enableMultiSites == 0) {
         chrt$site_no <- siteId
     } else {
         chrt <- merge(chrt, calib_sites[, c("FID", "site_no")], by = "FID")
     }

   chrt.valid <- copy(chrt)
}

# Check for empty output
if (nrow(chrt.cont) < 1) {
       write(paste0("No data found in control model output for link ", linkId, " after start date ", minDate), stdout())
       fileConn <- file(paste0(runDir, "/CALC_STATS_MISSING"))
       writeLines('', fileConn)
       close(fileConn)
       quit("no")
}

if (nrow(chrt.valid) < 1) {
       write(paste0("No data found in validation model output for link ", linkId, " after start date ", minDate), stdout())
       fileConn <- file(paste0(runDir, "/CALC_STATS_MISSING"))
       writeLines('', fileConn)
       close(fileConn)
       quit("no")
}

# Convert to daily if needed
if (calcDailyStats) {
  chrt.cont.d <- Convert2Daily(chrt.cont)
  chrt.valid.d <- Convert2Daily(chrt.valid)
  chrt.cont.obj <- copy(chrt.cont.d)
  chrt.valid.obj <- copy(chrt.valid.d)
  obs.obj <- Convert2Daily(obsStreamData)
  obs.obj$threshold <- obsStreamData$threshold[1] ## this is not going to be correct if we are doing the multi gage calibration. 
} else {
  chrt.cont.obj <- copy(chrt.cont)
  chrt.valid.obj <- copy(chrt.valid)
  obs.obj <- copy(obsStreamData)
}

# Merge
setkey(chrt.cont.obj, "site_no", "POSIXct")
if ("Date" %in% names(obs.obj)) obs.obj[, Date := NULL]
setkey(obs.obj, "site_no", "POSIXct")
#chrt.cont.obj <- merge(chrt.cont.obj, obs.obj, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)
chrt.cont.obj <- merge(chrt.cont.obj, obs.obj, by=c("site_no", "POSIXct"), all.x=TRUE, all.y=FALSE) # Xia 20210610, use all data to remove fake lines in hydrograph

setkey(chrt.valid.obj, "site_no", "POSIXct")
#chrt.valid.obj <- merge(chrt.valid.obj, obs.obj, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)
chrt.valid.obj <- merge(chrt.valid.obj, obs.obj, by=c("site_no", "POSIXct"), all.x=TRUE, all.y=FALSE) # Xia 20210610

# Check for empty output
if (nrow(chrt.cont.obj) < 1) {
    write(paste0("No data found in obs for gage ", siteId, " after start date ", minDate), stdout())
    fileConn <- file(paste0(runDir, "/CALC_STATS_MISSING"))
    writeLines('', fileConn)
    close(fileConn)
    quit("no")
}

if (nrow(chrt.valid.obj) < 1) {
    write(paste0("No data found in obs for gage ", siteId, " after start date ", minDate), stdout())
    fileConn <- file(paste0(runDir, "/CALC_STATS_MISSING"))
    writeLines('', fileConn)
    close(fileConn)
    quit("no")
}

# Setup for stats loop
runList <- list(df=c("chrt.cont.obj", "chrt.valid.obj"), run=c("default", "calibrated"))
dtList <- list(start=c(startCalibDate, startValidDate, minDate), 
               end=c(endCalibDate, endValidDate, maxDate),
               period=c("calib", "valid", "full"))
#dtList <- list(start=c(startValidDate), # this was used for the hyper res 
#               end=c(endValidDate),
#               period=c("valid"))

# Initialize stats table
validStats <- as.data.frame(matrix(, nrow=1, ncol=length(metrics)+3))
names(validStats) <- c("run", "period", "obj", metrics)

validStats_2 <- as.data.frame(matrix(, nrow=1, ncol=length(metrics)+3+1))
names(validStats_2) <- c("run", "period", "obj", metrics, "site_no")

# Calculate stats
loopcnt <- 1
loopcnt2 <- 1
for (i in 1:length(runList[[1]])) {
   for (j in 1:length(dtList[[1]])) {
      # Subset data
      chrt.obj <- get(runList[["df"]][i])
      chrt.obj <- chrt.obj[POSIXct >= dtList[["start"]][j] & POSIXct < dtList[["end"]][j],]
      chrt.obj.nona <- chrt.obj[!is.na(q_cms) & !is.na(obs),]

      if (enableMultiSites == 0) {
         if (any(c("POD", "FAR", "CSI") %in% metrics)) chrt.obj.nona.abcd1 <- calc_abcd1(data.frame(chrt.obj.nona), threshColName = "threshold",obsColName = "obs",modColName = "q_cms", headerCols=c('site_no'))
         if (any(c("corr1", "lbem", "lbemprime") %in% metrics)) chrt.obj.nona.nozeros = noZeroFunction(chrt.obj.nona$q_cms, chrt.obj.nona$obs, lubridate::month(chrt.obj.nona$POSIXct))
      }else{
        # add the weights for each gage:
        chrt.obj.nona <- merge(chrt.obj.nona, calib_sites, by = c("site_no"))
        if (any(c("POD", "FAR", "CSI") %in% metrics))  chrt.obj.nona.abcd1 <- calc_abcd1(data.frame(chrt.obj.nona), threshColName = "threshold",obsColName = "obs",modColName = "q_cms", headerCols=c('site_no', 'weight'))
        if (any(c("corr1", "lbem", "lbemprime") %in% metrics)) chrt.obj.nona.nozeros = chrt.obj.nona[, .(q_cms = noZeroFunction(q_cms, obs, lubridate::month(POSIXct))$q_cms,
                                                obs = noZeroFunction(q_cms, obs, lubridate::month(POSIXct))$obs,
                                                period = noZeroFunction(q_cms, obs, lubridate::month(POSIXct))$period) ,
                                            by = c("site_no", "weight")] # take care of the non zeros for each site
       }

      if (calcDailyStats) scales=c(1,10,30) else scales=c(1,24)
      my_exprs = quote(list(
        cor = cor(q_cms, obs),
        rmse = Rmse(q_cms, obs, na.rm=TRUE),
        bias = PBias(q_cms, obs, na.rm=TRUE),
        nse = hydroGOF::NSE(q_cms, obs, na.rm=TRUE, FUN=NULL, epsilon="Pushpalatha2012"),
        nselog = NseLogM(q_cms, obs), # to consider adding constant value to station with the occurrence of zero flows 
        nsewt = NseWtM(q_cms, obs), 
        nnse = NNse(q_cms, obs),
        nnsesq = NNseSq(q_cms, obs), 
        kge = hydroGOF::KGE(q_cms, obs, na.rm=TRUE, method="2009", out.type="single"), 
        hyperResMultiObj = hyperResMultiObj(q_cms, obs, na.rm=TRUE),
        msof = Msof(q_cms, obs, scales),
        #eventmultiobj = EventMultiObj(q_cms, obs, weight1=1, weight2=0, POSIXct, siteId) 
      ))
      my_exprs2 = quote(list(
        corr1 = r1(q_cms, obs), # Calculate Stedingers r1
        lbem = LBEms_function(q_cms, obs, period, calcDailyStats)[1],
        lbemprime =  LBEms_function(q_cms, obs, period, calcDailyStats)[2]
      ))
      my_exprs3 = quote(list( # Xia 20210610 to use all data with NA included
        eventmultiobj = EventMultiObj(q_cms, obs, weight1, weight2, POSIXct, siteId, basinType)[[1]],
        peak_bias = EventMultiObj(q_cms, obs, weight1, weight2, POSIXct, siteId, basinType)[[2]],
        peak_tm_err_hr = EventMultiObj(q_cms, obs, weight1, weight2, POSIXct, siteId, basinType)[[3]],
        event_volume_bias = EventMultiObj(q_cms, obs, weight1, weight2, POSIXct, siteId, basinType)[[4]]
      ))
      w = which(names(my_exprs) %in% metrics)
      w2 = which(names(my_exprs2) %in% metrics)
      if (!calcDailyStats) w3 = which(names(my_exprs3) %in% metrics) # Xia 20210610

      # let s just take care of objective function being capital
      objFn <- tolower(streamflowObjFunc)

      if (enableMultiSites == 0) {
        stat <- chrt.obj.nona[, eval(my_exprs[c(1,w)]), by = NULL]
        if (length(w2) > 0) stat <- cbind(stat, chrt.obj.nona.nozeros[, eval(my_exprs2[c(1,w2)]), by = NULL])
        if (!calcDailyStats) {if (length(w3) >0) stat <- cbind(stat, chrt.obj[, eval(my_exprs3[c(1,w3)]), by = NULL])} # Xia 20210610
        if (calcDailyStats) stat <- cbind(stat, event_metrics_daily) # Xia 20210610

        if (any(c("POD", "FAR", "CSI") %in% metrics)) {
           stat$POD = calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$POD
           stat$FAR = calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$FAR
           stat$CSI = calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$CSI
        }

        # Calc objective function
        if (objFn %in% c("nsewt","nse","nselog","nnsesq","nnse", "kge","cor","corr1", "lbem","lbemprime")) F_new_streamflow <- 1 - stat[, objFn, with = FALSE]
        if (objFn %in% c("rmse","msof","hyperResMultiObj","eventmultiobj")) F_new_streamflow <- stat[, objFn, with = FALSE] 

        # Archive results
        validStats_new <- cbind(data.table(run = runList[["run"]][i], period = dtList[["period"]][j], obj= F_new_streamflow), stat[, c(metrics), with = FALSE])
        validStats[loopcnt,] <- validStats_new
        loopcnt <- loopcnt + 1

     } else { #enableMultiSites = 1

        stat <- chrt.obj.nona[, eval(my_exprs[c(1,w)]), by = c("site_no", "weight")]
        if (length(w2) > 0){
            stat <- merge(stat, chrt.obj.nona.nozeros[, eval(my_exprs2[c(1,w2)]), by = c("site_no", "weight")],by = c("site_no", "weight"))
        }
        if (!calcDailyStats){ # Xia 20210610
            if (length(w3) > 0){
              stat <- merge(stat, chrt.obj[, eval(my_exprs3[c(1,w3)]), by = c("site_no", "weight")],by = c("site_no", "weight"))
            }
        }
        if (calcDailyStats) stat <- cbind(stat, event_metrics_daily) # Xia 20210610
        if (any(c("POD", "FAR", "CSI") %in% metrics)) {
             stat <- merge(stat, calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "weight", "threshName")),
                           by = c("site_no", "weight"))
        }

        statW <- c() # calculate the weighted metrics
        for (im in metrics){
          statW <- c(statW, stat[, c(im, "weight"), with = FALSE][, .(fn = sum(weight*get(im)))]$fn)
        }
        names(statW) <- metrics

        if (objFn %in% c("nsewt","nse","nselog","nnsesq","nnse","kge","cor","corr1", "lbem","lbemprime")) F_new_streamflow <- 1 - stat[, objFn, with = FALSE] 
        if (objFn %in% c("rmse","msof","hyperResMultiObj","eventmultiobj")) F_new_streamflow <- statW[objFn] 

      # Archive results
      validStats[loopcnt,] <- c(runList[["run"]][i], dtList[["period"]][j],  F_new_streamflow, statW)

      index1 = (loopcnt - 1) *nrow(stat)
      for(k in 1:nrow(stat)) {
        validStats_2[index1+k,] <- cbind(data.table(run = runList[["run"]][i], period = dtList[["period"]][j], obj= F_new_streamflow), stat[, c(metrics), with = FALSE], stat$site_no[k])
      }
      loopcnt = loopcnt  + 1
   }
 }
}
}
rm(chrt.obj, validStats_new)


########################################################
# PLOTS
#########################################################

writePlotDir <- paste0(validDir, "/plots")
dir.create(writePlotDir)

if (enableStreamflowCalib == 1) {
# Hydrographs
gg <- ggplot() +
              geom_line(data=chrt.cont.obj, aes(x=POSIXct, y=q_cms, color='default'), lwd=0.6) +
              geom_line(data=chrt.valid.obj, aes(x=POSIXct, y=q_cms, color='calibrated'), lwd=0.6) +
              geom_line(data=chrt.cont.obj, aes(x=POSIXct, y=obs, color='observed'), lwd=0.4) +
              geom_vline(xintercept=as.numeric(startValidDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              geom_vline(xintercept=as.numeric(endValidDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              geom_vline(xintercept=as.numeric(startCalibDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              geom_vline(xintercept=as.numeric(endCalibDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              ggtitle(paste0("Model Validation Hydrograph: ", siteId, "\n", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange', 'black'),
                                limits=c('default','calibrated','observed'),
                                label=c('default', 'calibrated', 'observed')) +
              labs(x="", y="Streamflow (m3/s)") +
              theme_bw() + theme_bw(base_size = 20) + facet_wrap(~site_no, ncol = 1)

ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_hydrogr.png"),
              plot=gg, units="in", width=16, height=8, dpi=300)
ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_hydrogr_log.png"),
              plot=gg+scale_y_log10(), units="in", width=16, height=8, dpi=300)

# Scatterplots
maxval <- max(max(chrt.cont.obj$q_cms, na.rm=TRUE), max(chrt.valid.obj$q_cms, na.rm=TRUE), max(chrt.cont.obj$obs, na.rm=TRUE))
gg1 <- ggplot() +
              geom_point(data=chrt.cont.obj, aes(x=obs, y=q_cms, color='default'), shape=1, size=3) +
              geom_point(data=chrt.valid.obj, aes(x=obs, y=q_cms, color='calibrated'), shape=1, size=3) +
              scale_shape_discrete(solid=FALSE) +
              geom_abline(intercept=0, slope=1, col='black', lty=1) +
              ggtitle(paste0("Full Period (", minDate, " to ", maxDate, "): \n", siteId, " ", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                                limits=c('default','calibrated'),
                                label=c('default', 'calibrated')) +
              labs(x="", y="modeled streamflow (m3/s)") +
              theme_bw() + theme(legend.position="none") + theme(axis.text=element_text(size=20), axis.title=element_text(size=20)) +
              xlim(0,maxval) + ylim(0,maxval) + facet_wrap(~site_no, ncol = 1)
gg2 <- ggplot() +
              geom_point(data=chrt.cont.obj[POSIXct >= startCalibDate & POSIXct < endCalibDate,], aes(x=obs, y=q_cms, color='default'), shape=1, size=3) +
              geom_point(data=chrt.valid.obj[POSIXct >= startCalibDate & POSIXct < endCalibDate,], aes(x=obs, y=q_cms, color='calibrated'), shape=1, size=3) +
              scale_shape_discrete(solid=FALSE) +
              geom_abline(intercept=0, slope=1, col='black', lty=1) +
              ggtitle(paste0("Calibration Period (", startCalibDate, " to ", endCalibDate, "): \n", siteId, " ", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                                limits=c('default','calibrated'),
                                label=c('default', 'calibrated')) +
              labs(x="observed streamflow (m3/s)", y="") +
              theme_bw() + theme(legend.position="none") + theme(axis.text=element_text(size=20), axis.title=element_text(size=20)) +
              xlim(0,maxval) + ylim(0,maxval)  + facet_wrap(~site_no, ncol = 1)


gg3 <- ggplot() +
              geom_point(data=chrt.cont.obj[POSIXct >= startValidDate & POSIXct < endValidDate,], aes(x=obs, y=q_cms, color='default'), shape=1, size=3) +
              geom_point(data=chrt.valid.obj[POSIXct >= startValidDate & POSIXct < endValidDate,], aes(x=obs, y=q_cms, color='calibrated'), shape=1, size=3) +
              scale_shape_discrete(solid=FALSE) +
              geom_abline(intercept=0, slope=1, col='black', lty=1) +
              ggtitle(paste0("Validation Period (", startValidDate, " to ", endValidDate, "): \n", siteId, " ", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                                limits=c('default','calibrated'),
                                label=c('default', 'calibrated')) +
              labs(x="", y="") +
              theme_bw() + theme(axis.text=element_text(size=20)) +
              theme(legend.position = c(0.2, 0.9),  legend.background = element_rect(colour = NA, fill = NA), legend.key.size = unit(1.25, "cm"),  legend.key = element_rect(colour = NA, fill = NA), legend.text = element_text(size=18)) +
              xlim(0,maxval) + ylim(0,maxval) + facet_wrap(~site_no, ncol = 1)

gg.all <- grid.arrange(gg1, gg2, gg3, ncol=3)
# gg.all <- grid.arrange(gg3,ncol=1) For the hyper res calirbation the gg1 and gg2 were commented out

ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_scatter.png"),
              plot=gg.all, units="in", width=16, height=8, dpi=300)


# Stats Barplots
results.plot <- melt(validStats[,c("run", "period", "obj", metrics)], id=c("period", "run"))
results.plot$period <- factor(results.plot$period, levels=c("calib", "valid", "full"))
#results.plot$period <- factor(results.plot$period, levels=c("valid")) # The above line was commented out for the hyper res calibration and this one was used
results.plot$run <- factor(results.plot$run, levels=c("default", "calibrated"))
results.plot <- results.plot[order(results.plot$variable, results.plot$period, results.plot$run),]
results.plot$value <- as.numeric(results.plot$value)
gg <- ggplot(data=results.plot, aes(x=factor(period), y=value, fill=run)) +
         geom_bar(stat="identity", position="dodge") +
         facet_wrap(~variable, scales="free_y") +
         scale_fill_manual(name="", values=c('dodgerblue', 'orange'),
             limits=c('default','calibrated'),
             label=c('default', 'calibrated')) +
         ggtitle(paste0("Model Validation Performance Metrics: ", siteId, "\n", siteName)) +
         labs(x="run period", y="value") +
         theme_bw() + theme_bw(base_size = 20)
ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_metrics.png"),
        plot=gg, units="in", width=16, height=8, dpi=300)

}


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  
#                SNOW
#
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if (enableSnowCalib == 1) {
   if (lsm_SPLIT_OUTPUT_COUNT == 1) {
     filesList <- list.files(path = outPathControl,
                             pattern = glob2rx("*0600.LDASOUT_DOMAIN*"),
                             full.names = TRUE)
      filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
      whFiles <- which(filesListDate >= minDate)
      filesList <- filesList[whFiles]
      if (length(filesList) == 0) stop("No matching files in specified directory.")
      mod.cont <- as.data.table(plyr::ldply(filesList, ReadSwe_Multi, .parallel = parallelFlag, mskvar.lsm = mskvar.lsm))
      mod.cont$site_no <- siteId
   
      write(paste0("Reading model out files. Parallel ", parallelFlag, " ncores=", ncores), stdout())
      filesList <- list.files(path = outPathValid,
                             pattern = glob2rx("*0600.LDASOUT_DOMAIN*"),
                             full.names = TRUE)
      filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
      whFiles <- which(filesListDate >= minDate)
      filesList <- filesList[whFiles]
      if (length(filesList) == 0) stop("No matching files in specified directory.")
      mod.valid <- as.data.table(plyr::ldply(filesList, ReadSwe_Multi, .parallel = parallelFlag, mskvar.lsm = mskvar.lsm))
      mod.valid$site_no <- siteId
   }

# Convert to daily if needed
  mod.cont.obj <- copy(mod.cont)
  mod.valid.obj <- copy(mod.valid)
  obs.obj.snow <- copy(obsSnowData)

# Merge
setkey(mod.cont.obj, "site_no", "POSIXct")
if ("Date" %in% names(obs.obj.snow)) obs.obj.snow[, Date := NULL]
setkey(obs.obj.snow, "site_no", "POSIXct")
mod.cont.obj <- merge(mod.cont.obj, obs.obj.snow, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)
mod.cont.obj.snow <- copy(mod.cont.obj)

setkey(mod.valid.obj, "site_no", "POSIXct")
mod.valid.obj <- merge(mod.valid.obj, obs.obj.snow, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)
mod.valid.obj.snow <- copy(mod.valid.obj)

# setup for stats loop
runList <- list(df=c("mod.cont.obj", "mod.valid.obj"), run=c("default", "calibrated"))
dtList <- list(start=c(startCalibDate, startValidDate, minDate),
               end=c(endCalibDate, endValidDate, maxDate),
               period=c("calib", "valid", "full"))
# Initialize stats table
validStats_snow <- as.data.frame(matrix(, nrow=1, ncol=length(metrics_snow)+3))
names(validStats_snow) <- c("run", "period", "obj", metrics_snow)

# Calculate stats
loopcnt <- 1
loopcnt2 <- 1
for (i in 1:length(runList[[1]])) {
   for (j in 1:length(dtList[[1]])) {
      # Subset data
      mod.obj <- get(runList[["df"]][i])
      mod.obj <- mod.obj[POSIXct >= dtList[["start"]][j] & POSIXct < dtList[["end"]][j],]
      mod.obj.nona <- mod.obj[!is.na(mod) & !is.na(obs),]

      if (any(c("POD", "FAR", "CSI") %in% metrics_snow)) mod.obj.nona.abcd1 <- calc_abcd1(data.frame(mod.obj.nona), threshColName = "threshold",obsColName = "obs",modColName = "mod", headerCols=c('site_no'))
      if (any(c("corr1", "lbem", "lbemprime") %in% metrics_snow)) mod.obj.nona.nozeros = noZeroFunction_snow(mod.obj.nona$mod, mod.obj.nona$obs, lubridate::month(mod.obj.nona$POSIXct))

      scales=c(1,24) # we are not doing daily
      my_exprs = quote(list(
        cor = cor(mod, obs),
        rmse = Rmse(mod, obs, na.rm=TRUE),
        bias = PBias(mod, obs, na.rm=TRUE),
        nse = hydroGOF::NSE(mod, obs, na.rm=TRUE, FUN=NULL, epsilon="Pushpalatha2012"),
        #nselog = hydroGOF::NSE(mod, obs, na.rm=TRUE, FUN=log, epsilon="Pushpalatha2012"),  # Xia commented below lines 20210610
        #nsewt = NseWt(mod, obs) ,
        #nnse = NNse(mod, obs),
        #nnsesq = NNseSq(mod, obs),
        kge = hydroGOF::KGE(mod, obs, na.rm=TRUE, method="2012", out.type="single"),
        #hyperResMultiObj = hyperResMultiObj(mod, obs, na.rm=TRUE),
        #msof = Msof(mod, obs, scales),
        #eventmultiobj = EventMultiObj(mod, obs, weight1=1, weight2=0, POSIXct, siteId)
      ))
      #my_exprs2 = quote(list(
      #  corr1 = r1(mod, obs), # Calculate Stedingers r1
      #  lbem = LBEms_function(mod, obs, period, calcDailyStats)[1],
      #  lbemprime =  LBEms_function(mod, obs, period, calcDailyStats)[2]
      #))
      w = which(names(my_exprs) %in% metrics_snow)
      #w2 = which(names(my_exprs2) %in% metrics_snow)

      # let s just take care of objective function being capital
      objFn <- tolower(snowObjFunc)

        stat <- mod.obj.nona[, eval(my_exprs[c(1,w)]), by = NULL]
        #if (length(w2) > 0) stat <- cbind(stat, mod.obj.nona.nozeros[, eval(my_exprs2[c(1,w2)]), by = NULL])

        if (any(c("POD", "FAR", "CSI") %in% metrics_snow)) {
           stat$POD = calc_contingency_stats(mod.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$POD
           stat$FAR = calc_contingency_stats(mod.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$FAR
           stat$CSI = calc_contingency_stats(mod.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$CSI
        }

        # Calc objective function
        if (objFn %in% c("nsewt","nse","nselog","kge","cor","corr1", "lbem","lbemprime")) F_new <- 1 - stat[, objFn, with = FALSE]
        if (objFn %in% c("rmse","msof","hyperResMultiObj")) F_new <- stat[, objFn, with = FALSE]

        # Archive results
        validStats_new <- cbind(data.table(run = runList[["run"]][i], period = dtList[["period"]][j], obj= F_new), stat[, c(metrics_snow), with = FALSE])
        validStats_snow[loopcnt,] <- validStats_new
        loopcnt <- loopcnt + 1
 }
}
}

# ------------------------------------------------------
#  SNOW  PLOTS
#-------------------------------------------------------
if (enableSnowCalib) {

# Time series
gg <- ggplot() +
              geom_line(data=mod.cont.obj, aes(x=POSIXct, y=mod, color='default'), lwd=0.6) +
              geom_line(data=mod.valid.obj, aes(x=POSIXct, y=mod, color='calibrated'), lwd=0.6) +
              geom_line(data=mod.cont.obj, aes(x=POSIXct, y=obs, color='observed'), lwd=0.4) +
              geom_vline(xintercept=as.numeric(startValidDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              geom_vline(xintercept=as.numeric(endValidDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              geom_vline(xintercept=as.numeric(startCalibDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              geom_vline(xintercept=as.numeric(endCalibDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
              ggtitle(paste0("Model Validation SWE Timeseries: ", siteId, "\n", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange', 'black'),
                                limits=c('default','calibrated','observed'),
                                label=c('default', 'calibrated', 'observed')) +
              labs(x="", y="SWE (kg m-2)") +
              theme_bw() + theme_bw(base_size = 20) + facet_wrap(~site_no, ncol = 1)

ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_timeseries_snow.png"),
              plot=gg, units="in", width=16, height=8, dpi=300)

# Scatterplots
maxval <- max(max(mod.cont.obj$mod, na.rm=TRUE), max(mod.valid.obj$mod, na.rm=TRUE), max(mod.cont.obj$obs, na.rm=TRUE))
gg1 <- ggplot() +
              geom_point(data=mod.cont.obj, aes(x=obs, y=mod, color='default'), shape=1, size=3) +
              geom_point(data=mod.valid.obj, aes(x=obs, y=mod, color='calibrated'), shape=1, size=3) +
              scale_shape_discrete(solid=FALSE) +
              geom_abline(intercept=0, slope=1, col='black', lty=1) +
              ggtitle(paste0("Full Period (", minDate, " to ", maxDate, "): \n", siteId, " ", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                                limits=c('default','calibrated'),
                                label=c('default', 'calibrated')) +
              labs(x="", y="modeled SWE (kg m-2)") +
              theme_bw() + theme(legend.position="none") + theme(axis.text=element_text(size=20), axis.title=element_text(size=20)) +
              xlim(0,maxval) + ylim(0,maxval) + facet_wrap(~site_no, ncol = 1)
gg2 <- ggplot() +
              geom_point(data=mod.cont.obj[POSIXct >= startCalibDate & POSIXct < endCalibDate,], aes(x=obs, y=mod, color='default'), shape=1, size=3) +
              geom_point(data=mod.valid.obj[POSIXct >= startCalibDate & POSIXct < endCalibDate,], aes(x=obs, y=mod, color='calibrated'), shape=1, size=3) +
              scale_shape_discrete(solid=FALSE) +
              geom_abline(intercept=0, slope=1, col='black', lty=1) +
              ggtitle(paste0("Calibration Period (", startCalibDate, " to ", endCalibDate, "): \n", siteId, " ", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                                limits=c('default','calibrated'),
                                label=c('default', 'calibrated')) +
              labs(x="observed SWE (kg m-2)", y="") +
              theme_bw() + theme(legend.position="none") + theme(axis.text=element_text(size=20), axis.title=element_text(size=20)) +
              xlim(0,maxval) + ylim(0,maxval)  + facet_wrap(~site_no, ncol = 1)


gg3 <- ggplot() +
              geom_point(data=mod.cont.obj[POSIXct >= startValidDate & POSIXct < endValidDate,], aes(x=obs, y=mod, color='default'), shape=1, size=3) +
              geom_point(data=mod.valid.obj[POSIXct >= startValidDate & POSIXct < endValidDate,], aes(x=obs, y=mod, color='calibrated'), shape=1, size=3) +
              scale_shape_discrete(solid=FALSE) +
              geom_abline(intercept=0, slope=1, col='black', lty=1) +
              ggtitle(paste0("Validation Period (", startValidDate, " to ", endValidDate, "): \n", siteId, " ", siteName)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                                limits=c('default','calibrated'),
                                label=c('default', 'calibrated')) +
              labs(x="", y="") +
              theme_bw() + theme(axis.text=element_text(size=20)) +
              theme(legend.position = c(0.2, 0.9),  legend.background = element_rect(colour = NA, fill = NA), legend.key.size = unit(1.25, "cm"),  legend.key = element_rect(colour = NA, fill = NA), legend.text = element_text(size=18)) +
              xlim(0,maxval) + ylim(0,maxval) + facet_wrap(~site_no, ncol = 1)

gg.all <- grid.arrange(gg1, gg2, gg3, ncol=3)
# gg.all <- grid.arrange(gg3,ncol=1) For the hyper res calirbation the gg1 and gg2 were commented out

ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_scatter_snow.png"),
              plot=gg.all, units="in", width=16, height=8, dpi=300)

# Stats Barplots
results.plot <- melt(validStats_snow[,c("run", "period", "obj", metrics_snow)], id=c("period", "run"))
results.plot$period <- factor(results.plot$period, levels=c("calib", "valid", "full"))
#results.plot$period <- factor(results.plot$period, levels=c("valid")) # The above line was commented out for the hyper res calibration and this one was used
results.plot$run <- factor(results.plot$run, levels=c("default", "calibrated"))
results.plot <- results.plot[order(results.plot$variable, results.plot$period, results.plot$run),]
results.plot$value <- as.numeric(results.plot$value)
gg <- ggplot(data=results.plot, aes(x=factor(period), y=value, fill=run)) +
         geom_bar(stat="identity", position="dodge") +
         facet_wrap(~variable, scales="free_y") +
         scale_fill_manual(name="", values=c('dodgerblue', 'orange'),
             limits=c('default','calibrated'),
             label=c('default', 'calibrated')) +
         ggtitle(paste0("Model Validation Performance Metrics: ", siteId, " ", siteName)) +
         labs(x="run period", y="value") +
         theme_bw() + theme_bw(base_size = 20)
ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_metrics_snow.png"),
        plot=gg, units="in", width=16, height=8, dpi=300)
}


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#
#                Soil Moisture
#
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if (enableSoilMoistureCalib == 1) {

  minDate        <- as.Date(minDate)
  maxDate        <- as.Date(maxDate)
  startCalibDate <- as.Date(startCalibDate)
  endCalibDate   <- as.Date(endCalibDate)
  startValidDate <- as.Date(startValidDate)
  endValidDate   <- as.Date(endValidDate)

  if (lsm_SPLIT_OUTPUT_COUNT == 1) {
    filesList <- list.files(path = outPathControl,
                           pattern = glob2rx("*.LDASOUT_DOMAIN*"),
                            full.names = TRUE)
    filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
    whFiles <- which(filesListDate >= minDate)
    filesList <- filesList[whFiles]
    if (length(filesList) == 0) stop("No matching files in specified directory.")
    mod.cont <- as.data.table(plyr::ldply(filesList, ReadSm_Multi, .parallel = parallelFlag, mskvar.lsm = mskvar.lsm))
    mod.cont$site_no <- siteId
 
    # let s calculate the moving average window of 7 or 15 days
    # since I am not sure how often we are going to output SM, let s first calculate the daily average of data before proceeding
    mod.cont$Date <- CalcDateTrunc(mod.cont$POSIXct)
    mod.cont <- mod.cont[, list(mod.d=mean(mod, na.rm=TRUE)), by = c("Date", "site_no")]

    # Merge
    setkey(mod.cont, "site_no", "Date")
    setkey(obsSoilData, "site_no", "Date")
    mod_soil.obj <- merge(mod.cont, obsSoilData, by=c("site_no", "Date"), all.x=TRUE, all.y=FALSE)

   
    # let s call the CDF matching function 
    mod.cont.obj.soil <- CalcSmCDF(mod_soil.obj, window_days)

    write(paste0("Reading model out files. Parallel ", parallelFlag, " ncores=", ncores), stdout())
   filesList <- list.files(path = outPathValid,
                           pattern = glob2rx("*.LDASOUT_DOMAIN*"),
                           full.names = TRUE)
    filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
    whFiles <- which(filesListDate >= minDate)
    filesList <- filesList[whFiles]
    if (length(filesList) == 0) stop("No matching files in specified directory.")
    mod.valid <- as.data.table(plyr::ldply(filesList, ReadSm_Multi, .parallel = parallelFlag, mskvar.lsm = mskvar.lsm))
    mod.valid$site_no <- siteId

    # let s calculate the moving average window of 7 or 15 days
    # since I am not sure how often we are going to output SM, let s first calculate the daily average of data before proceeding
    mod.valid$Date <- CalcDateTrunc(mod.valid$POSIXct)
    mod.valid <- mod.valid[, list(mod.d=mean(mod, na.rm=TRUE)), by = c("Date", "site_no")]

    # Merge
    setkey(mod.valid, "site_no", "Date")
    setkey(obsSoilData, "site_no", "Date")
    mod_soil.obj <- merge(mod.valid, obsSoilData, by=c("site_no", "Date"), all.x=TRUE, all.y=FALSE)
   
    # let s call the CDF matching function 
    mod.valid.obj.soil <- CalcSmCDF(mod_soil.obj, window_days)
  }
  
  # setup for stats loop
  runList <- list(df=c("mod.cont.obj.soil", "mod.valid.obj.soil"), run=c("default", "calibrated"))
  dtList <- list(start=c(startCalibDate, startValidDate, minDate),
                 end=c(endCalibDate, endValidDate, maxDate),
                 period=c("calib", "valid", "full"))
  # Initialize stats table
  validStats_soilmoisture <- as.data.frame(matrix(, nrow=1, ncol=length(metrics_soilmoisture)+3))
  names(validStats_soilmoisture) <- c("run", "period", "obj", metrics_soilmoisture)
  
  # Calculate stats
  loopcnt <- 1
  loopcnt2 <- 1
  for (i in 1:length(runList[[1]])) {
    for (j in 1:length(dtList[[1]])) {
      # Subset data
      mod.obj <- get(runList[["df"]][i])
      mod.obj <- mod.obj[Date >= dtList[["start"]][j] & Date < dtList[["end"]][j],]

         # Calc stats
         scales=c(1,24) # we are not doing daily stats 
         my_exprs = quote(list(
            cor = cor(mod, obs),
            rmse = Rmse(mod, obs, na.rm=TRUE),
            bias = PBias(mod, obs, na.rm=TRUE),
            nse = hydroGOF::NSE(mod, obs, na.rm=TRUE, FUN=NULL, epsilon="Pushpalatha2012"),
            kge = hydroGOF::KGE(mod, obs, na.rm=TRUE, method="2009", out.type="single"),
            kge_alpha = hydroGOF::KGE(mod, obs, na.rm=TRUE, method="2009", out.type="full")$KGE.elements['Alpha'],
         ))

      w = which(names(my_exprs) %in% metrics_soilmoisture)

      # let s just take care of objective function being capital
      objFn <- tolower(soilMoistureObjFunc)
      stat <- mod.obj[, eval(my_exprs[c(1,w)]), by = NULL]

      # Calc objective function
      if (objFn %in% c("nse","kge","cor")) F_new <- 1 - stat[, objFn, with = FALSE]
      
      # Archive results
      validStats_new <- cbind(data.table(run = runList[["run"]][i], period = dtList[["period"]][j], obj= F_new), stat[, c(metrics_soilmoisture), with = FALSE])
      validStats_soilmoisture[loopcnt,] <- validStats_new
      loopcnt <- loopcnt + 1
    }
  }
}

# ------------------------------------------------------
#  Soil Moisture PLOTS
#-------------------------------------------------------
if (enableSoilMoistureCalib) {
  
  # Time series
  gg <- ggplot() +
    geom_line(data=mod.cont.obj.soil, aes(x=Date, y=mod, color='default'), lwd=0.6) +
    geom_line(data=mod.valid.obj.soil, aes(x=Date, y=mod, color='calibrated'), lwd=0.6) +
    geom_line(data=mod.cont.obj.soil, aes(x=Date, y=obs, color='observed'), lwd=0.4) +
    geom_vline(xintercept=as.numeric(startValidDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
    geom_vline(xintercept=as.numeric(endValidDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
    geom_vline(xintercept=as.numeric(startCalibDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
    geom_vline(xintercept=as.numeric(endCalibDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +

    ggtitle(paste0("Model Validation Soil Moisture Timeseries: ", siteId, "\n", siteName)) +
    scale_color_manual(name="", values=c('dodgerblue', 'orange', 'black'),
                       limits=c('default','calibrated','observed'),
                       label=c('default', 'calibrated', 'observed')) +
    labs(x="", y="Soil Moisture") +
    theme_bw() + theme_bw(base_size = 20) + facet_wrap(~site_no, ncol = 1)
  
  ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_timeseries_soilmoisture.png"),
         plot=gg, units="in", width=16, height=8, dpi=300)
  
  # Scatterplots
  maxval <- max(max(mod.cont.obj.soil$mod, na.rm=TRUE), max(mod.valid.obj.soil$mod, na.rm=TRUE), max(mod.cont.obj.soil$obs, na.rm=TRUE))
  gg1 <- ggplot() +
    geom_point(data=mod.cont.obj.soil, aes(x=obs, y=mod, color='default'), shape=1, size=3) +
    geom_point(data=mod.valid.obj.soil, aes(x=obs, y=mod, color='calibrated'), shape=1, size=3) +
    scale_shape_discrete(solid=FALSE) +
    geom_abline(intercept=0, slope=1, col='black', lty=1) +
    ggtitle(paste0("Full Period (", minDate, " to ", maxDate, "): \n", siteId, " ", siteName)) +
    scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                       limits=c('default','calibrated'),
                       label=c('default', 'calibrated')) +
    labs(x="", y="Modeled Soil Moisture") +
    theme_bw() + theme(legend.position="none") + theme(axis.text=element_text(size=20), axis.title=element_text(size=20)) +
    xlim(0,maxval) + ylim(0,maxval) + facet_wrap(~site_no, ncol = 1)
  gg2 <- ggplot() +
    geom_point(data=mod.cont.obj.soil[Date>= startCalibDate & Date < endCalibDate,], aes(x=obs, y=mod, color='default'), shape=1, size=3) +
    geom_point(data=mod.valid.obj.soil[Date >= startCalibDate & Date < endCalibDate,], aes(x=obs, y=mod, color='calibrated'), shape=1, size=3) +
    scale_shape_discrete(solid=FALSE) +
    geom_abline(intercept=0, slope=1, col='black', lty=1) +
    ggtitle(paste0("Calibration Period (", startCalibDate, " to ", endCalibDate, "): \n", siteId, " ", siteName)) +
    scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                       limits=c('default','calibrated'),
                       label=c('default', 'calibrated')) +
    labs(x="Observed Soil Moisture Anomaly", y="") +
    theme_bw() + theme(legend.position="none") + theme(axis.text=element_text(size=20), axis.title=element_text(size=20)) +
    xlim(0,maxval) + ylim(0,maxval)  + facet_wrap(~site_no, ncol = 1)
  
  gg3 <- ggplot() +
    geom_point(data=mod.cont.obj.soil[Date>= startValidDate & Date < endValidDate,], aes(x=obs,y= mod,color='default'), shape=1, size=3) +
    geom_point(data=mod.valid.obj.soil[Date >= startValidDate & Date < endValidDate,], aes(x=obs, y=mod, color='calibrated'), shape=1, size=3) +
    scale_shape_discrete(solid=FALSE) +
    geom_abline(intercept=0, slope=1, col='black', lty=1) +
    ggtitle(paste0("Validation Period (", startValidDate, " to ", endValidDate, "): \n", siteId, " ", siteName)) +
    scale_color_manual(name="", values=c('dodgerblue', 'orange'),
                       limits=c('default','calibrated'),
                       label=c('default', 'calibrated')) +
    labs(x="", y="") +
    theme_bw() + theme(axis.text=element_text(size=20)) +
    theme(legend.position = c(0.2, 0.9),  legend.background = element_rect(colour = NA, fill = NA), legend.key.size = unit(1.25, "cm"),  legend.key = element_rect(colour = NA, fill = NA), legend.text = element_text(size=18)) +
    xlim(0,maxval) + ylim(0,maxval) + facet_wrap(~site_no, ncol = 1)
  
  gg.all <- grid.arrange(gg1, gg2, gg3, ncol=3)
  # gg.all <- grid.arrange(gg3,ncol=1) For the hyper res calirbation the gg1 and gg2 were commented out
  
  ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_scatter_soilmoisture.png"),
         plot=gg.all, units="in", width=16, height=8, dpi=300)
  
  # Stats Barplots
  results.plot <- melt(validStats_soilmoisture[,c("run", "period", "obj", metrics_soilmoisture)], id=c("period", "run"))
  results.plot$period <- factor(results.plot$period, levels=c("calib", "valid", "full"))
  results.plot$run <- factor(results.plot$run, levels=c("default", "calibrated"))
  results.plot <- results.plot[order(results.plot$variable, results.plot$period, results.plot$run),]
  results.plot$value <- as.numeric(results.plot$value)
  gg <- ggplot(data=results.plot, aes(x=factor(period), y=value, fill=run)) +
    geom_bar(stat="identity", position="dodge") +
    facet_wrap(~variable, scales="free_y") +
    scale_fill_manual(name="", values=c('dodgerblue', 'orange'),
                      limits=c('default','calibrated'),
                      label=c('default', 'calibrated')) +
    ggtitle(paste0("Model Validation Performance Metrics: ", siteId, " ", siteName)) +
    labs(x="run period", y="value") +
    theme_bw() + theme_bw(base_size = 20)
  ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_metrics_soilmoisture.png"),
         plot=gg, units="in", width=16, height=8, dpi=300)
  
}


#########################################################
# SAVE & EXIT
#########################################################

# Save and exit
#rm(objFn, mCurrent, r, siteId, rtlinkFile, linkId, startDate, ncores)
save.image(paste0(validDir, "/proj_data_VALID.Rdata"))

# Write param files
#if (enableStreamflowCalib) { # Xia commented out
#    write.table(validStats, file=paste0(validDir, "/valid_stats.txt"), row.names=FALSE, sep=" ")
#   } else if (enableSnowCalib) {
#    write.table(validStats_snow, file=paste0(validDir, "/valid_stats.txt"), row.names=FALSE, sep=" ")
#   } else if (enableSoilMoistureCalib) {
#     write.table(validStats_soilmoisture, file=paste0(validDir, "/valid_stats.txt"), row.names=FALSE, sep=" ")
#}
if (enableStreamflowCalib == 1) { # Xia 20210610
  if (enableSnowCalib == 0) validMetrics_snow <- data.frame(matrix(-9999, nrow = nrow(validStats), ncol = length(metrics_snow)+1))
  if (enableSnowCalib == 1) validMetrics_snow <- validStats_snow[,c("obj", metrics_snow)] 
    colnames(validMetrics_snow) <- paste0(c("obj", metrics_snow), "_snow")
    validMetrics <- cbind(validStats, validMetrics_snow)
}
if (enableStreamflowCalib == 0 & enableSnowCalib == 1) {
  validMetrics_streamflow <- data.frame(matrix(-9999, nrow = nrow(validStats_snow), ncol = length(metrics_streamflow)+1))
  colnames(validMetrics_streamflow) <- paste0(c("obj", metrics_streamflow))
  colnames(validStats_snow)[3:ncol(validStats_snow)] <- paste0(c("obj", metrics_snow), "_snow")
  validMetrics <- cbind(validStats_snow[,c("run","period")], validMetrics_streamflow, validStats_snow[,paste0(c("obj", metrics_snow), "_snow")])
}
validMetrics[is.na(validMetrics)]<--9999
write.table(validMetrics, file=paste0(validDir, "/valid_stats.txt"), row.names=FALSE, sep=" ")
#write.table(validStats, file=paste0(validDir, "/valid_stats.txt"), row.names=FALSE, sep=" ")

fileConn <- file(paste0(validDir, "/R_VALID_COMPLETE"))
writeLines('', fileConn)
close(fileConn)

write(summary(proc.time()), stdout())

quit("no")

