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

#########################################################
# SETUP
#########################################################

source("calib_utils.R")
source(namelistFile)
# passed in from namelist: runDir, validDir, objFn, siteId, linkId, ncores, startCalibDate, endCalibDate, startValidDate, endValidDate

attach(paste0(runDir, "/proj_data.Rdata"), name="calibdb")
#gageIndx <- gageIndx
metrics <- metrics
obsStrData <- obsStrData
detach(calibdb)

objFunc <- get(objFn)

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
# Stop cluster
if (parallelFlag) stopCluster(cl)

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
  obs.obj <- Convert2Daily(obsStrData)
} else {
  chrt.cont.obj <- copy(chrt.cont)
  chrt.valid.obj <- copy(chrt.valid)
  obs.obj <- copy(obsStrData)
}

# Merge
setkey(chrt.cont.obj, "site_no", "POSIXct")
if ("Date" %in% names(obs.obj)) obs.obj[, Date := NULL]
setkey(obs.obj, "site_no", "POSIXct")
chrt.cont.obj <- merge(chrt.cont.obj, obs.obj, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)

setkey(chrt.valid.obj, "site_no", "POSIXct")
chrt.valid.obj <- merge(chrt.valid.obj, obs.obj, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)

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
        nselog = hydroGOF::NSE(q_cms, obs, na.rm=TRUE, FUN=log, epsilon="Pushpalatha2012"),
        nsewt = NseWt(q_cms, obs) ,
        kge = hydroGOF::KGE(q_cms, obs, na.rm=TRUE, method="2012", out.type="single"),
        hyperResMultiObj = hyperResMultiObj(q_cms, obs, na.rm=TRUE),
        msof = Msof(q_cms, obs, scales)
      ))
      my_exprs2 = quote(list(
        corr1 = r1(q_cms, obs), # Calculate Stedingers r1
        lbem = LBEms_function(q_cms, obs, period)[1],
        lbemprime =  LBEms_function(q_cms, obs, period)[2]
      ))
      w = which(names(my_exprs) %in% metrics)
      w2 = which(names(my_exprs2) %in% metrics)

      # let s just take care of objective function being capital
      objFn <- tolower(objFn)

      if (enableMultiSites == 0) {
        stat <- chrt.obj.nona[, eval(my_exprs[c(1,w)]), by = NULL]
        if (length(w2) > 0) stat <- cbind(stat, chrt.obj.nona.nozeros[, eval(my_exprs2[c(1,w2)]), by = NULL])

        if (any(c("POD", "FAR", "CSI") %in% metrics)) {
           stat$POD = calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$POD
           stat$FAR = calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$FAR
           stat$CSI = calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "threshName"))$CSI
        }

        # Calc objective function
        if (objFn %in% c("nsewt","nse","nselog","kge","cor","corr1", "lbem","lbemprime")) F_new <- 1 - stat[, objFn, with = FALSE]
        if (objFn %in% c("rmse","msof","hyperResMultiObj")) F_new <- stat[, objFn, with = FALSE]

        # Archive results
        validStats_new <- cbind(data.table(run = runList[["run"]][i], period = dtList[["period"]][j], obj= F_new), stat[, c(metrics), with = FALSE])
        validStats[loopcnt,] <- validStats_new
        loopcnt <- loopcnt + 1

     } else { #enableMultiSites = 1

        stat <- chrt.obj.nona[, eval(my_exprs[c(1,w)]), by = c("site_no", "weight")]
        if (length(w2) > 0){
            stat <- merge(stat, chrt.obj.nona.nozeros[, eval(my_exprs2[c(1,w2)]), by = c("site_no", "weight")],by = c("site_no", "weight"))
        }
        if (any(c("POD", "FAR", "CSI") %in% metrics)) {
             stat <- merge(stat, calc_contingency_stats(chrt.obj.nona.abcd1, groupVars = c("site_no", "weight", "threshName")),
                           by = c("site_no", "weight"))
        }

        statW <- c() # calculate the weighted metrics
        for (im in metrics){
          statW <- c(statW, stat[, c(im, "weight"), with = FALSE][, .(fn = sum(weight*get(im)))]$fn)
        }
        names(statW) <- metrics

        if (objFn %in% c("nsewt","nse","nselog","kge","cor","corr1", "lbem","lbemprime")) F_new <- 1 - statW[objFn]
        if (objFn %in% c("rmse","msof","hyperResMultiObj")) F_new <- statW[objFn]

      # Archive results
      validStats[loopcnt,] <- c(runList[["run"]][i], dtList[["period"]][j],  F_new, statW)

      index1 = (loopcnt - 1) *nrow(stat)
      for(k in 1:nrow(stat)) {
        validStats_2[index1+k,] <- cbind(data.table(run = runList[["run"]][i], period = dtList[["period"]][j], obj= F_new), stat[, c(metrics), with = FALSE], stat$site_no[k])
      }
      loopcnt = loopcnt  + 1
   }
 }
}

rm(chrt.obj, validStats_new)


#########################################################
# PLOTS
#########################################################

writePlotDir <- paste0(validDir, "/plots")
dir.create(writePlotDir)

# Hydrographs
gg <- ggplot() + 
              geom_line(data=chrt.cont.obj, aes(x=POSIXct, y=q_cms, color='default'), lwd=0.6) +
              geom_line(data=chrt.valid.obj, aes(x=POSIXct, y=q_cms, color='calibrated'), lwd=0.6) +
              geom_line(data=chrt.cont.obj, aes(x=POSIXct, y=obs, color='observed'), lwd=0.4) +
              geom_vline(xintercept=as.numeric(startValidDate), lwd=1.8, col=alpha('grey70', 0.7), lty=2) +
#             ggtitle(paste0("Model Validation Hydrograph: ", siteId)) +
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
              ggtitle(paste0("Full Period (", minDate, " to ", maxDate, "): \n", siteId)) +
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
              ggtitle(paste0("Calibration Period (", startCalibDate, " to ", endCalibDate, "): \n", siteId)) +
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
              ggtitle(paste0("Validation Period (", startValidDate, " to ", endValidDate, "): \n", siteId)) +
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
         ggtitle(paste0("Model Validation Performance Metrics: ", siteId)) +
         labs(x="run period", y="value") +
         theme_bw() + theme_bw(base_size = 20)
ggsave(filename=paste0(writePlotDir, "/", siteId, "_valid_metrics.png"),
        plot=gg, units="in", width=16, height=8, dpi=300)

#########################################################
# SAVE & EXIT
#########################################################

# Save and exit
#rm(objFn, mCurrent, r, siteId, rtlinkFile, linkId, startDate, ncores)
save.image(paste0(validDir, "/proj_data_VALID.Rdata"))

# Write param files
write.table(validStats, file=paste0(validDir, "/valid_stats.txt"), row.names=FALSE, sep=" ")

fileConn <- file(paste0(validDir, "/R_VALID_COMPLETE"))
writeLines('', fileConn)
close(fileConn)

write(summary(proc.time()), stdout())

quit("no")




