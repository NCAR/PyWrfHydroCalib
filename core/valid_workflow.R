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
gageIndx <- gageIndx
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
   system.time({
    filesList <- list.files(path = outPathControl,
                        pattern = glob2rx("*.CHANOBS_DOMAIN*"),
                        full.names = TRUE)
    filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
    whFiles <- which(filesListDate >= minDate)
    filesList <- filesList[whFiles]
    if (length(filesList) == 0) stop("No matching files in specified directory.")
    chrt.cont <- as.data.table(plyr::ldply(filesList, ReadChFile, gageIndx, .parallel = parallelFlag))
   })

} else if (hydro_SPLIT_OUTPUT_COUNT == 0) {

   write(paste0("Reading control run model out file : CHANOBS_DOMAIN1.nc"), stdout())
   system.time({
   chanobsFile <- list.files(outPathControl, pattern = glob2rx("CHANOBS_DOMAIN1.nc"), full.names = TRUE)
   q_cms = ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "streamflow")
   if (length(dim(q_cms)) != 1) {
      rotate <- function(x) t(apply(x, 2, rev))
      q_cms <- rotate(q_cms) # R totate the matrix when it is reading it oin.
      q_cms <- q_cms[, gageIndx]
  }

   POSIXct<-as.POSIXct(ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "time")*60,
                       origin = "1970-01-01 00:00:00 UTC", tz = "UTC") # because the time is minutes from this origin
   chrt.cont <- data.frame(POSIXct,q_cms)
   chrt.cont <-as.data.table(chrt.cont)
  })

   # If the model crashes, then it would be append to the file after restarting the model,
   # therefore, we need to remove the duplicates in order to not double count for those.
   chrt.cont <- unique(chrt.cont)

   # remove the spin up part
   chrt.cont <- chrt.cont[POSIXct > minDate, ]
}


if (hydro_SPLIT_OUTPUT_COUNT == 1) {
   write(paste0("Reading validation run model out files. Parallel ", parallelFlag, " ncores=", ncores), stdout())
   system.time({
     filesList <- list.files(path = outPathValid,
                        pattern = glob2rx("*.CHANOBS_DOMAIN*"),
                        full.names = TRUE)
     filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
     whFiles <- which(filesListDate >= minDate)
     filesList <- filesList[whFiles]
     if (length(filesList) == 0) stop("No matching files in specified directory.")
     chrt.valid <- as.data.table(plyr::ldply(filesList, ReadChFile, gageIndx, .parallel = parallelFlag))
   })

} else if (hydro_SPLIT_OUTPUT_COUNT == 0) {

   write(paste0("Reading validation run model out file : CHANOBS_DOMAIN1.nc"), stdout())
   chanobsFile <- list.files(outPathValid, pattern = glob2rx("CHANOBS_DOMAIN1.nc"), full.names = TRUE)
   q_cms = ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "streamflow")
   if (length(dim(q_cms)) != 1) {
      rotate <- function(x) t(apply(x, 2, rev))
      q_cms <- rotate(q_cms) # R totate the matrix when it is reading it oin.
      q_cms <- q_cms[, gageIndx]
  }

   POSIXct<-as.POSIXct(ncdf4::ncvar_get(ncdf4::nc_open(chanobsFile), varid = "time")*60,
                       origin = "1970-01-01 00:00:00 UTC", tz = "UTC") # because the time is minutes from this origin

   chrt.valid <- data.frame(POSIXct,q_cms)
   chrt.valid <-as.data.table(chrt.valid)

   # If the model crashes, then it would be append to the file after restarting the model,
   # therefore, we need to remove the duplicates in order to not double count for those.
   chrt.valid <- unique(chrt.valid)

   # remove the spin up part
   chrt.valid <- chrt.valid[POSIXct > minDate, ]
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
  chrt.cont.d[, site_no := siteId]
  chrt.valid.d <- Convert2Daily(chrt.valid)
  chrt.valid.d[, site_no := siteId]
  chrt.cont.obj <- copy(chrt.cont.d)
  chrt.valid.obj <- copy(chrt.valid.d)
  obs.obj <- Convert2Daily(obsStrData)
  obs.obj[, site_no := siteId]
} else {
  chrt.cont[, site_no := siteId]
  chrt.valid[, site_no := siteId]
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

# Calculate stats
loopcnt <- 1
for (i in 1:length(runList[[1]])) {
   for (j in 1:length(dtList[[1]])) {
      # Subset data
      chrt.obj <- get(runList[["df"]][i])
      chrt.obj <- chrt.obj[POSIXct >= dtList[["start"]][j] & POSIXct < dtList[["end"]][j],]
      chrt.obj <- chrt.obj[!is.na(q_cms) & !is.na(obs),]

      # Calc objective function
      F_new <- objFunc(chrt.obj$q_cms, chrt.obj$obs)
      if (objFn %in% c("Nse", "NseLog", "NseWt", "Kge")) F_new <- 1 - F_new

      # Calc stats
      statCor <- cor(chrt.obj$q_cms, chrt.obj$obs)
      statRmse <- Rmse(chrt.obj$q_cms, chrt.obj$obs, na.rm=TRUE)
      statBias <- PBias(chrt.obj$q_cms, chrt.obj$obs, na.rm=TRUE)
      statNse <- hydroGOF::NSE(chrt.obj$q_cms, chrt.obj$obs, na.rm=TRUE, FUN=NULL, epsilon="Pushpalatha2012")
      statNseLog <- hydroGOF::NSE(chrt.obj$q_cms, chrt.obj$obs, na.rm=TRUE, FUN=log, epsilon="Pushpalatha2012")
      statNseWt <- NseWt(chrt.obj$q_cms, chrt.obj$obs)
      statKge <- hydroGOF::KGE(chrt.obj$q_cms, chrt.obj$obs, na.rm=TRUE, method="2012", out.type="single")
      statMsof <- Msof(chrt.obj$q_cms, chrt.obj$obs)
      statHyperResMultiObj <- hyperResMultiObj(chrt.obj$q_cms, chrt.obj$obs, na.rm=TRUE)

      # Archive results
      #validStats_new <- list(runList[["run"]][i], dtList[["period"]][j], F_new, statCor, statRmse, statBias, statNse, statNseLog, statNseWt, statKge, statMsof)
      validStats_new <- list(runList[["run"]][i], dtList[["period"]][j], F_new, statCor, statRmse, statBias, statNse, statNseLog, statNseWt, statKge, statMsof, statHyperResMultiObj)
      validStats[loopcnt,] <- validStats_new
      loopcnt <- loopcnt + 1
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
              ggtitle(paste0("Model Validation Hydrograph: ", siteId)) +
              scale_color_manual(name="", values=c('dodgerblue', 'orange', 'black'),
                                limits=c('default','calibrated','observed'),
                                label=c('default', 'calibrated', 'observed')) +
              labs(x="", y="Streamflow (m3/s)") +
              theme_bw() + theme_bw(base_size = 20)

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
              xlim(0,maxval) + ylim(0,maxval)
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
              xlim(0,maxval) + ylim(0,maxval) 

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
              xlim(0,maxval) + ylim(0,maxval)
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




