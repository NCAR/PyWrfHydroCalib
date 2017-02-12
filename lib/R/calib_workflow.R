#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly=TRUE)
namelistFile <- args[1]
#mCurrent <- args[2]

.libPaths("/glade/u/home/adugger/system/R/Libraries/R3.2.2")
library(rwrfhydro)
library(data.table)
library(ggplot2)
library(plyr)

#########################################################
# SETUP
#########################################################

source("calib_utils.R")
source(namelistFile)
objFunc <- get(objFn)

# Metrics
metrics <- c("cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof")

#########################################################
# MAIN CODE
#########################################################

# First loop check
if (file.exists(paste0(runDir, "/proj_data.Rdata"))) { 
   load(paste0(runDir, "/proj_data.Rdata"))
} else {
   # First run so need to initialize
   #ReadNamelist(paste0(runDir, "/calibScript.R"))
   cyclecount <- 0

   # Read parameter bounds 
   paramBnds <- read.table(paste0(runDir, "/calib_parms.tbl"), header=TRUE, sep=",", stringsAsFactors=FALSE)
   paramBnds <- subset(paramBnds, paramBnds$calib_flag==1)

   # Setup plot directory
   writePlotDir <- paste0(runDir, "/plots")
   dir.create(writePlotDir)

   # Load obs so we have them for next iteration
   load(paste0(runDir, "/OBS/obsStrData.Rdata"))
   if ("q_cms" %in% names(obsStrData)) obsStrData$q_cms <- NULL

   # Find the index of the gage
   rtLink <- ReadRouteLink(rtlinkFile)
   gageIndx <- which(rtLink$link == linkId)
   rm(rtLink)

   # Setup value lists from paramBnds
   xnames <- paramBnds$parameter
   x0 <- paramBnds$ini
   names(x0) <- xnames
   x_min <- paramBnds$minValue
   names(x_min) <- xnames
   x_max <- paramBnds$maxValue
   names(x_max) <- xnames

   # Initialize parameter archive DF
   message("Initialize parameter archive")
   x_archive <- as.data.frame(matrix(, nrow=1, ncol=length(xnames)+2+length(metrics)))
   names(x_archive) <- c("iter", xnames, "obj", metrics)

   # Output parameter set
   x_new <- x0
   cyclecount <- 1

   x_new_out <- c(cyclecount, x_new)
   names(x_new_out)[1] <- "iter"
   write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")

   # Save and exit
   #rm(mCurrent)
   save.image(paste0(runDir, "/proj_data.Rdata"))

   #system(paste0("touch ", runDir, "/R_COMPLETE"))
   fileConn <- file(paste0(runDir, "/R_COMPLETE"))
   writeLines('', fileConn)
   close(fileConn)

   quit("no")
}

if (cyclecount > 0) {

 if (mCurrent < cyclecount) {
   # Extra check for python workflow. If the counts get off due to a crash, just spit out previous params_new and params_stats.
   message(paste0("Cycle counts off so repeating last export. mCurrent=", mCurrent, " cyclecount=", cyclecount))
   if (exists("paramStats")) write.table(paramStats, file=paste0(runDir, "/params_stats.txt"), row.names=FALSE, sep=" ")
   write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")

   fileConn <- file(paste0(runDir, "/R_COMPLETE"))
   writeLines('', fileConn)
   close(fileConn)

   quit("no")

 } else {

   # Setup parallel
   if (ncores>1) {
        parallelFlag <- TRUE
        library(doParallel)
        cl <- makeForkCluster(ncores)
        registerDoParallel(cl)
   } else {
        parallelFlag <- FALSE
   }

   # Read model out and calculate performance metric
   outPath <- paste0(runDir, "/OUTPUT")
   print(outPath)

   # Read files
   message("Reading model out files.")
   system.time({
   filesList <- list.files(path = outPath,
                          pattern = glob2rx("*.CHRTOUT_DOMAIN*"),
                          full.names = TRUE)
   filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
   whFiles <- which(filesListDate >= startDate)
   filesList <- filesList[whFiles]
   if (length(filesList) == 0) stop("No matching files in specified directory.")
   chrt <- as.data.table(plyr::ldply(filesList, ReadChFile, gageIndx, .parallel = parallelFlag))
   })

   # Convert to daily
   chrt.d <- Convert2Daily(chrt)
   chrt.d[, site_no := siteId]
   assign(paste0("chrt.d.", cyclecount), chrt.d)

   # Merge
   setkey(chrt.d, "site_no", "POSIXct")
   setkey(obsStrData, "site_no", "POSIXct")
   chrt.d <- merge(chrt.d, obsStrData, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)

   # Calc objective function
   F_new <- objFunc(chrt.d$q_cms, chrt.d$obs)
   if (objFn %in% c("Nse", "NseLog", "NseWt", "Kge")) F_new <- 1 - F_new

   # Calc stats
   statCor <- cor(chrt.d$q_cms, chrt.d$obs)
   statRmse <- Rmse(chrt.d$q_cms, chrt.d$obs, na.rm=TRUE)
   statBias <- PBias(chrt.d$q_cms, chrt.d$obs, na.rm=TRUE)
   statNse <- Nse(chrt.d$q_cms, chrt.d$obs, na.rm=TRUE)
   statNseLog <- NseLog(chrt.d$q_cms, chrt.d$obs, na.rm=TRUE)
   statNseWt <- NseWt(chrt.d$q_cms, chrt.d$obs)
   statKge <- Kge(chrt.d$q_cms, chrt.d$obs, na.rm=TRUE)
   statMsof <- Msof(chrt.d$q_cms, chrt.d$obs)

   # Archive results
   x_archive[cyclecount,] <- c(cyclecount, x_new, F_new, statCor, statRmse, statBias, statNse, statNseLog, statNseWt, statKge, statMsof)

   # Evaluate objective function
   if (cyclecount == 1) {
      x_best <- x_new
      F_best <- F_new
      iter_best <- cyclecount
      bestFlag <- 1
   } else if (F_new <= F_best) {
      x_best <- x_new
      F_best <- F_new
      iter_best <- cyclecount
      bestFlag <- 1
   } else {
      bestFlag <- 0
   }

   # Add best flag and output
   paramStats <- cbind(x_archive[cyclecount,c("iter", "obj", metrics)], data.frame(best=bestFlag))
   write.table(paramStats, file=paste0(runDir, "/params_stats.txt"), row.names=FALSE, sep=" ")

   if (cyclecount < m) {
      # Select next parameter set
      x_new <- DDS.sel(i=cyclecount, m=m, r=r, xnames=xnames, x_min=x_min, x_max=x_max, x_best=x_best)
      cyclecount <- cyclecount+1  

      # Output next parameter set
      x_new_out <- c(cyclecount, x_new)
      names(x_new_out)[1] <- "iter"
      write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")
   }

   # Stop cluster
   if (parallelFlag) stopCluster(cl)


#########################################################
# PLOTS
#########################################################

   # Update basic objective function plot
   message("Basin objective function plot...")
   gg <- ggplot(data=x_archive, aes(x=iter, y=obj)) + 
              geom_point() + theme_bw() + 
              labs(x="run", y="objective function")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_calib_run_obj.png"),
              plot=gg, units="in", width=6, height=5, dpi=300)

   # Update the Objective function versus the parameter variable
   message("Obj function vs. params...")
   DT.m1 = melt(x_archive[, 1:length(x_archive)], id.vars = c("obj"), measure.vars =names(x_archive)[2:(length(x_archive))])
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(value, obj))
   gg <- gg + ggplot2::geom_point(size = 1, color = "red", alpha = 0.3)+facet_wrap(~variable, scales="free_x")
   gg <- gg + ggplot2::ggtitle(paste0("Scatter Plot of Obj. function versus parameters: ", siteId))
   gg <- gg + ggplot2::xlab("Parameter Values")+theme_bw()+ggplot2::ylab("Objective Function")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_obj_vs_parameters_calib_run.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)

   # Plot the variables as a function of calibration runs
   message("Params over runs...")
   DT.m1 = melt(x_archive, id.vars = c("iter"), measure.vars =names(x_archive)[2:length(x_archive)])
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(iter, value))
   gg <- gg + ggplot2::geom_point(size = 1, color = "red", alpha = 0.3)+facet_wrap(~variable, scales="free")
   gg <- gg + ggplot2::ggtitle(paste0("Parameter change with iteration: ", siteId))
   gg <- gg + ggplot2::xlab("Calibration Iteration")+theme_bw()
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_parameters_calib_run.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)

   # Plot all the stats
   message("Metrics plot...")
   DT.m1 = melt(x_archive[,which(names(x_archive) %in% c("iter", "obj", "cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof"))],
               iter.vars = c("iter"), measure.vars = c("obj", "cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof"))
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(iter, value))
   gg <- gg + ggplot2::geom_point(size = 1, color = "red", alpha = 0.3)+facet_wrap(~variable, scales="free")
   gg <- gg + ggplot2::ggtitle(paste0("Metric Sensitivity: ", siteId))
   gg <- gg + ggplot2::xlab("Calibration Iteration No.")+theme_bw()+ylab("Value")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_metric_calib_run.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)

   # Plot the time series of the observed, control, best calibration result and last calibration iteration 
   message("Hydrograph...")
   # The first iteration is the control run  called chrt.d.1
   controlRun <- chrt.d.1
   controlRun [, run := "Control Run"]
   # We have already advanced the cyclescount, so subtract 1 to get last complete
   lastRun <- get(paste0("chrt.d.", cyclecount-1))
   lastRun [ , run := "Last Run"]
   # the best iteration should be find
   bestRun <- get(paste0("chrt.d.", iter_best))
   bestRun [ , run := "Best Run"]

   obsStrDataPlot <- copy(obsStrData)
   setnames(obsStrDataPlot, "obs", "q_cms")
   obsStrDataPlot <- obsStrDataPlot[, c("Date", "q_cms", "POSIXct", "site_no"), with=FALSE]
   obsStrDataPlot <- obsStrDataPlot[as.integer(POSIXct) >= min(as.integer(controlRun$POSIXct)) & as.integer(POSIXct) <= max(as.integer(controlRun$POSIXct)),]
   obsStrDataPlot[ , run := "Observation"]

   chrt.d_plot <- rbindlist(list(controlRun, lastRun, bestRun, obsStrDataPlot), use.names = TRUE, fill=TRUE)

   gg <- ggplot2::ggplot(chrt.d_plot, ggplot2::aes(POSIXct, q_cms, color = run))
   gg <- gg + ggplot2::geom_line(size = 0.2, alpha = 0.7)
   gg <- gg + ggplot2::ggtitle(paste0("Streamflow time series for ", siteId))
   #gg <- gg + scale_x_datetime(limits = c(as.POSIXct("2008-10-01"), as.POSIXct("2013-10-01")))
   gg <- gg + ggplot2::xlab("Date")+theme_bw( base_size = 15) + ylab ("Streamflow (cms)")
   gg <- gg + scale_color_manual(name="", values=c('black', 'dodgerblue', 'orange' , "red"),
                                 limits=c('Observation','Control Run', "Best Run", "Last Run"),
                                  label=c('Observation','Control Run', "Best Run", "Last Run"))

   ggsave(filename=paste0(writePlotDir, "/", siteId, "_hydrograph.png"),
           plot=gg, units="in", width=8, height=4, dpi=300)

   # Plot the scatter plot of the best, last and control run.
   message("Scatterplot...")
   maxval <- max(chrt.d_plot$q_cms, rm.na = TRUE)
   gg <- ggplot()+ geom_point(data = merge(chrt.d_plot [run %in% c("Control Run", "Last Run", "Best Run")], obsStrData, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE),
                              aes (obs, q_cms, color = run), alpha = 0.5)
   gg <- gg + scale_color_manual(name="", values=c('dodgerblue', 'orange' , "red"),
                                 limits=c('Control Run', "Best Run", "Last Run"),
                                 label=c('Control Run', "Best Run", "Last Run"))
   gg <- gg + ggtitle(paste0("Simulated vs observed flow : ", siteId )) + theme_bw( base_size = 15)
   gg <- gg + geom_abline(intercept = 0, slope = 1) + coord_equal()+ xlim(0,maxval) + ylim(0,maxval)
   gg <- gg + xlab("Observed flow (cms)") + ylab ("Simulated flow (cms)")

   ggsave(filename=paste0(writePlotDir, "/", siteId, "_scatter.png"),
           plot=gg, units="in", width=8, height=8, dpi=300)


#########################################################
# SAVE & EXIT
#########################################################

   # Save and exit
   #rm(mCurrent)
   save.image(paste0(runDir, "/proj_data.Rdata"))

   #system(paste0("touch ", runDir, "/R_COMPLETE"))
   fileConn <- file(paste0(runDir, "/R_COMPLETE"))
   writeLines('', fileConn)
   close(fileConn)

   quit("no")

 }

}



