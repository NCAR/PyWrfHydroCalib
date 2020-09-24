#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly=TRUE)
namelistFile <- args[1]
#mCurrent <- args[2]

#.libPaths("/glade/u/home/adugger/system/R/Libraries/R3.2.2")
#library(rwrfhydro)
library(data.table)
library(ggplot2)
library(ncdf4)
library(plyr)
library(hydroGOF)
#########################################################
# SETUP
#########################################################

source("calib_utils.R")
source(namelistFile)
objFunc <- get(objFn)

# Metrics
#metrics <- c("cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof")
metrics <- c("cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof", "hyperResMultiObj", "POD", "FAR", "CSI", "corr1", "lbem", "lbemprime")
#metrics_DB <- c("cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof", "hyperResMultiObj")

#########################################################
# MAIN CODE
#########################################################

# First loop check
if (file.exists(paste0(runDir, "/proj_data.Rdata"))) { 
   # If the run directories have changed for any reason, over-write them in the
   # R Dataset file. This is for when a user may take over a job, and move
   # the data.  
   writePlotDirCheck3 <- paste0(runDir, "/plots")
   outPathCheck3 <- paste0(runDir, "/OUTPUT")
   runDirCheck3 <- runDir

   load(paste0(runDir, "/proj_data.Rdata"))

   if (writePlotDir != writePlotDirCheck3){
      writePlotDir <- writePlotDirCheck3
      outPath <- outPathCheck
      runDir <- runDirCheck
      rm(writePlotDirCheck3,outPathCheck)
   }  
} else {
   # First run so need to initialize
   #ReadNamelist(paste0(runDir, "/calibScript.R"))
   cyclecount <- 0
   lastcycle <- FALSE

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
   #rtLink <- ReadRouteLink(rtlinkFile)
   #gageIndx <- which(rtLink$link == linkId)
   #rm(rtLink)

   # Setup value lists from paramBnds
   xnames <- paramBnds$parameter
   x0 <- paramBnds$ini
   names(x0) <- xnames
   x_min <- paramBnds$minValue
   names(x_min) <- xnames
   x_max <- paramBnds$maxValue
   names(x_max) <- xnames

   # Initialize parameter archive DF
   write("Initialize parameter archive", stdout())
   x_archive <- as.data.frame(matrix(, nrow=1, ncol=length(xnames)+2+length(metrics)))
   names(x_archive) <- c("iter", xnames, "obj", metrics)

   if (enableMultiSites == 1) {
        x_archive_2 <- as.data.frame(matrix(, nrow=1, ncol=length(xnames)+2+length(metrics)+1))
        names(x_archive_2) <- c("iter", xnames, "obj", metrics, "site_no")
   }
   # Output parameter set
   x_new <- x0
   cyclecount <- 1

   x_new_out <- c(cyclecount, x_new)
   names(x_new_out)[1] <- "iter"
   # MOVE TO END: write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")

   # Save and exit
   rm(objFn, mCurrent, r, siteId, rtlinkFile, linkId, startDate, ncores)
   save.image(paste0(runDir, "/proj_data.Rdata"))
   
   # Write param files
   write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")

   #system(paste0("touch ", runDir, "/R_COMPLETE"))
   fileConn <- file(paste0(runDir, "/R_COMPLETE"))
   writeLines('', fileConn)
   close(fileConn)

   quit("no")
}

if (cyclecount > 0) {

 if (mCurrent < cyclecount) {
   # Extra check for python workflow. If the counts get off due to a crash, just spit out previous params_new and params_stats.
   write(paste0("Cycle counts off so repeating last export. mCurrent=", mCurrent, " cyclecount=", cyclecount), stdout())
   if (exists("paramStats")) write.table(paramStats, file=paste0(runDir, "/params_stats.txt"), row.names=FALSE, sep=" ")
   if (exists("x_new_out")) write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")

   fileConn <- file(paste0(runDir, "/R_COMPLETE"))
   writeLines('', fileConn)
   close(fileConn)

   quit("no")

 } else {

   # Read model out and calculate performance metric
   outPath <- paste0(runDir, "/OUTPUT")
   write(paste0("Output dir: ", outPath), stdout())

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

   ### Reading the Streamflow values (Later Snow and Soil Moisture and other variables would be added)
   if (hydro_SPLIT_OUTPUT_COUNT == 1) {

      # Read files
      write(paste0("Reading model out files. Parallel ", parallelFlag, " ncores=", ncores), stdout())
      filesList <- list.files(path = outPath,
                             pattern = glob2rx("*.CHANOBS_DOMAIN*"),
                             full.names = TRUE)
      filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
      whFiles <- which(filesListDate >= startDate)
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

   } else if (hydro_SPLIT_OUTPUT_COUNT == 0) {

      write(paste0("Reading model out file : CHANOBS_DOMAIN1.nc"), stdout())
      chanobsFile <- list.files(outPath, pattern = glob2rx("CHANOBS_DOMAIN1.nc"), full.names = TRUE)
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
     chrt <- chrt[POSIXct > startDate, ]

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
  }

   # Stop cluster
   if (parallelFlag) stopCluster(cl)

   # Check for empty output
   if (nrow(chrt) < 1) {
       write(paste0("No data found in model output for link ", linkId, " after start date ", startDate), stdout())
       fileConn <- file(paste0(runDir, "/CALC_STATS_MISSING"))
       writeLines('', fileConn)
       close(fileConn)
       quit("no")
   }

   # Convert the observation dataset to a data.table if it hasn't already.
   obsStrData <- as.data.table(obsStrData)

   # Convert to daily if needed and tag object
   if (calcDailyStats) {
     chrt.d <- Convert2Daily(chrt)
     assign(paste0("chrt.obj.", cyclecount), chrt.d)
     chrt.obj <- copy(chrt.d)
     obs.obj <- Convert2Daily(obsStrData)
   } else {
     assign(paste0("chrt.obj.", cyclecount), chrt)
     chrt.obj <- copy(chrt)
     obs.obj <- copy(obsStrData)
   }

   # Merge
   setkey(chrt.obj, "site_no", "POSIXct")
   if ("Date" %in% names(obs.obj)) obs.obj[, Date := NULL]
   # Convert the observation dataset to a data.table if it hasn't already.
   obs.obj <- as.data.table(obs.obj)
   setkey(obs.obj, "site_no", "POSIXct")
   chrt.obj <- merge(chrt.obj, obs.obj, by=c("site_no", "POSIXct"), all.x=TRUE, all.y=FALSE)
   # Check for empty output
   if (nrow(chrt.obj) < 1) {
      write(paste0("No data found in obs for gage ", siteId, " after start date ", startDate), stdout())
      fileConn <- file(paste0(runDir, "/CALC_STATS_MISSING"))
      writeLines('', fileConn)
      close(fileConn)
      quit("no")
   }

   # Calc stats
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
      x_archive[cyclecount,] <- c(cyclecount, x_new, F_new, stat[, c(metrics), with = FALSE])

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
      x_archive[cyclecount,] <- c(cyclecount, x_new, F_new, statW)
      index1 = (cyclecount - 1) *nrow(stat)
      for(i in 1:nrow(stat)) {
        x_archive_2[index1+i,] <- c(cyclecount, x_new, F_new, stat[i,c(metrics), with = FALSE],
                                    stat$site_no[i])
      }

 }
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
   #MOVE WRITE TO END: write.table(paramStats, file=paste0(runDir, "/params_stats.txt"), row.names=FALSE, sep=" ")

   if (cyclecount < m) {
      # Select next parameter set
      x_new <- DDS.sel(i=cyclecount, m=m, r=r, xnames=xnames, x_min=x_min, x_max=x_max, x_best=x_best)
      cyclecount <- cyclecount+1  

      # Output next parameter set
      x_new_out <- c(cyclecount, x_new)
      names(x_new_out)[1] <- "iter"
      #MOVE WRITE TO END: write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")
      write(x_new_out, stdout())
   } else {
      lastcycle <- TRUE
   }


#########################################################
# PLOTS
#########################################################
# First we check if all the objective function values are less than the threshold (here 5), define it as no outlier in the iterations
# If there are objFun values greater than the threshold in the objFun, then calulate the 90% of the objFun
# Any iteration with objFun values above the 90% would be flagged as outlier. And then two plots will be created 
# one with all iteration including the outliers, two only 90% of the data if there was an outlier in the model. 
 
objFunThreshold <- 5
objFunQuantile <- quantile(x_archive$obj, 0.9)

if (any(x_archive$obj > objFunThreshold)) {
   write("Outliers found!", stdout())

   # Check which outlier threshold to use
   if (any(x_archive$obj <= objFunThreshold)) {
     x_archive_plot <- subset(x_archive, x_archive$obj <= objFunThreshold)
     x_archive_plot_count <- nrow(x_archive) - nrow(x_archive_plot) 
     x_archive_plot_threshold <- objFunThreshold
   } else {
     x_archive_plot <- subset(x_archive, x_archive$obj <= objFunQuantile)
     x_archive_plot_count <- nrow(x_archive) - nrow(x_archive_plot)
     x_archive_plot_threshold <- objFunQuantile
   }

   if (!exists("x_archive_plot_count_track")) x_archive_plot_count_track <- data.frame()
   x_archive_plot_count_track <- rbind(x_archive_plot_count_track, data.frame(iter=ifelse(lastcycle, cyclecount, cyclecount-1), outliers=nrow(x_archive)-nrow(x_archive_plot)))

   # Outlier count
   if (nrow(x_archive_plot_count_track) > 0) {
       write("Outlier count plot...", stdout())
       gg <- ggplot(data=x_archive_plot_count_track, aes(x=iter, y=outliers)) +
            geom_point() + theme_bw() +
            labs(x="run", y="count of outlier cycles")
       ggsave(filename=paste0(writePlotDir, "/", siteId, "_calib_outliers.png"),
            plot=gg, units="in", width=6, height=5, dpi=300)
   }

} else {
  write("No outliers found.", stdout())
  # All the objFun vlaues are less than the threshold defined above, therefore, there will not be any outliers specified
   x_archive_plot <- x_archive
   x_archive_plot_count <- 0
   x_archive_plot_threshold <- objFunThreshold
}

#**************************************************************************************************************************************
#                                   Create the plots with outlier
#**************************************************************************************************************************************

   # Update basic objective function plot
   write("Basin objective function plot...", stdout())
   gg <- ggplot(data=x_archive, aes(x=iter, y=obj)) + 
              geom_point() + theme_bw() + 
              labs(x="run", y="objective function")
   gg <- gg + geom_point(data = x_archive[iter_best,], aes(x=iter, y=obj,size = "Best Iteration"), color = "red", shape = 8)
   gg <- gg + scale_size_manual(name = "", values = 2) 
   gg <- gg + scale_x_continuous(name = "Iteration") + scale_y_continuous(name="Objective Function")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_calib_run_obj_outlier.png"),
              plot=gg, units="in", width=6, height=4, dpi=300)

   # Update the Objective function versus the parameter variable
   write("Obj function vs. params...", stdout())
   DT.m1 = melt(x_archive[, setdiff(names(x_archive), metrics)], id.vars = c("obj"), measure.vars = setdiff( names(x_archive), c(metrics, "iter", "obj")))
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   DT.m1.best <- melt(x_archive[iter_best, setdiff(names(x_archive), metrics)], id.vars = c("obj"), measure.vars = setdiff( names(x_archive), c(metrics, "iter", "obj")))
   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(value, obj))
   gg <- gg + ggplot2::geom_point(size = 1, color = "black", alpha = 0.3)+facet_wrap(~variable, scales="free_x")
   gg <- gg + ggplot2::geom_point(data = DT.m1.best, aes(value, obj), size = 2, color = "red", shape = 8)+facet_wrap(~variable, scales="free_x")
   gg <- gg + ggplot2::ggtitle(paste0("Objective function vs. parameters: ", siteId, "\n", siteName))
   gg <- gg + ggplot2::xlab("Parameter Values")+theme_bw()+ggplot2::ylab("Objective Function")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_obj_vs_parameters_calib_run_outlier.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)


   # Plot the variables as a function of calibration runs
   write("Params over runs...", stdout())
   DT.m1 = melt(x_archive[, setdiff(names(x_archive), metrics)], id.vars = c("iter"), measure.vars = setdiff(names(x_archive), c("iter", metrics)))
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   DT.m1.best = melt(x_archive[iter_best, setdiff(names(x_archive), metrics)], id.vars = c("iter"), measure.vars = setdiff(names(x_archive), c("iter", metrics)))
   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(iter, value))
   gg <- gg + ggplot2::geom_point(size = 1, color = "black", alpha = 0.3)+facet_wrap(~variable, scales="free_y")
   gg <- gg + ggplot2::geom_point(data = DT.m1.best,  ggplot2::aes(iter, value), size = 2, color = "red", shape = 8)+facet_wrap(~variable, scales="free_y")
   gg <- gg + ggplot2::ggtitle(paste0("Parameter change with iteration: ", siteId, "\n", siteName))
   gg <- gg + ggplot2::xlab("Calibration Iteration")+theme_bw()
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_parameters_calib_run_outlier.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)

   # Plot all the stats
   write("Metrics plot...", stdout())
   #DT.m1 = melt(x_archive[,which(names(x_archive) %in% c("iter", "obj", "cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof"))],
   #            iter.vars = c("iter"), measure.vars = c("obj", "cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof"))
   DT.m1 = melt(x_archive[,which(names(x_archive) %in% c("iter", "obj", metrics))],
               iter.vars = c("iter"), measure.vars = c("obj", metrics))
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))

   DT.m1.best = melt(x_archive[iter_best,which(names(x_archive) %in% c("iter", "obj", metrics))],
               iter.vars = c("iter"), measure.vars = c("obj", metrics))

   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(iter, value))
   gg <- gg + ggplot2::geom_point(size = 1, color = "black", alpha = 0.3)+facet_wrap(~variable, scales="free")
   gg <- gg + ggplot2::geom_point(data = DT.m1.best, ggplot2::aes(iter, value), size = 2, color = "red", shape = 8)+facet_wrap(~variable, scales="free")
   gg <- gg + ggplot2::ggtitle(paste0("Metric sensitivity: ", siteId, "\n", siteName))
   gg <- gg + ggplot2::xlab("Calibration Iteration No.")+theme_bw()+ylab("Value")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_metric_calib_run_outlier.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)

#############################################################################################################################################################################
#                      Create the plots without outliers
############################################################################################################################################################################3

  # Update basic objective function plot
   write("Basin objective function plot...", stdout())
   gg <- ggplot(data=x_archive_plot, aes(x=iter, y=obj)) +
              geom_point() + theme_bw() +
              labs(x="run", y="objective function") +
              ggtitle(paste0("ObjFun: ", siteId,  ", No. outliers = ", x_archive_plot_count, ", Threshold = ",  formatC(x_archive_plot_threshold, digits  = 4), "\n", siteName))
   gg <- gg + geom_point(data = x_archive[iter_best,], aes(x=iter, y=obj,size = "Best Iteration"), color = "red", shape = 8)
   gg <- gg + scale_size_manual(name = "", values = 2)
   gg <- gg + scale_x_continuous(name = "Iteration") + scale_y_continuous(name="Objective Function")


   ggsave(filename=paste0(writePlotDir, "/", siteId, "_calib_run_obj.png"),
              plot=gg, units="in", width=6, height=4, dpi=300)

   # Update the Objective function versus the parameter variable
   write("Obj function vs. params...", stdout())
   DT.m1 = melt(x_archive_plot[, setdiff(names(x_archive_plot), metrics)], id.vars = c("obj"), measure.vars = setdiff( names(x_archive_plot), c(metrics, "iter", "obj")))
   DT.m1.best <- melt(x_archive[iter_best, setdiff(names(x_archive_plot), metrics)], id.vars = c("obj"), measure.vars = setdiff( names(x_archive), c(metrics, "iter", "obj")))
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(value, obj))
   gg <- gg + ggplot2::geom_point(size = 1, color = "black", alpha = 0.3)+facet_wrap(~variable, scales="free_x")
   gg <- gg + ggplot2::geom_point(data = DT.m1.best, aes(value, obj), size = 2, color = "red", shape = 8)+facet_wrap(~variable, scales="free_x")
   gg <- gg + ggplot2::ggtitle(paste0("ObjFun vs. Params: ", siteId,  ", No. outliers = ", x_archive_plot_count, ", Threshold = ",  formatC(x_archive_plot_threshold, digits  = 4), "\n", siteName))
   gg <- gg + ggplot2::xlab("Parameter Values")+theme_bw()+ggplot2::ylab("Objective Function")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_obj_vs_parameters_calib_run.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)


   # Plot the variables as a function of calibration runs
   write("Params over runs...", stdout())
   DT.m1 = melt(x_archive_plot[, setdiff(names(x_archive_plot), metrics)], id.vars = c("iter"), measure.vars = setdiff(names(x_archive_plot), c("iter", metrics)))
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   DT.m1.best = melt(x_archive_plot[iter_best, setdiff(names(x_archive_plot), metrics)], id.vars = c("iter"), measure.vars = setdiff(names(x_archive_plot), c("iter", metrics)))

   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(iter, value))
   gg <- gg + ggplot2::geom_point(size = 1, color = "black", alpha = 0.3)+facet_wrap(~variable, scales="free_y")
   gg <- gg + ggplot2::geom_point(data = DT.m1.best, aes(iter, value), size = 2, color = "red", shape = 8)+facet_wrap(~variable, scales="free_y")
   gg <- gg + ggplot2::ggtitle(paste0("Parameter vs. iteration: ", siteId,  ", No. outliers = ", x_archive_plot_count, ", Threshold = ",  formatC(x_archive_plot_threshold, digits  = 4), "\n", siteName))
   gg <- gg + ggplot2::xlab("Calibration Iteration")+theme_bw()
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_parameters_calib_run.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)

   # Plot all the stats
   write("Metrics plot...", stdout())
   #DT.m1 = melt(x_archive_plot[,which(names(x_archive_plot) %in% c("iter", "obj", "cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof"))],
   #            iter.vars = c("iter"), measure.vars = c("obj", "cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof"))
   DT.m1 = melt(x_archive_plot[,which(names(x_archive_plot) %in% c("iter", "obj", metrics))],
               iter.vars = c("iter"), measure.vars = c("obj", metrics))
   DT.m1 <- subset(DT.m1, !is.na(DT.m1$value))
   DT.m1.best = melt(x_archive_plot[iter_best,which(names(x_archive_plot) %in% c("iter", "obj", metrics))],
               iter.vars = c("iter"), measure.vars = c("obj", metrics))

   gg <- ggplot2::ggplot(DT.m1, ggplot2::aes(iter, value))
   gg <- gg + ggplot2::geom_point(size = 1, color = "black", alpha = 0.3)+facet_wrap(~variable, scales="free")
   gg <- gg + ggplot2::geom_point(data = DT.m1.best, ggplot2::aes(iter, value), size = 1, color = "red", shape = 8)+facet_wrap(~variable, scales="free")
   gg <- gg + ggplot2::ggtitle(paste0("Metric Sensitivity: ", siteId, ", No. outliers = ", x_archive_plot_count, ", Threshold = ",  formatC(x_archive_plot_threshold, digits  = 4), "\n", siteName))
   gg <- gg + ggplot2::xlab("Calibration Iteration No.")+theme_bw()+ylab("Value")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_metric_calib_run.png"),
         plot=gg, units="in", width=8, height=6, dpi=300)

   # Plot the time series of the observed, control, best calibration result and last calibration iteration
   write("Hydrograph...", stdout())
   # The first iteration is the control run  called chrt.obj.1
   controlRun <- copy(chrt.obj.1)
   controlRun [, run := "Control Run"]
   # We have already advanced the cyclescount, so subtract 1 to get last complete
   lastRun <- copy(get(paste0("chrt.obj.", ifelse(lastcycle, cyclecount, cyclecount-1))))
   lastRun [ , run := "Last Run"]
   # the best iteration should be find
   bestRun <- copy(get(paste0("chrt.obj.", iter_best)))
   bestRun [ , run := "Best Run"]

   obsStrDataPlot <- copy(chrt.obj)
   obsStrDataPlot[, q_cms := NULL]
   setnames(obsStrDataPlot, "obs", "q_cms")
   obsStrDataPlot <- obsStrDataPlot[, c("q_cms", "POSIXct", "site_no"), with=FALSE]
   obsStrDataPlot <- obsStrDataPlot[as.integer(POSIXct) >= min(as.integer(controlRun$POSIXct)) & as.integer(POSIXct) <= max(as.integer(controlRun$POSIXct)),]
   obsStrDataPlot[ , run := "Observation"]

   chrt.obj_plot <- rbindlist(list(controlRun, lastRun, bestRun, obsStrDataPlot), use.names = TRUE, fill=TRUE)
   # Cleanup
   rm(controlRun, lastRun, bestRun, obsStrDataPlot)


   gg <- ggplot2::ggplot(chrt.obj_plot, ggplot2::aes(POSIXct, q_cms, color = run)) + facet_wrap(~site_no, , scales="free_y", ncol = 1)
   gg <- gg + ggplot2::geom_line(size = 0.3, alpha = 0.7)
   gg <- gg + ggplot2::ggtitle(paste0("Streamflow time series for ", siteId, "\n", siteName))
   #gg <- gg + scale_x_datetime(limits = c(as.POSIXct("2008-10-01"), as.POSIXct("2013-10-01")))
   gg <- gg + ggplot2::xlab("Date")+theme_bw( base_size = 15) + ylab ("Streamflow (cms)")
   gg <- gg + scale_color_manual(name="", values=c('black', 'dodgerblue', 'orange' , "dark green"),
                                 limits=c('Observation','Control Run', "Best Run", "Last Run"),
                                  label=c('Observation','Control Run', "Best Run", "Last Run"))

   ggsave(filename=paste0(writePlotDir, "/", siteId, "_hydrograph.png"),
           plot=gg, units="in", width=8, height=4, dpi=300)


# Plot the scatter plot of the best, last and control run.
   write("Scatterplot...", stdout())
   maxval <- max(chrt.obj_plot$q_cms, na.rm = TRUE)
   gg <- ggplot()+ geom_point(data = merge(chrt.obj_plot [run %in% c("Control Run", "Last Run", "Best Run")], obs.obj, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE),
                              aes (obs, q_cms, color = run), alpha = 0.5) + facet_wrap(~site_no)
   gg <- gg + scale_color_manual(name="", values=c('dodgerblue', 'orange' , "dark green"),
                                 limits=c('Control Run', "Best Run", "Last Run"),
                                 label=c('Control Run', "Best Run", "Last Run"))
   gg <- gg + ggtitle(paste0("Simulated vs. observed flow : ", siteId, "\n", siteName)) + theme_bw( base_size = 15)
   gg <- gg + geom_abline(intercept = 0, slope = 1) + coord_equal()+ xlim(0,maxval) + ylim(0,maxval)
   gg <- gg + xlab("Observed flow (cms)") + ylab ("Simulated flow (cms)")

   ggsave(filename=paste0(writePlotDir, "/", siteId, "_scatter.png"),
           plot=gg, units="in", width=8, height=8, dpi=300)



#########################################################
# SAVE & EXIT
#########################################################

   # Save and exit
   rm(objFn, mCurrent, r, siteId, rtlinkFile, linkId, startDate, ncores)
   save.image(paste0(runDir, "/proj_data.Rdata"))

   # Write param files
   write.table(paramStats, file=paste0(runDir, "/params_stats.txt"), row.names=FALSE, sep=" ")
   if (cyclecount <= m) write.table(data.frame(t(x_new_out)), file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")

   # remove the CAHNOBS_DOMAIN file since we do not need it anymore , and the files gets appended if left there
   if (hydro_SPLIT_OUTPUT_COUNT == 0) file.remove(chanobsFile)

   #system(paste0("touch ", runDir, "/R_COMPLETE"))
   fileConn <- file(paste0(runDir, "/R_COMPLETE"))
   writeLines('', fileConn)
   close(fileConn)

   write(summary(proc.time()), stdout())

   quit("no")

 }

}



