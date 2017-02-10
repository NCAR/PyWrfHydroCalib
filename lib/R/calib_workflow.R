#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly=TRUE)
namelistFile <- args[1]

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
metrics <- c("cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge")

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
   load(paste0(runDir, "/obsStrData.Rdata"))
   if ("q_cms" %in% names(obsStrData)) obsStrData$q_cms <- NULL

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
   save.image(paste0(runDir, "/proj_data.Rdata"))
   system(paste0("touch ", runDir, "/R_COMPLETE"))
   quit("no")
}

if (cyclecount > 0) {

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
   chrt <- as.data.table(plyr::ldply(filesList, ReadChFile, linkId, .parallel = parallelFlag))
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

   # Archive results
   x_archive[cyclecount,] <- c(cyclecount, x_new, F_new, statCor, statRmse, statBias, statNse, statNseLog, statNseWt, statKge)

   # Evaluate objective function
   if (cyclecount == 1) {
      x_best <- x_new
      F_best <- F_new
      bestFlag <- 1
   } else if (F_new <= F_best) {
      x_best <- x_new
      F_best <- F_new
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

   # Update plot
   gg <- ggplot(data=x_archive, aes(x=iter, y=obj)) + 
              geom_point() + theme_bw() + 
              labs(x="run", y="objective function")
   ggsave(filename=paste0(writePlotDir, "/", siteId, "_calib_run_obj.png"),
              plot=gg, units="in", width=6, height=5, dpi=300)


   # Save and exit
   save.image(paste0(runDir, "/proj_data.Rdata"))
   system(paste0("touch ", runDir, "/R_COMPLETE"))
   quit("no")

}



