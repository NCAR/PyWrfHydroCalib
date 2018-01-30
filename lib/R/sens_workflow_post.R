#!/usr/bin/env Rscript
namelistFile <- 'namelist.sensitivity'

library(data.table)
library(ggplot2)
library(plyr)
library(boot)
#########################################################
# SETUP
#########################################################

source("calib_utils.R")
source(namelistFile)
objFunc <- get(objFn)

# Metrics
metrics <- c("objFn", "cor", "rmse", "bias", "nse", "nselog", "nsewt", "kge", "msof")

#########################################################
# MAIN CODE
#########################################################

if (file.exists(paste0(runDir,"/proj_data_SENS.Rdata"))) { 
  load(paste0(runDir,"/proj_data_SENS.Rdata"))
} else {
  message("No proj_data_SENS.Rdata file found")
}

# Read parameter bounds 
#paramBnds <- read.table(paste0(runDir, "/sens_params.tbl"), header=TRUE, sep=",", stringsAsFactors=FALSE)
#paramBnds <- subset(paramBnds, paramBnds$sens_flag==1)

# Setup plot directory
writePlotDir <- paste0(runDir, "/plots")
dir.create(writePlotDir)

# Load obs so we have them for next iteration
load(paste0(runDir, "/OBS/obsStrData.Rdata"))
#obsDT <- obsDT[!is.na(obs),]
obsDT <- obsStrData[!is.na(obs),]

# convrt the hourly obs to daily obs
obsDT$Date <- CalcDateTrunc(obsDT$POSIXct)
setkey(obsDT, Date)
obsDT.d <- obsDT[, list(obs = mean(obs, na.rm = TRUE)), by = "Date"]

# Find the index of the gage
#rtLink <- ReadRouteLink(rtlinkFile)
#rtLink <- data.table(rtLink)
#linkId <- which(trimws(rtLink$gages) %in% siteId)

# Initialize chrtout
if (!exists("chrt.d.all")) chrt.d.all <- data.table()
if (!exists("chrt.h.all")) chrt.h.all <- data.table()

for (cyclecount in 1:nrow(x_all)) {
  # Read model out and calculate performance metric
  outPath <- paste0(runDir, "/OUTPUT_", cyclecount-1)

  # Read files
  load(paste0(outPath, "/chrt.Rdata"))
  
  # add the chrt data to the chrt.h.all and calculate the stats for the hourly time step
  chrt[, site_no := siteId]
  setkey(chrt, "site_no", "POSIXct")
  setkey(obsDT, "site_no", "POSIXct")
  chrt.h <- merge(chrt, obsDT, by=c("site_no", "POSIXct"), all.x=FALSE, all.y=FALSE)
  chrt.h$id <- cyclecount
  # chrt.h$tag <- x_all$tag[cyclecount] We do not have any tag anymore
  chrt.h.all <- rbindlist(list(chrt.h.all, chrt.h))
  
  # Calc objective function
  F_new <- objFunc(chrt.h$q_cms, chrt.h$obs)
  if (objFn %in% c("Nse", "NseLog", "NseWt", "Kge")) F_new <- 1 - F_new
  
  # Calc stats
  statCor <- cor(chrt.h$q_cms, chrt.h$obs)
  statRmse <- Rmse(chrt.h$q_cms, chrt.h$obs, na.rm=TRUE)
  statBias <- PBias(chrt.h$q_cms, chrt.h$obs, na.rm=TRUE)
  statNse <- Nse(chrt.h$q_cms, chrt.h$obs, na.rm=TRUE)
  statNseLog <- NseLog(chrt.h$q_cms, chrt.h$obs, na.rm=TRUE)
  statNseWt <- NseWt(chrt.h$q_cms, chrt.h$obs)
  statKge <- Kge(chrt.h$q_cms, chrt.h$obs, na.rm=TRUE)
  statMsof <- Msof(chrt.h$q_cms, chrt.h$obs)
  if (is.na(statMsof)) statMsof <- 0.0 
 
  # Archive results
  x_archive_h[cyclecount,] <- c(x_all[cyclecount,], F_new, statCor, statRmse, statBias, statNse, statNseLog, statNseWt, statKge, statMsof)
  
  ########################## ####### DAILY CALCULATIONS ###################################################### 
  # Convert to daily
  chrt.d <- Convert2Daily(chrt)
  obsDT.d[, site_no := siteId]
  chrt.d[, site_no := siteId]
  # Merge
  setkey(chrt.d, "site_no", "Date")
  setkey(obsDT.d, "site_no", "Date")
  chrt.d <- merge(chrt.d, obsDT.d,  by=c("site_no", "Date"), all.x=FALSE, all.y=FALSE)
  chrt.d$id <- cyclecount
  # chrt.d$tag <- x_all$tag[cyclecount] We do not have tag here.
  chrt.d.all <- rbindlist(list(chrt.d.all, chrt.d))
  
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
  if (is.na(statMsof)) statMsof <- 0.0
  
  # Archive results
  x_archive[cyclecount,] <- c(x_all[cyclecount,], F_new, statCor, statRmse, statBias, statNse, statNseLog, statNseWt, statKge, statMsof)
}

# Interim save
save.image(paste0(runDir,"/proj_data_SENS.Rdata"))

# Saving stat files for the daily and hourly time step. 

stat_daily <- x_archive[, c("id", metrics)]
stat_daily$timeStep <- "daily"
stat_hourly <- x_archive_h[, c("id", metrics)]
stat_hourly$timeStep <- "hourly"
stat <- rbind(stat_daily, stat_hourly)


write.table(stat, file=paste0(runDir, "/stat_sensitivity.txt"), row.names=FALSE, sep=" ")

################################ DELSA Calculations for each Metric at both hourly and daily time step

if (SA_method == "DELSA") {
  delsaFirst <- list()
  for (timeStep in c("hourly", "daily")) {
    for (metric in metrics)  {
      if (timeStep == "daily") x <- list(y = x_archive[, metric], X0 = X0, X = rbind(X0, X), varprior = varprior)
      if (timeStep == "hourly") x <- list(y = x_archive_h[, metric], X0 = X0, X = rbind(X0, X), varprior = varprior)
      
      id <- deparse(substitute(x))
      
      Kpar = ncol(x$X0)
      Nsamp = nrow(x$X0)
      vartot = rep(0, Nsamp)
      delsafirst = deriv = varfir = matrix(NA, ncol = Kpar, nrow = Nsamp)
      out <- as.numeric(x$y)
      for (rsamp in 1:Nsamp) {
        for (jpar in 1:Kpar) {
          idx.pert = Nsamp * jpar + rsamp
          deriv[rsamp, jpar] = (out[idx.pert] - out[rsamp])/(x$X[idx.pert,
                                                                 jpar] - x$X[rsamp, jpar])
          varfir[rsamp, jpar] = (deriv[rsamp, jpar]^2) * (x$varprior[jpar])
          vartot[rsamp] = vartot[rsamp] + varfir[rsamp, jpar]
          if (jpar == Kpar) {
            for (jjpar in 1:Kpar) delsafirst[rsamp, jjpar] = varfir[rsamp,
                                                                    jjpar]/vartot[rsamp]
          }
        }
      }
      colnames(delsafirst) = colnames(x$X)
      delsaFirst[[timeStep]][[metric]]$delsafirst = delsafirst
      assign(id, x, parent.frame())
    }
  }
}


# the default plots from the Sensitivity Packages
obj = x

#plot1 # these plots are only provided for the daily timestep and the objective function as the metric
temp = as.data.frame(delsaFirst$daily$obj$delsafirst)
names(temp) <-  names(x_all)[2:ncol(x_all)]
temp$id <- 1:nrow(temp)
temp = reshape2::melt(temp, id.var = "id")
gg <- ggplot2::ggplot(data = temp, ggplot2::aes(x = value,
                                                colour = variable)) + 
  ggplot2::stat_ecdf() +
  ggplot2::scale_x_continuous("DELSA results for first order sensitivity") +
  ggplot2::scale_y_continuous("Cum. frequency") +
  ggplot2::labs(title = "CDF of first order sensitivity across parameter space")
ggsave(filename=paste0(writePlotDir, "/", chrt.d.all$site_no[1], "_CDF_DELSA.png"),
       plot=gg, units="in", width=16, height=8, dpi=300)

#plot2
temp$y <- obj$y[temp$id]

temp2 = as.data.frame(obj$X0)
names(temp2) <-  names(x_all)[2:ncol(x_all)]
temp2$id <- 1:nrow(temp2)
temp2 = reshape2::melt(temp2, id.var = "id")
temp2$x <- temp2$value
temp2$value <- NULL
temp = merge(temp, temp2)

gg <- ggplot2::ggplot(data = temp) + ggplot2::geom_point(ggplot2::aes(x = value,
                                                                      y = y)) + 
  ggplot2::scale_x_continuous(name = "DELSA first order sensitivity") +
  ggplot2::scale_y_continuous(name = "Model output") +
  ggplot2::facet_wrap(~variable, scales = "free") +
  ggplot2::labs(title = "First order sensitivity as related to model response")
ggsave(filename=paste0(writePlotDir, "/", chrt.d.all$site_no[1], "_DELSA_model_response.png"),
       plot=gg, units="in", width=16, height=8, dpi=300)

#plot3
gg <- ggplot2::ggplot(data = temp) + ggplot2::geom_point(ggplot2::aes(y = value,
                                                                      x = x, colour = y)) + 
  ggplot2::scale_y_continuous(name = "DELSA first order sensitivity") +
  ggplot2::scale_x_continuous(name = "Parameter value") +
  ggplot2::scale_color_continuous(name = "Model response") +
  ggplot2::facet_wrap(~variable, scales = "free") +
  ggplot2::labs(title = "First order sensitivity as as related to parameter value")
ggsave(filename=paste0(writePlotDir, "/", chrt.d.all$site_no[1], "_DELSA_parameter_value.png"),
       plot=gg, units="in", width=16, height=8, dpi=300)

# Let s do a bootstrap resampling, I want to do this for all the metrics and both temporal resolutions
Quantile <- function(data, indices, SA_quantileFrac = 0.9) {
  d <- data[indices] # allow boot to select sample
  quantileNo <- quantile(d, SA_quantileFrac) #calcualte the quantile
  return(quantileNo)
}

bootRes <- data.table()
for (timeStep in c("hourly", "daily")) {
  for (metric in setdiff(metrics, "msof")) {
    for (param in 1:(ncol(x_all)-1)) {
      results <- boot(data=delsaFirst[[timeStep]][[metric]]$delsafirst[, param],
                      statistic=Quantile, 
                      R=SA_bootstrap_replicates)
      bootRes <- rbindlist(list(bootRes, data.table(delsaFirst = results$t[,1],
                                                    timeStep = timeStep, metric = metric, 
                                                    parameter = names(x_all)[param+1])))
    }
  }
}

# add the plots 
gg <- ggplot(bootRes, aes(parameter, delsaFirst)) + geom_boxplot()+
  facet_grid(metric~timeStep)+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggsave(filename=paste0(writePlotDir, "/", chrt.d.all$site_no[1], "_DELSA_uncertainty_estimate.png"),
       plot=gg, units="in", width=16, height=8, dpi=300)

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Summary plots

# Plot setup
ggPalette <- gg_color_hue(14)

plotGroups <- list(soil=c('bexp', 'dksat', 'smcmax', 'refkdt', 'slope', 'RETDEPRTFAC', 'LKSATFAC'),
                   other=c('Zmax', 'Expon', 'CWPVT', 'VCMX25', 'MP', 'HVT', 'MFSNO'))

# Hydrographs
gg <- ggplot(data=chrt.d.all) +
  geom_line(aes(x=Date, y=q_cms, color=id, group=id), lwd=0.6) +
  geom_point(aes(x=Date, y=obs, group=id), color = "black") + 
  scale_y_log10() +
  ggtitle(paste0("Model Sensitivity: ", chrt.d.all$site_no[1]))
ggsave(filename=paste0(writePlotDir, "/", chrt.d.all$site_no[1], "_hydrograph.png"),
       plot=gg, units="in", width=16, height=8, dpi=300)

# Save and exit
#if (parallelFlag) stopCluster(cl)
save.image(paste0(runDir,"/proj_data_SENS.Rdata"))

# Touch an empty COMPLETE file to inform the next step of the process this has completed.
fileConn <- file(paste0(runDir, "/postProc.COMPLETE"))
writeLines('', fileConn)
close(fileConn)

quit("no")


