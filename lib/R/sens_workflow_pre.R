#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly=TRUE)
namelistFile <- 'namelist.sensitivity'

#library(rwrfhydro)
library(data.table)
library(ggplot2)
library(plyr)
library(sensitivity)
library(randtoolbox)

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

# First run so need to initialize
cyclecount <- 0

# Setup value lists from paramBnds
message("Setup lists")

# Read parameter bounds 
paramBnds <- read.table(paste0(runDir, "/sens_params.tbl"), header=TRUE, sep=",", stringsAsFactors=FALSE)
paramBnds <- subset(paramBnds, paramBnds$sens_flag==1)

if (SA_method == "DELSA") {
  
  # Setup value lists from paramBnds
  par.ranges <- list()
  for(i in 1:nrow(paramBnds)) {
    par.ranges[[paramBnds[i, "parameter"]]] <- unname(unlist(paramBnds[i, c("minValue", "maxValue")]))
  }
  X0 <- sensitivity::parameterSets(par.ranges, samples = SA_sample_size, method = c(SA_par_gen_method))
  varprior = sapply(par.ranges, diff)^2/12
  
  X = do.call(rbind, lapply(1:ncol(X0), function(i) {
    X2i = X0
    X2i[, i] = X2i[, i] * SA_perturb
    X2i
  }))
  
  x_all = as.data.frame(rbind(X0, X))
  names(x_all) <- names(par.ranges)
  
} else {
  stop("It only supports the DELSA technique at this point")
}

# add the index as the first columns, since it existed in Aubrey s way of coding 
x_all <- cbind.data.frame(id = c(1:nrow(x_all)), x_all)

# Initialize parameter archive DF
message("Initialize parameter archive")
x_archive <- as.data.frame(matrix(, nrow=1, ncol=ncol(x_all)+length(metrics)))
names(x_archive) <- c(names(x_all), metrics)

x_archive_h <- x_archive # adding this since we want to calculate both at daily and hourly time step

# Output parameter set
message("Output parameter set")
write.table(x_all, file=paste0(runDir, "/params_new.txt"), row.names=FALSE, sep=" ")

# Save and exit
save.image(paste0(runDir, "/proj_data_SENS.Rdata"))
quit("no")

