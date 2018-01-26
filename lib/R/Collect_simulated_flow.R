
# This is to read the CHNOBS files and write out the time series to Rdata set,
# Goal: After each run is done, we would take out the simulated flow and 
# save it into a Rdata set and remove the files ...

#outPath <- "/glade/u/home/karsten/AREZOO/OUTPUT_0"
#linkId =  21983449 
#ncores = 1
#startDate <- as.POSIXct("20130102", format = "%Y%m%d", tz = "UTC")

args <- commandArgs(trailingOnly=TRUE)
outPath <- args[1]

source('../namelist.sensitivity')
source('../calib_utils.R')
ncores = 1


############### these are what are in the calib util, I am leaving it here for sanity, you can check it with out this also.
library(data.table)

##########################################################################################################

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

# the required Function to read obs CHNOBS file
ReadObsFile <- function(file, linkId){
  a <- GetNcdfFile(file, variables = c("feature_id", "streamflow"), quiet = TRUE)
  a <- subset(a, feature_id == linkId)
  a$POSIXct <- as.POSIXct(strsplit(basename(file),"[.]")[[1]][1], format = "%Y%m%d%H%M", tz = "UTC")
  return(a)
}


# Read files
message("Reading model out files.")
filesList <- list.files(path = outPath,
                        pattern = glob2rx("*.CHANOBS_DOMAIN1*"),
                        full.names = TRUE)
filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
whFiles <- which(filesListDate >= startDate)
filesList <- filesList[whFiles]
if (length(filesList) == 0) stop("No matching files in specified directory.")
chrt <- as.data.table(plyr::ldply(filesList, ReadObsFile, linkId, .parallel = parallelFlag))
setnames(chrt, "streamflow", "q_cms")

save(chrt, file = paste0(outPath, "/chrt.Rdata"))

# submit removing the CHNOBS file


