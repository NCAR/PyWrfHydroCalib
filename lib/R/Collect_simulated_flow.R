
# This is to read the CHNOBS files and write out the time series to Rdata set,
# Goal: After each run is done, we would take out the simulated flow and 
# save it into a Rdata set and remove the files ...

outPath <- "/glade/scratch/arezoo/SA/wrfhydro_calib/CalibDemo2/RUN.TEMPLATE/output_CHNOB"
linkId =  8329634
nCores = 1

#------------------------- DO NOT TOUCH FROM HERE

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
  a <- subset(GetNcdfFile(f, quiet = TRUE), feature_id == linkId)[, c("feature_id", "streamflow", "time")]
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
setNames(chrt, "sreamflow", "q_cms")

save(chrt, file = paste0(outPath, "/chrt.Rdata"))

# submit removing the CHNOBS file


