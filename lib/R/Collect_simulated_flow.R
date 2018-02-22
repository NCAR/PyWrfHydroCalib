
# This is to read the CHNOBS files and write out the time series to Rdata set,
# Goal: After each run is done, we would take out the simulated flow and 
# save it into a Rdata set and remove the files ...

#outPath <- "/glade/u/home/karsten/AREZOO/OUTPUT_0"
#linkId =  21983449 
#ncores = 1
#startDate <- as.POSIXct("20130102", format = "%Y%m%d", tz = "UTC")

args <- commandArgs(trailingOnly=TRUE)
outPath <- args[1]

source('namelist.sensitivity')
source('calib_utils.R')
ncores = nCores


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
ReadObsFile <- function(cor, filesList, pointsNo, linkId){
  filesListSub <- filesList[which(pointsNo == cor)]
  output <- NULL
  for (file in filesListSub) {
    a <- GetNcdfFile(file, variables = c("feature_id", "streamflow"), quiet = TRUE)
    a <- subset(a, feature_id == linkId)
    a$POSIXct <- as.POSIXct(strsplit(basename(file),"[.]")[[1]][1], format = "%Y%m%d%H%M", tz = "UTC")
    if (is.null(output)) {output = a} else {output = rbind.data.frame(output, a)}
  }

  return(output)
}


# Read files
system.time({
  message("Reading model out files.")
  filesList <- list.files(path = outPath,
                          pattern = glob2rx("*.CHANOBS_DOMAIN1*"),
                          full.names = TRUE)
  print(outPath)
  print(filesList)
  filesListDate <- as.POSIXct(unlist(plyr::llply(strsplit(basename(filesList),"[.]"), '[',1)), format = "%Y%m%d%H%M", tz = "UTC")
  whFiles <- which(filesListDate >= startDate)
  filesList <- filesList[whFiles]
  if (length(filesList) == 0) stop("No matching files in specified directory.")

  pointsNo <- c(rep(1:ncores, each = floor(length(filesList)/ncores)), rep(ncores, length(filesList) %% ncores))

  chrt <- as.data.table(plyr::ldply(1:ncores,  ReadObsFile, linkId, .parallel = parallelFlag, pointsNo = pointsNo, filesList = filesList,
                                    .paropts=list(.export=c("GetNcdfFile", "NamedList"))))
  setnames(chrt, "streamflow", "q_cms")

  save(chrt, file = paste0(outPath, "/chrt.Rdata"))
})

# submit removing the CHNOBS file
system(paste0("rm -rf ",outPath,"/*.CHANOBS*"))

# Touch an empty COMPLETE file to inform the next step of the process this has completed.
fileConn <- file(paste0(outPath, "/R_COLLECT.COMPLETE"))
writeLines('', fileConn)
close(fileConn)

quit("no")
