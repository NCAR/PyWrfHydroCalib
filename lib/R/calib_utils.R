###----------------- UTILITIES -------------------###

# Functions migrated from rwrfhydro to remove rwrfhydro dependency for user.

NamedList <- function(theNames) {
  theList <- as.list(theNames)
  names(theList)<- theNames
  theList
}

GetNcdfFile <- function (file, variables = NULL, exclude = FALSE, quiet = FALSE,
          flip2D = TRUE, collapse_degen = TRUE) {
  if (!file.exists(file))
    warning(paste0("The file ", file, "does not exist."),
            immediate. = TRUE)
  if (!quiet)
    ncdump(file)
  nc <- ncdf4::nc_open(file)
  varsInFile <- names(nc$var)
  dimVarsInFile <- names(nc$dim)
  whDimVarsVals <- plyr::laply(nc$dim, "[[", "create_dimvar")
  if (any(whDimVarsVals))
    varsInFile <- c(dimVarsInFile[whDimVarsVals], varsInFile)
  returnVars <- if (!is.null(variables)) {
    varsNotInFile <- setdiff(variables, varsInFile)
    if (length(varsNotInFile))
      warning(paste0("The following variables were not found in the file",
                     paste(varsNotInFile, collapse = ", ")))
    if (!exclude)
      intersect(variables, varsInFile)
    else setdiff(varsInFile, variables)
  }
  else varsInFile
  varNDims <- unlist(lapply(nc$var, function(vv) vv$ndims))
  if (length(whZeroDim <- which(varNDims == 0))) {
    if (!quiet)
      cat("The following variables are ommitted because they have zero dimensions: ",
          names(whZeroDim), "\n")
    returnVars <- setdiff(returnVars, names(whZeroDim))
  }
  doGetVar <- function(theVar) ncdf4::ncvar_get(nc, varid = theVar,
                                                collapse_degen = collapse_degen)
  outList <- plyr::llply(NamedList(returnVars), doGetVar)
  doGetVarAtt <- function(theVar) ncdf4::ncatt_get(nc, varid = theVar)
  attList <- plyr::llply(NamedList(returnVars), doGetVarAtt)
  natts <- nc$natts
  if (natts > 0)
    attList$global <- ncdf4::ncatt_get(nc, 0)
  ncdf4::nc_close(nc)
  nDims <- plyr::laply(outList, function(ll) length(dim(ll)))
  if (flip2D & any(nDims == 2)) {
    wh2D <- which(nDims == 2)
    for (ww in wh2D) outList[[ww]] <- FlipUD(outList[[ww]])
  }
  if (!(all(nDims == nDims[1])) | !(all(nDims == 1)))
    return(outList)
  vecLen <- plyr::laply(outList[-10], length)
  if (all(vecLen == vecLen[1]))
    outList <- as.data.frame(outList)
  if (natts > 0)
    attributes(outList) <- c(attributes(outList), attList)
  outList
}

CalcDateTrunc <- function(timePOSIXct, timeZone="UTC") {
  timeDate <- as.Date(trunc(as.POSIXct(format(timePOSIXct, tz=timeZone),
                                       tz=timeZone), "days"))
  return(timeDate)
}

ReadRouteLink <- function(linkFile) {
     rtLinks <- GetNcdfFile(linkFile, variables=c("time"), exclude=TRUE, quiet=TRUE)
     rtLinks$site_no <- stringr::str_trim(rtLinks$gages)
     rtLinks
}

# Namelist read function

ReadNamelist <- function(nlist) {
   source(nlist)
   #load(obsFile)
}

# Convert to daily flow

Convert2Daily <- function(str) {
   #str$Date <- rwrfhydro::CalcDateTrunc(str$POSIXct)
   str$Date <- CalcDateTrunc(str$POSIXct)
   setkey(str, Date)
   if ("q_cms" %in% names(str)) {
      str.d <- str[, list(q_cms=mean(q_cms, na.rm=TRUE)), by = "Date"]
   } else if ("obs" %in% names(str)) {
      str.d <- str[, list(obs=mean(obs, na.rm=TRUE)), by = "Date"]
   }
   str.d$POSIXct <- as.POSIXct(paste0(str.d$Date, " 00:00"), tz="UTC")
   str.d
}

# Read streamflow from netcdf file

ReadChFile <- function(file, idList){
    nc <- ncdf4::nc_open(file)
    output <- data.frame(q_cms = ncdf4::ncvar_get(nc, varid = "streamflow", start = idList , count =1),
                         POSIXct = as.POSIXct(strsplit(basename(file),"[.]")[[1]][1], format = "%Y%m%d%H%M", tz = "UTC"))
    ncdf4::nc_close(nc)
    return(output)
}

###----------------- OPTIMIZATION -------------------###

# DDS parameter selection function

DDS.sel <- function(i, m, r, xnames, x_min, x_max, x_best) {

   # Set parameter set
   P_i <- 1-log(i)/log(m)
   sel <- c()
   for (d in 1:length(xnames)) {
      sel[d] <- sample(c(1,0), 1, prob=c(P_i, 1-P_i))
   }
   N <- xnames[as.logical(sel)]
   if (length(N) < 1) N <- sample(xnames, 1)

   # Set new values for selected parameters
   x_new <- x_best
   for (j in N) {
      xj_min <- x_min[[j]]
      xj_max <- x_max[[j]]
      xj_best <- x_best[[j]]
      sigj <- r * (xj_max - xj_min)
      x_new[[j]] <- xj_best + sigj*rnorm(1)
      if (x_new[[j]] < xj_min) {
         x_new[[j]] <- xj_min + (xj_min - x_new[[j]])
         if (x_new[[j]] > xj_max) {
            x_new[[j]] <- xj_min
         }
      }
      if (x_new[[j]] > xj_max) {
         x_new[[j]] <- xj_max - (x_new[[j]] - xj_max)
         if (x_new[[j]] < xj_min) {
            x_new[[j]] <- xj_max
         }
      }
   }
   x_new

}

###----------------- METRICS -------------------###

# RMSE
Rmse <- function (m, o, na.rm=TRUE) {
    err <- sum((m - o)^2, na.rm=na.rm)/(min(sum(!is.na(m)),sum(!is.na(o))))
    rmserr <- sqrt(err)
    rmserr
}

# Percent Bias
PBias <- function (m, o, na.rm=TRUE) {
    sum(m - o, na.rm=na.rm)/sum(o, na.rm=na.rm) * 100
}

# NSE
Nse <- function (m, o, nullModel=mean(o, na.rm=na.rm), na.rm=TRUE) {
    err1 <- sum((m - o)^2, na.rm=na.rm)
    err2 <- sum((o - nullModel)^2, na.rm=na.rm)
    ns <- 1 - (err1/err2)
    ns
}

# LogNSE
NseLog <- function (m, o, nullModel=mean(o, na.rm=na.rm), na.rm=TRUE) {
    m <- log(m + 1e-04)
    o <- log(o + 1e-04)
    err1 <- sum((m - o)^2, na.rm=na.rm)
    err2 <- sum((o - nullModel)^2, na.rm=na.rm)
    ns <- 1 - (err1/err2)
    ns
}

# Weighted NSE LogNSE
NseWt <- function (m, o, w=0.5, p=1) { 
    # NSE
    err1 <- sum((m - o)^2, na.rm=T)
    err2 <- sum((o - mean(o, na.rm=T))^2, na.rm=T)
    nse <- 1 - (err1/err2)
    # Ln NSE
    lnm <- log(m + 1e-04)
    lno <- log(o + 1e-04)
    err1 <- sum((lnm - lno)^2, na.rm=T)
    err2 <- sum((lno - mean(lno, na.rm=T))^2, na.rm=T)
    lnnse <- 1 - (err1/err2)
    # Weighted mean
    res <- ((w^p) * (nse^p) + (w^p) * (lnnse^p))^(1/p)
}

# KGE
Kge <- function (m, o, na.rm=TRUE, s.r=1, s.alpha=1, s.beta=1) {
  use <- if(na.rm) 'pairwise.complete.obs' else 'everything'
  r     <- cor(m, o, use=use)
  alpha <- sd(m, na.rm=na.rm) / sd(o, na.rm=na.rm)
  beta  <- mean(m, na.rm=na.rm) / mean(o, na.rm=na.rm)
  eds = sqrt( (s.r*(r-1))^2 + (s.alpha*(alpha-1))^2 + (s.beta*(beta-1))^2 )
  kges = 1-eds
}

# multi-scale objective function (MSOF)
# There is not limit on the number of scales to be considered
# The value of scales is defined as the number of time steps of data.
# For example, given daily data, scales=c(1,10,30) means calculate
# MSOF based on daily, 10-day, and 30-day time scales; if it is hourly
# data, scales=c(1,24,72,240,720) means calculate MSOF based on hourly,
# daily, 3-day, 10-day, and 30-day time scales

Msof <- function(m,o, scales=c(1,24))  {

   if (sum(scales<1)>0) stop("Scales (number of time steps) must not be less than 1!")

   ns <- length(scales)
   n1 <- length(m)
   n2 <- n1 - n1 %% scales
   sum0 <- 0
   for (i in 1:ns) {
     m1 <- m[1:n2[i]]; o1 <- o[1:n2[i]]
     if (scales[i]==1) {
        m2<-m1; o2<-o1
     } else {
        # compute model and observation at the prescribed scales
        m2 <- colMeans(matrix(m1,nrow=scales[i]),na.rm=TRUE)
        o2 <- colMeans(matrix(o1,nrow=scales[i]),na.rm=TRUE)
     }

     # remove missing values in the averaged time series
     # before computing objevtive function
     idx <- !is.na(m2) & !is.na(o2)
     m2 <- m2[idx]; o2 <- o2[idx]

     sum0 <- sum0 + sum((m2-o2)^2)*var(o, na.rm=TRUE)/var(o2)
   }
   obj <- sqrt(sum0)

}

hyperResMultiObj <- function(m, o, nullModel=mean(o, na.rm=na.rm), na.rm=TRUE) {
   # Input arguments:
   # m - Vector of modeled streamflow (cms)
   # o - Vector of observed streamflow (cms)
   # dt - Time step of model/obs in seconds

   # Establish weights for various metrics
   w0 = 0.4
   w1 = 0.2
   w2 = 0.4

   # First calculate the Normalized Nash Sutcliffe Efficiency (nnse)
   err1 <- sum((m - o)^2, na.rm=na.rm)
   err2 <- sum((o - nullModel)^2, na.rm=na.rm)
   nse <- 1 - (err1/err2)
   NNSE <- 1/(2-nse)

   # Second, calculate the peak discharge error
   Pe <- (max(m,na.rm=TRUE) - max(o,na.rm=TRUE)) / max(o,na.rm=TRUE)

   # Third, calculate the volume error. 
   # We are assuming streamflow is cubic meters / second....
   # Also assuming the passed in timestep of observations is in seconds.
   Ve <- sum((m - o), na.rm=TRUE) / sum(o, na.rm=TRUE)

   # Apply a weighting to calulate a final metric. 
   objMetric <- w0*(1.0 - NNSE) + w1*abs(Pe) + w2*abs(Ve)
   objMetric  

}

###----------------- PLOTTING -------------------###

# ggplot color palette
gg_color_hue <- function(n) {
   hues = seq(15, 375, length = n + 1)
   hcl(h = hues, l = 65, c = 100)[1:n]
 }

# Multiplot
# Multiple plot function
#
# ggplot objects can be passed in ..., or to plotlist (as a list of ggplot objects)
# - cols:   Number of columns in layout
# - layout: A matrix specifying the layout. If present, 'cols' is ignored.
#
# If the layout is something like matrix(c(1,2,3,3), nrow=2, byrow=TRUE),
# then plot 1 will go in the upper left, 2 will go in the upper right, and
# 3 will go all the way across the bottom.
#
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  library(grid)

  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)

  numPlots = length(plots)

  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                    ncol = cols, nrow = ceiling(numPlots/cols))
  }

 if (numPlots==1) {
    print(plots[[1]])

  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))

    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))

      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

#' Create and or name a list with its entries.
#' 
#' \code{NamedList} creates a list with names equal to its entries. 
#' @param theNames Vector to be coerced to character.
#' @return List with names equal to entries.
#' @examples 
#' NamedList(1:5)
#' @keywords manip
#' @export
NamedList <- function(theNames) {
  theList <- as.list(theNames)
  names(theList)<- theNames
  theList
}
