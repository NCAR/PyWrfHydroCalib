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

ReadChFile_Multi <- function(file) {
  chanobs <- GetNcdfFile(file, variables = c("feature_id", "streamflow"), quiet = TRUE)
  chanobs$POSIXct <-  as.POSIXct(strsplit(basename(file),"[.]")[[1]][1], format = "%Y%m%d%H%M", tz = "UTC")
  return(chanobs)
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

# Convert to daily value  

Convert2Daily <- function(str) {
   #str$Date <- rwrfhydro::CalcDateTrunc(str$POSIXct)
   str$Date <- CalcDateTrunc(str$POSIXct)
   setkey(str, Date)
   if ("q_cms" %in% names(str)) {
      str.d <- str[, list(q_cms=mean(q_cms, na.rm=TRUE)), by = c("Date", "site_no")]
   } else if ("obs" %in% names(str)) {
      str.d <- str[, list(obs=mean(obs, na.rm=TRUE)), by = c("Date", "site_no")]
   } else if ("mod" %in% names(str)) { # Xia added for SWE
      str.d <- str[, list(mod=mean(mod, na.rm=TRUE)), by = c("Date", "site_no")]
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
#    err1 <- sum((m - o)^2, na.rm=T)
#    err2 <- sum((o - mean(o, na.rm=T))^2, na.rm=T)
#    nse <- 1 - (err1/err2)
    # Ln NSE
#    lnm <- log(m + 1e-04)
#    lno <- log(o + 1e-04)
#    err1 <- sum((lnm - lno)^2, na.rm=T)
#    err2 <- sum((lno - mean(lno, na.rm=T))^2, na.rm=T)
#    lnnse <- 1 - (err1/err2)
 
    nse   <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=NULL, epsilon="Pushpalatha2012")
    lnnse <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=log, epsilon="Pushpalatha2012")   
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

#------------------ New metrics added from NWMv30 -----------------------------------

# May 27, 2020; E. Towler
# Use Lamontagne github Rcode: https://github.com/JRLamontagne/Efficiency/blob/master/functions3BT.R
# to create a deriviate R code to calculate r1, Stedinger's (1981) lognormal estimator. 
# These are calculated using the same input data as LBEm and LBE'm 
# (whereby if there are zeros, an epsilon is added)
# Line to be added to /glade/u/home/arezoo/wrf_hydro/PyWrfHydroCalib/core/calib_workflow.R 
# statCorR1 = r1(chrt.obj.nona.nozeros$mod, chrt.obj.nona.nozeros$obs)
# June12, 2020, added modification for Tau to be zero if (min(x) - tau <= 0) to avoid log(0) (happens with sites with many zeros)

r1 <- function(mod, obs){
   S <- mod#data[,2]   # Specify similuations
   O <- obs#data[,1]   # Specify observations
   # Get sample size of station data (how many observed and PRMS simulated streamflow data)
   n <- length(obs)#nrow(data)
   
   # Calculate correlation coefficient (calculate both r and r*=r1)
   r <- cor(S,O)   # Calculate correlation coefficient (rho) between simulations and observations

   
   ############## LBE ##############
   # Eq. 16
   # Calculate tau for observations and simulations
   if ((min(O)+max(O)-2*median(O))>0){
     tau_O <- (min(O)*max(O)-median(O)^2)/(min(O)+max(O)-2*median(O))     # Compute tau_O for each MC trial
   } else {
     tau_O <- 0   # Fit LN2 model instead of LN3 because Stedinger (1980) lower bound estimator cannot be computed
   }
   
   if ((min(S)+max(S)-2*median(S))>0){
     tau_S <- (min(S)*max(S)-median(S)^2)/(min(S)+max(S)-2*median(S))     # Compute tau_S for each MC trial
   } else {
     tau_S <- 0   # Fit LN2 model instead of LN3 because Stedinger (1980) lower bound estimator cannot be computed
   }
   
   #### ADDED 11.23.2019 to be consistent with Vogel's suggestion (11/1/2019) if tau values are negative set tau to zero. This means fitting LN2 at these sites.
   if (tau_O<0 | tau_S<0){
     tau_O <- 0
     tau_S <- 0
   }
   
   # New added by Erin June 12, 2020, to deal with sites with many zeros, where tau ends up being min(O) or min(S); avoids log 0.
   if ( (min(O) - tau_O <= 0) | (min(S) - tau_S <= 0) ) {
     tau_O = 0 # As above, this means fitting LN2 at these sites.
     tau_S = 0 # As above, this means fitting LN2 at these sites.
   }
   
   u <- log(O-tau_O)      # Compute u based on observations of LN2 to use for LBE equation
   v <- log(S-tau_S)      # Compute v based on observations of LN2 to use for LBE equation
 
   # Estimator of correlation coefficient, r* (r1),  using Stedinger (1981) eqn 2
   # Stedinger (1981) eqn 3: estimator of variance of log of observations and simulations (CHECK!!!)
   s2_yOyS <- 1/n*sum((u-mean(u))*(v-mean(v))) 
   s2_yO <- 1/n*sum((u-mean(u))^2)   # Stedinger (1981) eqn 3: estimator of variance of log of observations
   s2_yS <- 1/n*sum((v-mean(v))^2)    # Stedinger (1981) eqn 3: estimator of variance of log of simulations
   r1 <- (exp(s2_yOyS)-1)/sqrt((exp(s2_yO)-1)*(exp(s2_yS)-1)) # Stedinger (1981) eqn 2: estimator of correlation coefficient, r* (r1)
   return(r1)
}

#Function that adds an epsilon to entire distribution if there are zeros
# Based on hydroGOF library, for NSE(... FUN=log, epsilon="Pushpalatha2012") based on:
# Pushpalatha, R., Perrin, C., Le Moine, N. and Andreassian, V. (2012).
# A review of efficiency criteria suitable for evaluating low-flow simulations.
# Journal of Hydrology, 420, 171-182. DOI: 10.1016/j.jhydrol.2011.11.055
# If there are no zeros in the distribution, returns original dataframe.
noZeroFunction = function(mod, obs, period){
  zmin = min(mod, obs, na.rm = T) # Xia added na.rm=T on 2020/06/17
  if (zmin ==0) {
    #Following Push2012, though I don't get identical results, they are very close
    epsilon = mean(obs, na.rm=T)/100
    obs = obs + epsilon
    mod = mod + epsilon
  } # end if (zmin =0)
  df = data.table::data.table(q_cms = mod, obs = obs, period = period)  # added library before function by Xia on 20200616
  return(df)
} # end function


# May 27, 2020; E. Towler
# Use Lamontagne github Rcode: https://github.com/JRLamontagne/Efficiency/blob/master/functions3BT.R
# to create a deriviate R code to calculate NSE, logNSE, and KGE. 
# These are re-calculated here so we use the same input data to compare with LBEm and LBE'm 
# (since if there are zeros, we add an epsilon, so these will change slightly)

# Line to be added to /glade/u/home/arezoo/wrf_hydro/PyWrfHydroCalib/core/calib_workflow.R 
#statNSEs = NSEs_noZeros(chrt.obj.nona.nozeros$mod, chrt.obj.nona.nozeros$obs)

# Function to be added to /glade/u/home/arezoo/wrf_hydro/PyWrfHydroCalib/core/calib_utils.R
NSEs_noZeros <- function(mod, obs, w=0.5, p=1) { # Function reads in paired model and obs without any NAs.
  
  # Calculate NSE and logNSE in here, so we are positive we are using the exact same estimates for zeros
  S <- mod   # Specify similuations
  O <- obs   # Specify observations
  # Get sample size of station data (how many observed and PRMS simulated streamflow data)
  n <- length(O)
  
  ############## NSE ##############
  # Nash-Sutcliffe Efficiency (NSE)                              # Xia: n-1 instead of n in denomitor as in standard NSE on 20200616
  NSE <- 1-((1/n)*sum((S-O)^2))/((1/(n-1))*sum((O-mean(O))^2))   # Barber et al. 2019 eqn. 8b
  
  # Compute NSE using natural logs. Call this LogNSE
  LogNSE <- 1-((1/n)*sum((log(S)-log(O))^2))/((1/(n-1))*sum((log(O)-mean(log(O)))^2))

  # Compute weighted NSE
  wtNSE <- ((w^p) * (NSE^p) + (w^p) * (LogNSE^p))^(1/p)
  
  ############## KGE ##############
  alpha <- sd(S)/sd(O)        # Estimator of alpha = estimator of theta = std. dev. simulations / std. dev. observations
  beta <- mean(S)/mean(O)
  # Calculate correlation coefficient (calculate both r and r*=r1)
  r <- cor(S,O)   # Calculate correlation coefficient (rho) between simulations and observations
  KGE <- 1-sqrt((beta-1)^2+(alpha-1)^2+(r-1)^2)   # Barber et al. 2019 eqn 14
  
  NSEs = c(NSE, LogNSE, wtNSE, KGE)
  return(NSEs)
} #end   
  
# May 27, 2020; E. Towler
# Use Lamontagne github Rcode: https://github.com/JRLamontagne/Efficiency/blob/master/functions3BT.R
# to create a deriviate R code to calculate BNL3 parameters.
# This is called by the LBEms_function
# June12, 2020, added modification for Tau to be zero if (min(x) - tau <= 0) to avoid log(0) (happens with sites with many zeros)
#               and a jitter to the obs and mod if ther variance for a particular month is zero. (i.e., all values are zero or same value in a month)

parms <- function(data){
  S <- data[,2]   # Specify similuations
  O <- data[,1]   # Specify observations
  
  # New added by Erin June 12, 2020. For some sites, an entire month may be zeros
  # (or same value for entire month), which leads to var=0. Add small jitter. 
  # Sometimes the first time run, a warning is issued, but does not seem to affect anything.
  # Warning message:
  # In stats::runif(length(x), -amount, amount) :
  #  '.Random.seed[1]' is not a valid integer, so ignored
  if (var(O)==0 | var(S)==0) {
    O = jitter(O)
    S = jitter(S)
  }
  
  # Get sample size of station data (how many observed and PRMS simulated streamflow data)
  n <- nrow(data)
  
  mu_O <- mean(O)  # Calculate mean of observations,mu_O (mean of column 1)
  
  ############## LBE ##############
  # Eq. 16
  # Calculate tau for observations and simulations
  if ((min(O)+max(O)-2*median(O))>0){
    tau_O <- (min(O)*max(O)-median(O)^2)/(min(O)+max(O)-2*median(O))     # Compute tau_O for each MC trial
  } else {
    tau_O <- 0   # Fit LN2 model instead of LN3 because Stedinger (1980) lower bound estimator cannot be computed
  }
  
  if ((min(S)+max(S)-2*median(S))>0){
    tau_S <- (min(S)*max(S)-median(S)^2)/(min(S)+max(S)-2*median(S))     # Compute tau_S for each MC trial
  } else {
    tau_S <- 0   # Fit LN2 model instead of LN3 because Stedinger (1980) lower bound estimator cannot be computed
  }
  
  #### ADDED 11.23.2019 to be consistent with Vogel's suggestion (11/1/2019) if tau values are negative set tau to zero. This means fitting LN2 at these sites.
  # If can't reproduce past results comment this out
 if (tau_O<0 | tau_S<0){
    tau_O <- 0
    tau_S <- 0
  }
  
  # New added by Erin June 12, 2020, to deal with sites with many zeros, where tau ends up being min(O) or min(S); avoids log 0.
  #if ( (min(O) - tau_O <= 0) | (min(S) - tau_S <= 0) ) { # commented out by Xia
  if ( (min(O) - tau_O <= 0.0000000001) | (min(S) - tau_S <= 0.0000000001) ) { # Erin: New June 17, 2020, when there are many zeros, sometimes this is just barely positive, but TauO should be zero in that case 
    tau_O = 0 # As above, this means fitting LN2 at these sites.
    tau_S = 0 # As above, this means fitting LN2 at these sites.
  }
  
  u <- log(O-tau_O)      # Compute u based on observations of LN2 to use for LBE equation
  mu_u <- mean(u)
  var_u <- (1/(n-1))*sum((u-mean(u))^2)   # Population variance of 
  sd_u <- sqrt(var_u)
  v <- log(S-tau_S)      # Compute v based on observations of LN2 to use for LBE equation
  mu_v <- mean(v)
  var_v <- (1/(n-1))*sum((v-mean(v))^2)   # Population variance of v
  sd_v <- sqrt(var_v)
  
  rho_log <- cor(u,v)
  
  # Estimator of correlation coefficient, r* (r1),  using Stedinger (1981) eqn 2
  # Stedinger (1981) eqn 3: estimator of variance of log of observations and simulations (CHECK!!!)
  s2_yOyS <- 1/n*sum((u-mean(u))*(v-mean(v))) 
  s2_yO <- 1/n*sum((u-mean(u))^2)   # Stedinger (1981) eqn 3: estimator of variance of log of observations
  s2_yS <- 1/n*sum((v-mean(v))^2)    # Stedinger (1981) eqn 3: estimator of variance of log of simulations
  r1 <- (exp(s2_yOyS)-1)/sqrt((exp(s2_yO)-1)*(exp(s2_yS)-1)) # Stedinger (1981) eqn 2: estimator of correlation coefficient, r* (r1)
  
  # LN3 estimators
  Co_LBE <- (sqrt(exp(2*mean(u)+var_u)*(exp(var_u)-1)))/(tau_O+exp(mean(u)+var_u/2)) # LBE estimate of coefficient of variation of observations
  theta_LBE <- sqrt((exp(2*mean(v)+var_v)*(exp(var_v)-1))/(exp(2*mean(u)+var_u)*(exp(var_u)-1))) # LBE estimate of theta
  delta_LBE <- 1-(tau_S+exp(mean(v)+var_v/2))/(tau_O+exp(mean(u)+var_u/2)) # LBE estimate of delta
  
  LBE <- 2*theta_LBE*r1-theta_LBE^2-delta_LBE^2/Co_LBE^2 # Barber et al. 2019 eq. 16
  LBEprime <- 1-sqrt((-1*delta_LBE)^2+(theta_LBE-1)^2+(r1-1)^2)
  
  LN3parms <- cbind(mu_O, r1, Co_LBE, delta_LBE, theta_LBE, tau_O, tau_S, mu_u, mu_v, sd_u, sd_v, rho_log)
  
  return(LN3parms)
}


# May 19, 2020; E. Towler
# Use Lamontagne github Rcode  https://github.com/JRLamontagne/Efficiency/blob/master/LBEm.R
# to create a deriviate R code to calculate âLBEmâ and "LBE'm", 
# From Lamontagne, J. R., Barber C, Vogel RM (in review). 
# Improved Estimators of Model Performance Efficiency for Skewed Hydrologic Data, 
# Water Resources Research. 
# June 12, 2020. Generalized the code for cases where there is not any data in a month (e.g., frozen rivers in Jan/Feb.)


# Line to be added to /glade/u/home/arezoo/wrf_hydro/PyWrfHydroCalib/core/calib_workflow.R 
#statLBEms = LBEms_function(chrt.obj.nona.nozeros$mod, chrt.obj.nona.nozeros$obs, chrt.obj.nona.nozeros$period)

# Function to be added to /glade/u/home/arezoo/wrf_hydro/PyWrfHydroCalib/core/calib_utils.R
LBEms_function <- function(mod, obs, period, calcDailyStats) { # Function reads in paired model and obs without any NAs. 
                                                               # Xia added calcDailyStats which is provided in proj_data.Rdata on 20200617
  # Remove NA
  id<-!is.na(mod) &!is.na(obs); mod <- mod[id]; obs <- obs[id]; period<-period[id] # Xia on 20200617
  
  allData = data.frame("O"=obs, "S"= mod, "month" = period)
  
  mu_O_month <- matrix(nrow=1, ncol=12)
  Co_month <- matrix(nrow=1, ncol=12)
  delta_month <- matrix(nrow=1, ncol=12)
  theta_month <- matrix(nrow=1, ncol=12)
  rho_month <- matrix(nrow=1, ncol=12)
  tau_O_month<- matrix(nrow=1, ncol=12)
  tau_S_month<- matrix(nrow=1, ncol=12)
  mu_u_month<- matrix(nrow=1, ncol=12)
  mu_v_month<- matrix(nrow=1, ncol=12)
  sd_u_month<- matrix(nrow=1, ncol=12)
  sd_v_month<- matrix(nrow=1, ncol=12)
  rho_log_month<- matrix(nrow=1, ncol=12)
  # mixture moment estimators
  mu_mix_O_month<- matrix(nrow=1, ncol=12)
  var_mix_O_month<- matrix(nrow=1, ncol=12)
  mu_mix_S_month<- matrix(nrow=1, ncol=12)
  var_mix_S_month<- matrix(nrow=1, ncol=12)
  mu_mix_SO_month<- matrix(nrow=1, ncol=12)
  mu_u_mix<- matrix(nrow=1, ncol=12)
  mu_v_mix<- matrix(nrow=1, ncol=12)
  tau_O_mix<- matrix(nrow=1, ncol=12)
  tau_S_mix<- matrix(nrow=1, ncol=12)
  sd_u_mix<- matrix(nrow=1, ncol=12)
  sd_v_mix<- matrix(nrow=1, ncol=12)
  mu_mix_O_month_MC<- matrix(nrow=1, ncol=12)
  var_mix_O_month_MC_PART<- matrix(nrow=1, ncol=12)
  mu_mix_S_month_MC<- matrix(nrow=1, ncol=12)
  var_mix_S_month_MC_PART<- matrix(nrow=1, ncol=12)
  mu_mix_SO_month_MC<- matrix(nrow=1, ncol=12)
  rho_mix<- matrix(nrow=1, ncol=12)
  
  mu_mix_O_MC <- matrix(nrow=1, ncol=1)
  var_mix_O_MC <- matrix(nrow=1, ncol=1)
  mu_mix_S_MC <- matrix(nrow=1, ncol=1)
  var_mix_S_MC <- matrix(nrow=1, ncol=1)
  mu_mix_SO_MC <- matrix(nrow=1, ncol=1)
  theta_mix_MC <- matrix(nrow=1, ncol=1)
  delta_mix_MC <- matrix(nrow=1, ncol=1)
  Co_mix_MC <- matrix(nrow=1, ncol=1)
  r_mix_MC <- matrix(nrow=1, ncol=1)
  LBE_mix_MC <- matrix(nrow=1, ncol=1)
  LBEprime_mix_MC <- matrix(nrow=1, ncol=1)
  mu_mix_O <- matrix(nrow=1, ncol=1)
  var_mix_O <- matrix(nrow=1, ncol=1)
  mu_mix_S <- matrix(nrow=1, ncol=1)
  var_mix_S <- matrix(nrow=1, ncol=1)
  
  mu_mix_SO <- matrix(nrow=1, ncol=1)
  
  theta_mix <- matrix(nrow=1, ncol=1)
  delta_mix <- matrix(nrow=1, ncol=1)
  Co_mix <- matrix(nrow=1, ncol=1)
  Cs_mix <- matrix(nrow=1, ncol=1)
  r_mix <- matrix(nrow=1, ncol=1)
  LBE_mix <- matrix(nrow=1, ncol=1)
  LBEprime_mix <- matrix(nrow=1, ncol=1)
  rho <- matrix(nrow=1, ncol=1)

  if (!calcDailyStats) monsample <- 720  else monsample <- 30  # Xia: need at least 1-yr sample in each month for calibration at hourly (720) or monthly (30) scale on 20200617
  for (m in 1:12){ # For calculations based on monthly data
    oneSite_month <- subset(allData, allData$month== m) # Pull data for individual site's month
    if (nrow(oneSite_month) > monsample ) { # added this line - Xia pointed out case where there's missing data for one month
      LN3params_month <- parms(oneSite_month[,1:2])  # Xia: 3-yr sample limit for 0 as we discussed? on 20200616
      mu_O_month[m] <- LN3params_month[1] # real space mean
      rho_month[m] <- LN3params_month[2]
      Co_month[m] <- LN3params_month[3]
      delta_month[m] <- LN3params_month[4]
      theta_month[m] <- LN3params_month[5]
      tau_O_month[m] <- LN3params_month[6]
      tau_S_month[m] <- LN3params_month[7]
      mu_u_month[m] <- LN3params_month[8]
      mu_v_month[m] <- LN3params_month[9]
      sd_u_month[m] <- LN3params_month[10]
      sd_v_month[m] <- LN3params_month[11]
      rho_log_month[m] <- LN3params_month[12]
    
      # Per Vogel's suggestion (11/1/2019) if tau values are negative set tau to zero. This means fitting LN2 at these sites.
      if (tau_O_month[m]<0 | tau_S_month[m]<0){
        tau_O_month[m] <- 0
        tau_S_month[m] <- 0
        LN2count_month <- LN2count_month + 1
      }
    
      # mixture moment estimators
      mu_mix_O_month[m] <- tau_O_month[m]+exp(mu_u_month[m]+sd_u_month[m]^2/2)
      var_mix_O_month[m] <- (exp(2*mu_u_month[m]+sd_u_month[m]^2)*(exp(sd_u_month[m]^2)-1))
      mu_mix_S_month[m] <- tau_S_month[m]+exp(mu_v_month[m]+sd_v_month[m]^2/2)
      var_mix_S_month[m] <- (exp(2*mu_v_month[m]+sd_v_month[m]^2)*(exp(sd_v_month[m]^2)-1))
      mu_mix_SO_month[m] <- (mu_mix_S_month[m]*mu_mix_O_month[m]+rho_month[m]*sqrt(var_mix_S_month[m])*sqrt(var_mix_O_month[m]))
    } # end if (nrow(oneSite_month) > 0 ) { # added this line.
  } # end for (m in 1:12){ # For calculations based on monthly data
  
  # mixture moments from RAW data  - June 12, edited these to be mean( , na.rm =T), instead of 1/12*sum
  mu_mix_O  <- mean(mu_mix_O_month, na.rm =T)
  var_mix_O <- mean((var_mix_O_month+mu_mix_O_month^2), na.rm=T)-mu_mix_O^2
  mu_mix_S  <- mean(mu_mix_S_month, na.rm=T)
  var_mix_S <- mean((var_mix_S_month+mu_mix_S_month^2),na.rm=T)-mu_mix_S^2
  
  mu_mix_SO <- mean(mu_mix_SO_month, na.rm=T)
  
  theta_mix <- sqrt(var_mix_S)/sqrt(var_mix_O)
  delta_mix <- 1-mu_mix_S/mu_mix_O
  Co_mix <- sqrt(var_mix_O)/mu_mix_O
  Cs_mix <- sqrt(var_mix_S)/mu_mix_S
  #r1_mix[i] <- (1/nrow(oneSite)*sum(oneSite[,3]*oneSite[,4])-mu_mix_O[i]*mu_mix_S[i])/(sqrt(var_mix_O[i]*var_mix_S[i]))
  r_mix <- (mu_mix_SO-mu_mix_O*mu_mix_S)/(sqrt(var_mix_O*var_mix_S))
  
  LBE_mix <- 2*theta_mix*r_mix-theta_mix^2-delta_mix^2/Co_mix^2
  LBEprime_mix <- 1-sqrt(delta_mix^2+(theta_mix-1)^2+(r_mix-1)^2)
  LBEms = c(LBE_mix, LBEprime_mix)
  return(LBEms)
} #end   


# calculating the contigency metrics
  
#abcd <- calc_abcd1(data.frame(pairedData),headerCols=c('site_no'),
#                   threshColName = ss,obsColName = "q_cms_obs",modColName = "q_cms_mod")

calc_abcd1 = function (pairedData, threshColName, obsColName = "streamflow_obs", 
          modColName = "streamflow_mod", headerCols = c("feature_id", 
                                                       "inittime", "validtime", "leadtime"), allowNA = FALSE) {
  pairedData <- pairedData[, c(headerCols, obsColName, modColName, 
                               threshColName)]
  if (nrow(pairedData) == 0) {
    stop("pairedData has no rows")
  }
  if (!allowNA) {
    if ((obs.na <- any(is.na(pairedData[obsColName]))) | 
        (mod.na <- any(is.na(pairedData[modColName]))) | 
        (thresh.na <- any(is.na(pairedData[threshColName])))) {
      if (obs.na) 
        warning("CalcAbcd1: Missing observations removed in supplied data.table.")
      if (mod.na) 
        warning("CalcAbcd1: Missing model values removed in supplied data.table. ")
      if (thresh.na) 
        warning("CalcAbcd1: Missing threshold values removed in supplied data.table.")
    }
    pairedData <- pairedData[complete.cases(pairedData[c(obsColName, 
                                                         modColName, threshColName)]), ]
  }
  pairedData$obsAboveThresh <- ifelse(pairedData[obsColName] > 
                                        pairedData[threshColName], 1, 0)
  pairedData$modAboveThresh <- ifelse(pairedData[modColName] > 
                                        pairedData[threshColName], 1, 0)
  dt <- data.table::as.data.table(pairedData)
  dt[, `:=`(a1 = as.integer(obsAboveThresh & modAboveThresh), 
            b1 = as.integer((!obsAboveThresh) & modAboveThresh), 
            c1 = as.integer(obsAboveThresh & (!modAboveThresh)), 
            d1 = as.integer((!obsAboveThresh) & (!modAboveThresh))), 
     ]
  retDf <- dt[, setdiff(names(dt), c(obsColName, modColName)), 
              with = FALSE]
  retDf <- as.data.frame(retDf)
  retDf$threshName <- threshColName
  names(retDf)[names(retDf) == threshColName] = "threshValue"
  retDf <- retDf[c(headerCols, "threshName", "threshValue", 
                   "modAboveThresh", "obsAboveThresh", "a1", "b1", "c1", 
                   "d1")]
  return(retDf)
}

#contStats <- calc_contingency_stats(abcd, groupVars = c("site_no", "threshName"))

calc_contingency_stats = function (abcd1DF, groupVars = c("feature_id", "threshName"), 
          na.rm = TRUE, aggFun = "sum", returnStats = c("POD", "FAR", "CSI")) {
  if (!("threshName" %in% groupVars)) {
    warning("threshName not in specified groupVars. Stats may be calculated inappropriately accross multiple thresholds")
  }
  aggFun <- match.fun(aggFun)
  abcd1DF <- data.table::as.data.table(abcd1DF)
  if (na.rm) {
    abcd1DF[, `:=`(isNa = is.na(a1 * b1 * c1 * d1))]
    abcd1DF <- abcd1DF[isNa == FALSE, ]
  }
  if (is.null(groupVars)) {
    abcd1DF <- abcd1DF[, .(a = aggFun(a1), b = aggFun(b1), 
                           c = aggFun(c1), d = aggFun(d1), n = .N)]
  }else {
    abcd1DF <- abcd1DF[, .(a = aggFun(a1), b = aggFun(b1), 
                           c = aggFun(c1), d = aggFun(d1), n = .N), by = groupVars]
  }
  checkSum <- all(abcd1DF[, .(check = a + b + c + d == n)]$check)
  if (!checkSum) 
    warning("a+b+c+d", ifelse(checkSum, "does", "does NOT"), 
            "equal n in all cases. If sum is used,\n  then this should be OK (unless NAs were passed). Experimental aggrecation functions \n  may not yield the sum.")
  abcd1DF[, `:=`(PC = (a + d)/(n), 
                 POD = a/(a + c), 
                 FAR = b/(a + b), 
                 CSI = a/(a + b + c), 
                 POFD = b/(b + d),#bias = (a + b)/(a + c), # AREZOO --> commenting this one out as there is a bias term in the R code, if you want this then you need to change the name
                 oddsRatio = (a * d)/(b * c), 
                 HeidkeSS = (2 * (a * d - b * c))/((a + c) * (c + d) + (a + b) * (b + d)), 
                 PierceSS = (a * d - b * c)/((a + c) * (b + d)), 
                 ClaytonSS = (a * d - b * c)/((a + b) * (c + d)), 
                 GilbertSS = (a - ((a + b)/(a + c)/n))/(a - ((a + b)/(a + c)/n) + b + c), 
                 EDS = 2 * log((a + c)/n)/log(a/n) - 1, 
                 SEDS = (log((a + b)/n) + log((a + c)/n))/log(a/n) - 1, Q = (a * d - b * c)/(a * d + b * c)), ]
  return(abcd1DF)
}

### Xia added NNseSq, NseLogM and NseWtM on 20200616.

# NNSE
NNse <- function (m, o) {
    nse <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=NULL, epsilon=0)
    nnse <- 1/(2-nse)
}

# NNSE on squared streamflow 
NNseSq <- function (m, o) {
    nse <- hydroGOF::NSE(m^2, o^2, na.rm=TRUE, FUN=NULL, epsilon=0)
    nnse <- 1/(2-nse)
}

# Modifed LogNSE  
NseLogM <- function (m, o) {
    # Add 1/100 of mean observed flow to all values if zero flow occurs
    if (min(m, o, na.rm=T)==0) {
      lnse <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=log, epsilon="Pushpalatha2012")
    }else{
      lnse <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=log)
    }
}

# Modifed NseWt  
NseWtM <- function (m, o, w=0.5, p=1) {
    # Add 1/100 of mean observed flow to all values if zero flow occurs
    if (min(m, o, na.rm=T)==0) {
      nse   <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=NULL, epsilon="Pushpalatha2012")
      lnnse <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=log, epsilon="Pushpalatha2012")
    }else{
      nse   <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=NULL)
      lnnse <- hydroGOF::NSE(m, o, na.rm=TRUE, FUN=log)
    }
    # Weighted mean
    res <- ((w^p) * (nse^p) + (w^p) * (lnnse^p))^(1/p)
}

# Event identification for hourly streamflow (model or observation) (written by Yuqiong) 
eventIdentification <- function(data, snowy=FALSE, slow=FALSE,threshPeak,threshold_prob=FALSE,nhourCompound=-1) {

# input arguments
# data: data.frame for streamflow, 1st column is time in POSIXct format,
#       2nd column is the flow values
# snowy: logical, whether the basin is dominated by SEASONAL snow
# slow: logical, whether the basin is a slow-response (e.g.,groundwater-deriven) basin
# threshPeak: threshold for event peaks; peaks below threshold are
#       disgarded
# threshold_prob: logical varible to indicate whether threhPeak
#       is climatological probabilities or the actual values in units of
#       streamflow data
# nhourCompound: max distance (hours) for construct compound events
#       set to a number >=0 if compound events are desired. For example, 
#       if nhourCompound=2, those events next to each other with a distance
#       <=2 hours are combined into a compound event.

# parameters 
nwinSpan=1.5*24
nwinDecay=30*24
minEventDist=24
minRiseDuration=2
minRecessionDuration=6
if (snowy) {
  nwinSpan=10*24
  nwinDecay=90*24
  minRiseDuration=24*5
  minRecessionDuration=24*5
}
if (slow) {
  nwinSpan=10*24
  nwinDecay=60*24
  minRiseDuration=24*2
  minRecessionDuration=24*5
}
minEventDuration=minRiseDuration + minRecessionDuration

data <- na.omit(data)
names(data) <- c("time","value")
if (threshold_prob) {
thresh1 <- quantile(data$value, threshPeak)
} else {
thresh1 <- threshPeak
}
thresh2 <- quantile(data$value, 0.5) #median flow

# identify data gaps and break into chunks with no missing data
dates <- seq(min(data$time),max(data$time), by="hour")
dates1 <- dates[!dates %in% data$time]
chunks <- match(dates1, dates)
nchunk <- length(chunks)+1

# loop through chunks to identy peaks for each chunk and then put them back together
dataAll <- eventsAll <- data.table::data.table()
for (i1 in 1:nchunk) {

   # start index of current non-missing period
   if (i1==1) { j1 <- 1
   } else { j1 <- chunks[i1-1]+1 }

   # end index of current non-missing period
   if (i1==nchunk) { j2 <- length(dates)
   } else { j2 <- chunks[i1]-1 }

   if (j1>j2) next
   if (length(j1:j2) < 6) next 

   # print(i1)

   # data for current chunk
   data2 <- subset(data, time %in% dates[j1:j2])
   if (max(data2$value) < thresh1) next

   #local weighted regression smoothing
   data2$hour <- 1:nrow(data2)
   span <- nwinSpan/nrow(data2)
   fit <- loess(value ~ hour, degree=1,span = span, data=data2)
   data2$smooth <- fit$fitted

   # identify peaks in smoothed data
   d1 <- c(NA, diff(data2$smooth))
   ipeak <- NULL
   if (d1[2]<0) ipeak <- c(ipeak,1)
   for (i2 in 2:(nrow(data2)-1))
     if (d1[i2]>=0 & d1[i2+1]<=0) ipeak <- c(ipeak,i2)
   
   if (length(ipeak)==0) next

   # construct initial events
   events0 <- data.table::data.table()
   for (i2 in 1:length(ipeak)) {


     t2 <- ipeak[i2]
     if (t2==1 | t2==nrow(data2)) next

     # end point (on smoothed data)
     ix2 <- t2+nwinDecay
     if (ix2>nrow(data2)) ix2 <- nrow(data2)
     x1 <- data2$smooth[t2:ix2]
     x2 <- diff(x1)
     x3 <- zoo::rollsum(x2,6,align="left")
     ix3 <- which(x3[-(1:minRecessionDuration)] > -0.01)
     if (length(ix3)>0) {
       end1 <- data2$time[t2+min(ix3)+minRecessionDuration]
     } else if (ix2==nrow(data2)){
       end1 <- data2$time[nrow(data2)]
     } else {
       next
     } 

     # start point (on smoothed data)
     ix2 <- t2-nwinDecay
     if (ix2<1) ix2 <- 1
     x1 <- data2$smooth[seq(t2,ix2,-1)]
     x2 <- diff(x1)
     x3 <- zoo::rollsum(x2,6,align="right")
     ix3 <- which(x3[-(1:minRiseDuration)] > -0.01)
     if (length(ix3)>0) {
       start1 <- data2$time[t2-(min(ix3)+minRiseDuration)]
     } else if (ix2==1) {
       start1 <- data2$time[1]
     } else {
       next
     }
     if (nrow(events0)>=1) 
       if (start1 < events0$end[nrow(events0)]) 
         start1 <- events0$end[nrow(events0)]
  
     # peak point (on original data) 
     dt1 <- subset(data2, time>=start1 & time<=end1)
     peak1 <- dt1$time[which.max(dt1$value)]

     # adjust start and end points (on original data)
     start2 <- start1
     if ((peak1-3600*4)>start1) { 
     times <- seq(start1,peak1-3600,by="hour")
     for (k1 in 1:length(times)) {
       t1 <- times[k1]
       v1 <- subset(data2,time>=t1 & time<=t1+(3*3600))$value
       m1 <- mean(v1)
       r1 <- max(v1)-min(v1)
       sd1 <- sd(v1)
       nsd1 <- sd1/m1
       if (is.na(nsd1)) break
       if (nsd1>0.1) break
     }
     if (k1<length(times)) start2 <- t1
     }

     #print(paste0("i1:",i1," i2:",i2))
     end2 <- end1
     if (end1 > (peak1+3600*4)) {
     times <- rev(seq(peak1+3600,end1,by="hour"))
     for (k1 in 1:length(times)) {
       t1 <- times[k1]
       v1 <- subset(data2,time<=t1 & time>=t1-(3*3600))$value
       #print(paste0("k1: ",k1))
       #print(v1)
       m1 <- mean(v1)
       r1 <- max(v1)-min(v1)
       sd1 <- sd(v1)
       nsd1 <- sd1/m1
       if (is.na(nsd1)) break
       if (nsd1>0.05) break
     }
     if (k1<length(times)) end2 <- t1
     }

     # add to event data frame 
     events0 <- rbind(events0,data.table::data.table(start=start2,peak=peak1,end=end2))
   }
   if (nrow(events0)==0) next

   # compute event duration 
   events0[,nhour:=as.integer(difftime(end,start,units="hour"))+1]
   events0[,nrise:=as.integer(difftime(peak,start,units="hour"))+1]
   events0[,nrece:=as.integer(difftime(end,peak,units="hour"))]

   # remove spurious events
   events0 <- subset(events0, nhour>=6)
   if (nrow(events0)==0) next

   # adjust end points based on start point of next event
   if (nrow(events0)>1) {
   for (i2 in 1:(nrow(events0)-1)) {
     iend <- match(events0$end[i2], data2$time)
     istart <- match(events0$start[i2+1], data2$time)
     if (iend>istart | ((istart-iend)<=10 & data2$value[istart]<data2$value[iend]))
       events0$end[i2] <- data2$time[istart]
   }}

   # discard events at the start or end of periods with data missing
   # if the flow at start/end point is below median flow (thresh2)
   #ix1 <- match(events0$start,data2$time)
   #ix2 <- which(ix1 != 1 & !is.na(data2$value[ix1-1]))
   #ix2 <- ix2 | (ix1==1 & data2$value[ix1]<=thresh2)
   #events0 <- events0[ix2,]
   #ix1 <- match(events0$end,data2$time)
   #ix2 <- which(ix1 != nrow(data2) & !is.na(data2$value[ix1+1]))
   #ix2 <- ix2 | (ix1==nrow(data2) & data2$value[ix1]<=thresh2)
   #events0 <- events0[ix2,]

   # discard events where peak is at the start or end (due to missing data)
   events0 <- subset(events0, peak!=start & peak!=end)
   if (nrow(events0)==0) next

   #print(paste0(i1," ",nrow(events0)))

   # merge if duplicated start/peak/end
   events0 <- events0[!duplicated(events0),]
   for (tag1 in c("start","peak","end")) {
     dt1 <- subset(data.table::as.data.table(table(events0[[tag1]])),N>1)
     dt1$V1 <- as.POSIXct(dt1$V1, format="%Y-%m-%d %H:%M:%S")
     if (nrow(dt1)>0) {
       for (t1 in dt1$V1) {
         events1 <- subset(events0, get(tag1) == t1)
         events0 <- subset(events0,! get(tag1) %in% t1)
         tmp <- data.table::data.table(start=min(events1$start),
           peak=events1$peak[which.max(data2$value[match(events1$peak,data2$time)])],
           end=max(events1$end))
         tmp[,nhour:=as.integer(difftime(end,start,units="hour"))+1]
         tmp[,nrise:=as.integer(difftime(peak,start,units="hour"))+1]
         tmp[,nrece:=as.integer(difftime(end,peak,units="hour"))]

         events0 <- rbind(events0,tmp) 
   }}}

   # combine events under the following conditions
   # 1. duration too short (rising limb + recession limb),
   #    when nearby event exists (otherwise remove event)
   # 2. peak too close to neighbor event peak
   # 3. event peak falls inside a neighbor event period
   # 4. event too small 
   # 5. one-legged event

   events1 <- data.frame()
   while(nrow(events0)>1) {

   events0 <- events0[order(start),]

   # 1. duration too short
   ix1 <- which(events0$nhour < minEventDuration)
   # 2. peak too close
   ix1 <- c(ix1, which(as.numeric(diff(events0$peak),"hours") < minEventDist)+1)
   # 3. peak inside neighbor event
   ne1 <- nrow(events0)
   peak1 <- events0$peak[1:(ne1-1)]
   start1 <- events0$start[2:ne1]
   peak2 <- events0$peak[2:ne1]
   end2 <- events0$end[1:(ne1-1)]
   ix1 <- c(ix1, which(peak1>=start1), which(peak2<=end2))
   # 4. event too small (based on smoothed data) 
   s2 <- data2$smooth[match(events0$start,data2$time)]
   p2 <- data2$smooth[match(events0$peak,data2$time)]
   e2 <- data2$smooth[match(events0$end,data2$time)]
   h1s <- p2-s2 #rising limb height
   h2s <- p2-e2 #recession limb height
   ix1 <- c(ix1, which(ifelse(h1s<=h2s,h1s,h2s)<0.1))
   # 5. one-legged event (based on original data)
   s1 <- data2$value[match(events0$start,data2$time)]
   p1 <- data2$value[match(events0$peak,data2$time)]
   e1 <- data2$value[match(events0$end,data2$time)]
   h1 <- p1-s1 #rising limb height
   h2 <- p1-e1 #recession limb height
   ix1 <- which(h1<0 | h2<0 | abs(h1)<(0.2*abs(h2)) | abs(h2)<(abs(h1)*0.2))

   if (length(ix1)==0) break
   
   ix1 <- sort(unique(ix1))
   i2 <- ix1[1]
   dif1 <- as.integer(difftime(events0$start[i2],events0$end[i2-1],unit="hours"))
   dif2 <- as.integer(difftime(events0$start[i2+1],events0$end[i2],unit="hours"))
   dist1 <- as.integer(difftime(events0$peak[i2],events0$peak[i2-1],unit="hours"))
   dist2 <- as.integer(difftime(events0$peak[i2+1],events0$peak[i2],unit="hours"))

   flag1 <- 0   
   #first event or event on rising limb, merge with the next event
   if (i2==1 | h2s<0.1 | h2<0 | abs(h2)<(abs(h1)*0.2)) { 
     if(!is.na(dif2) & dif2 < minEventDist) {
       events0$start[i2+1] <- events0$start[i2]; flag1 <- 1
     }
   #last event or event on recession limb, merge with the previous event
   } else if (i2==nrow(events0) | h1s<0.1 | h1<0 | abs(h1)<(abs(h2)*0.5)) {
     if (!is.na(dif1) & dif1 < minEventDist) {
       events0$end[i2-1] <- events0$end[i2]; flag1 <- 1
     } 
   } else {
     if (dist1<=dist2) {
     if (!is.na(dif1) & dif1 < minEventDist) {
       events0$end[i2-1] <- events0$end[i2]; flag1 <- 1
     }} else if (!is.na(dif2) & dif2 < minEventDist) { 
       events0$start[i2+1] <- events0$start[i2]; flag1 <- 1
     }
   }
   if (flag1==0) events1 <- rbind(events1,events0[i2,])
   events0 <- events0[-i2,] # now remove the event
   
   # recompute event duration
   events0[,nhour:=as.integer(difftime(end,start,units="hour"))+1]
   events0[,nrise:=as.integer(difftime(peak,start,units="hour"))+1]
   events0[,nrece:=as.integer(difftime(end,peak,units="hour"))]
   }
  
   # add back those events that are not combined above 
   if (nrow(events1)>0) {
     events0 <- rbind(events0,events1)
     events0 <- events0[order(start),]
   } 

   if (nrow(events0)>1) {
   # adjust peaks, start and end points
   # start point should be the lowerest point on the rising limb
   # end point should be the lowerest point on the recession limb
   # peak should be the highest point during event period
   #if (i1==1658) print(events0)
   for (i2 in 1:nrow(events0)) {
     istart <- match(events0$start[i2], data2$time)
     ipeak <- match(events0$peak[i2], data2$time)
     iend <- match(events0$end[i2], data2$time)
     ix0 <- which.min(data2$value[istart:ipeak])
     events0$start[i2] <- data2$time[istart+ix0-1]
   
     ix0 <- which.min(data2$value[ipeak:iend])
     events0$end[i2] <- data2$time[ipeak+ix0-1]
   
     dt1 <- subset(data2, time %in% seq(events0$start[i2],events0$end[i2],by="hour"))
     events0$peak[i2] <- dt1$time[which.max(dt1$value)]
   }
  
   # check if event start/peak/end are in order
   events0 <- events0[order(start),]
   ne1 <- nrow(events0)
   ix1 <- which((1:ne1) != order(events0$peak))
   if (length(ix1)>0) print("WARNING: events peak not in order!")
   ix1 <- which((1:ne1) != order(events0$end))
   if (length(ix1)>0) print("WARNING: events end not in order!")
   
   ix1 <- as.integer(difftime(events0$start[2:ne1],events0$end[1:(ne1-1)],units="hour"))
   if(sum(ix1<0)) print(paste0("WARNING: event starts before previous event ends: ",paste(which(ix1<0)+1,collapse=", ")))

   # combine adjacent events (to form compound events)
   if (nhourCompound >= 0) {
   ne1 <- nrow(events0)
   ends1 <- events0$end[1:(ne1-1)]
   starts1 <- events0$start[2:ne1]
   dif1 <- as.integer(difftime(starts1,ends1,units="hour"))
   ix1 <- which(dif1<=nhourCompound)+1
   if (length(ix1)==0) break
   kk <- 1
   while(kk<=length(ix1)) {
     ixs <- ix1[kk]-1
     while(1) {
       ixs <- c(ixs,ix1[kk])
       kk <- kk +1
       if (kk>length(ix1)) break
       if (ix1[kk] > (ix1[kk-1]+1)) break
     }
     peaks <- subset(data2, time %in% events0$peak[ixs])$value
     events0$peak[ixs[1]] <- events0$peak[ixs][which.max(peaks)]
     events0$end[ixs[1]] <- events0$end[ixs[length(ixs)]]
   }
   events0 <- events0[!(1:ne1 %in% ix1),]
   }

   # adjust adjecent events so that the previous event ends at lowest point
   # between the two peaks and the next event starts at the same point
   for (i2 in 1:(nrow(events0)-1)) {
     ipeak <- match(events0$peak[i2],data2$time)
     ipeak1 <- match(events0$peak[i2+1],data2$time)
     values <- data2$value[ipeak:ipeak1]
     if (sum(values>=thresh2)==length(values)) {
       ix0 <- which.min(values)
       events0$end[i2] <- events0$start[i2+1] <- data2$time[ipeak+ix0-1]
   }}

   }

   events0[,nhour:=as.integer(difftime(end,start,units="hour"))+1]
   events0[,nrise:=as.integer(difftime(peak,start,units="hour"))+1]
   events0[,nrece:=as.integer(difftime(end,peak,units="hour"))]

   # remove events below threshold
   ix1 <- which(data2$value[match(events0$peak,data2$time)] >= thresh1)
   events0 <- events0[ix1,]

   # combine events and data from all chunks
   data2$hour <- NULL
   dataAll <- rbind(dataAll, data2)
   eventsAll <- rbind(eventsAll, events0)
}

## put back those chunks that have no peaks (and hence removed during event identification)
#data0 <- subset(data, ! time %in% dataAll$time)
#if (nrow(data0)>0) {
#data0$smooth <- NA
#dataAll <- rbind(dataAll, data0)
#dataAll <- dataAll[order(dataAll$time),]
#}

#return(list(eventsAll=eventsAll, dataAll=dataAll))
return(eventsAll)
}

# Match observed events with model events (written by Yuqiong)
#
# Output is a list with 4 variables: 
# 1) events_obs_match (data.table)
#    observed events that are matched with model events
# 2) events_mod_match (data.table) 
#    model events that are matched with obs events
#
# 3) matched_mod (vector) 
#    values indicating whether the orginal model events are matched
#    1: one or more obs events found (combine obs events if more than one found)
#    0: no obs event found to match
#    NA: obs data is missing at model event peak 
#
# 4) matched_obs (vector)
#    values indicating whether the orginal obs events are matched
#    1: one of more model events found (combine model events if more than one found)
#    0: no model event found to match
#

matchEvents <- function(data_obs,data_mod,events_obs, events_mod) { 

library(data.table)

no1 <- nrow(events_obs)
nm1 <- nrow(events_mod)
match_mod <- rep(0,nm1)
match_obs <- rep(0,no1)
events_mod1 <- events_obs1 <- data.table()

if (no1>=1 & nm1>=1) {
events_obs <- events_obs[order(start),]
events_mod <- events_mod[order(start),]

# loop through observed events to find matches in the model events
# match is identified if model peak is within the observed event period
for (i1 in 1:no1) {

dates1 <- seq(events_obs$start[i1],events_obs$end[i1],by="hour")
ix0 <- which(events_mod$peak %in% dates1)
if (length(ix0)>0) {
  events_obs1 <- rbind(events_obs1,events_obs[rep(i1,length(ix0)),])
  events_mod1 <- rbind(events_mod1,events_mod[ix0,])
  match_obs[i1] <- 1
  match_mod[ix0] <- 1
}}

# loop through model events to find additional matches of obs events
# match is identified if obs peak is within the model event period
for (i1 in 1:nm1) {
dates1 <- seq(events_mod$start[i1],events_mod$end[i1],by="hour")
ix0 <- which((events_obs$peak %in% dates1) & (match_obs==0))
if (length(ix0)>0) {
  events_mod1 <- rbind(events_mod1,events_mod[rep(i1,length(ix0)),])
  events_obs1 <- rbind(events_obs1,events_obs[ix0,])
  match_mod[i1] <- 1
  match_obs[ix0] <- 1
}}

if (nrow(events_obs1)>0) {

# sort events in order
events_obs1 <- events_obs1[order(peak),]
events_mod1 <- events_mod1[order(peak),]

# combine events if duplicated starts, peaks, ends
while(1) {
ix1 <- which(duplicated(events_mod1$start))
ix1 <- c(ix1,which(duplicated(events_mod1$peak)))
ix1 <- c(ix1,which(duplicated(events_mod1$end)))
ix1 <- c(ix1,which(duplicated(events_obs1$start)))
ix1 <- c(ix1,which(duplicated(events_obs1$peak)))
ix1 <- c(ix1,which(duplicated(events_obs1$end)))
ix1 <- sort(unique(ix1))
if (length(ix1)==0) break

# combine obs events
events_obs1$start[ix1[1]-1] <- min(events_obs1$start[ix1[1]-1],events_obs1$start[ix1[1]])
events_obs1$end[ix1[1]-1] <- max(events_obs1$end[ix1[1]-1],events_obs1$end[ix1[1]])
peak0 <- data_obs$value[match(events_obs1$peak[ix1[1]-1],data_obs$time)]
peak1 <- data_obs$value[match(events_obs1$peak[ix1[1]],data_obs$time)]
if (peak0<peak1) events_obs1$peak[ix1[1]-1] <- events_obs1$peak[ix1[1]]
events_obs1[,nhour:=as.integer(difftime(end,start,units="hour"))+1]
events_obs1[,nrise:=as.integer(difftime(peak,start,units="hour"))+1]
events_obs1[,nrece:=as.integer(difftime(end,peak,units="hour"))]

# combine model events
events_mod1$start[ix1[1]-1] <- min(events_mod1$start[ix1[1]-1],events_mod1$start[ix1[1]])
events_mod1$end[ix1[1]-1] <- max(events_mod1$end[ix1[1]-1],events_mod1$end[ix1[1]])
peak0 <- data_mod$value[match(events_mod1$peak[ix1[1]-1],data_mod$time)]
peak1 <- data_mod$value[match(events_mod1$peak[ix1[1]],data_mod$time)]
if (peak0<peak1) events_mod1$peak[ix1[1]-1] <- events_mod1$peak[ix1[1]]
events_mod1[,nhour:=as.integer(difftime(end,start,units="hour"))+1]
events_mod1[,nrise:=as.integer(difftime(peak,start,units="hour"))+1]
events_mod1[,nrece:=as.integer(difftime(end,peak,units="hour"))]

events_obs1 <- events_obs1[-ix1[1],]
events_mod1 <- events_mod1[-ix1[1],]
}}

# for model events where observation is missing, mark with NA 
for (i1 in 1:nm1) {
  if (match_mod[i1]==1) next
  if (is.na(match(events_mod$peak[i1],data_obs$time))) match_mod[i1] <- NA 
}
}

return(list(events_obs_match=events_obs1,
            events_mod_match=events_mod1,
            matched_mod=match_mod,
            matched_obs=match_obs))
}

EventMultiObj <- function(m, o, weight1, weight2, period, siteId, basinType) { # Xia replaced old version with new version
# Input arguments:
# m: model streamflow; o: observation streamflow
# period: date; siteId: gage ID
# basinType has flags: 0: snowy; 1: slow; 2: flashy, 3: regular 
# weight1Event and weight2Event are weights for peak bias and volume bias to get combined metric   

# parameters
maxGapFill <- 5
snow1 <- FALSE
if (basinType == 0) snow1 <- TRUE
slow1 <- FALSE
if (basinType == 1) slow1 <- TRUE
prob_peak <- 0.9
if (basinType == 2) prob_peak <- 0.8

# construct data frame
data1 <- data.frame(mod = m, obs = o, Date = period)
data1 <- data1[!duplicated(data1$Date), ]

# fill short data gap
dates <- seq(min(data1$Date),max(data1$Date), by="hour")
tmp <- data.frame(Date=dates)
data1<-merge(tmp, data1, by="Date", all=T)
for (m1 in c("mod","obs")){
 data1[[m1]] <- zoo::na.approx(data1[[m1]],na.rm=FALSE,maxgap=maxGapFill)
}

# identify obs events
thresh1 <- quantile(data1$obs,prob_peak,na.rm=T)
data_obs <- data1[,c("Date","obs")]
names(data_obs) <- c("time","value")
events_obs <- eventIdentification(data_obs,snowy=snow1,slow=slow1,threshPeak=thresh1,threshold_prob=FALSE)

# identify model events
data_mod <- data1[,c("Date","mod")]
names(data_mod) <- c("time","value")
events_mod <- eventIdentification(data_mod,snowy=snow1,slow=slow1,threshPeak=thresh1,threshold_prob=FALSE)

# match events
matchResults <- matchEvents(data_obs,data_mod,events_obs,events_mod)
matchResults$peak_threshold <- thresh1
matchResults$events_obs_all <- events_obs
matchResults$events_mod_all <- events_mod

ne<-nrow(matchResults$events_obs_match)
if (ne>0) {

# peak timing error
times_obs <- matchResults$events_obs_match$peak
times_mod <- matchResults$events_mod_match$peak
peak_tm_err_hr <- mean(abs(as.numeric(difftime(times_mod, times_obs, units = "hours"))))

# peak magnitude bias
peaks_obs <- data1[["obs"]][match(times_obs,data1$Date)] 
peaks_mod <- data1[["mod"]][match(times_mod,data1$Date)]
peak_bias <- mean(abs(peaks_mod-peaks_obs)/peaks_obs*100) 
print(paste0("peak bias (%): ", round(peak_bias, 2)))

# volume bias
volume_bias <- 0
ij <- 0
for (i1 in 1:ne) {
  k1 <- match(matchResults$events_obs_match$start[i1], data1$Date)
  k2 <- match(matchResults$events_obs_match$end[i1], data1$Date)
  if (!is.na(k1) & !is.na(k2)) obs1 <- data1$obs[k1:k2] else obs1 <- NA
  k1 <- match(matchResults$events_mod_match$start[i1], data1$Date)
  k2 <- match(matchResults$events_mod_match$end[i1], data1$Date)
  if (!is.na(k1) & !is.na(k2)) mod1 <- data1$mod[k1:k2] else mod1 <- NA
  if (!is.na(sum(mod1)) & !is.na(sum(obs1))) {
    ij <- ij + 1
    volume_bias <- volume_bias + abs(sum(mod1)-sum(obs1))/sum(obs1)*100
  }
}
volume_bias <- volume_bias/ij
#volume_bias <- volume_bias/ne
print(paste0("volume bias (%): ", round(volume_bias, 2)))

# event-based objective function 
obj <- weight1*abs(peak_bias) + weight2*abs(volume_bias)
print(paste0("event-based objective function (%): ", round(obj,2)))
}else{
obj<-peak_bias<-volume_bias<-peak_tm_err_hr<-NA
}
return(list(obj = obj, peak_bias = peak_bias, peak_tm_err_hr = peak_tm_err_hr, volume_bias = volume_bias))
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



#-------------------------------------------------------------------------------------------
#--------  This part has all the functions for the mean areal values  ---------------------
#-------------------------------------------------------------------------------------------

create_lsm_mask <- function(linkFile, linkId, spwtFile, geoFile, fulldomFile) {

    rl <- GetNcdfFile(linkFile, variables = c("time"), exclude = TRUE,
        quiet = TRUE)
    rl$site_no <- stringr::str_trim(rl$gages)

    # function to Trace upstream in route link
    traceUp <- function(linkid, rl) {
        uplinks <- c(linkid)
        donelinks <- c()
        while (length(uplinks) > 0) {
          i <- uplinks[1]
          donelinks <- c(donelinks, i)
          newlinks <- subset(rl$link, rl$to==i)
          uplinks <- c(uplinks, newlinks)
          uplinks <- unique(subset(uplinks, !(uplinks %in% donelinks)))
        }
        donelinks
     }

    # Calling Trace up function
    uplinks <- traceUp(linkId, rl)

    ReadWtFile <- function (wtFile) {
      ncid <- ncdf4::nc_open(wtFile)
      i_index <- ncdf4::ncvar_get(ncid, "i_index")
      j_index <- ncdf4::ncvar_get(ncid, "j_index")
      IDmask <- ncdf4::ncvar_get(ncid, "IDmask")
      weight <- ncdf4::ncvar_get(ncid, "weight")
      regridweight <- ncdf4::ncvar_get(ncid, "regridweight")
      data <- data.frame(i_index = i_index, j_index = j_index,
          IDmask = IDmask, weight = weight, regridweight = regridweight)
      polyid <- ncdf4::ncvar_get(ncid, "polyid")
      overlaps <- ncdf4::ncvar_get(ncid, "overlaps")
      polys <- data.frame(polyid = polyid, overlaps = overlaps)
      list(data, polys)
    }

    spwtList <- ReadWtFile(spwtFile)

    # subset the spatial weight file
    spwt.data <- spwtList[[1]]
    spwt.data <- subset(spwt.data, spwt.data$IDmask %in% uplinks)

    # Calculate what percentage of each cell contributes to the desired location
    spwt.data.sumcell <- plyr::ddply(spwt.data, .(i_index, j_index), summarize, sumwt=sum(regridweight))

    # Define a 2D field at the hydro resolution

    # Get the hydro dimensions from thr Fulldom file
    ncid <- ncdf4::nc_open(fulldomFile)
    xlen.hyd <- ncid$dim$x$len
    ylen.hyd <- ncid$dim$y$len
    ncdf4::nc_close(ncid)

    # create an empty 2D mask using the dimension of the hydro grid
    mskvar.hyd <- matrix(0, nrow=xlen.hyd, ncol=ylen.hyd)

     # for each pixel assign what percentage of it falls in the contributing area of the gage/outlet
    for (n in 1:nrow(spwt.data.sumcell)) {
        mskvar.hyd[spwt.data.sumcell$i_index[n], spwt.data.sumcell$j_index[n]] <- spwt.data.sumcell$sumwt[n]
    }

    # Create 2D field at the LSM resolution
    library(raster)

    # Get the LSM dimension from the geogrid file
    ncid <- ncdf4::nc_open(geoFile)
    xlen.geo <- ncid$dim$x$len
    ylen.geo <- ncid$dim$y$len
    ncdf4::nc_close(ncid)

    # create an empty 2D mask using the dimension of the LSM grid
    mskvar.lsm <- matrix(1, xlen.geo, ylen.geo)

    # resample the hydro mask to LSM mask
    mskvar.lsm <- as.matrix(resample(raster(mskvar.hyd), raster(mskvar.lsm), 'bilinear'))
    return(mskvar.lsm)
}

# Defien a function to calculate basin mean
basin_avg <- function(myvar, mymsk, minValid=-9998) {
    myvar[which(myvar<minValid)]<-NA
    mymsk[which(is.na(myvar))]<-NA
    sum(mymsk*myvar, na.rm=TRUE)/sum(mymsk, na.rm=TRUE)
}


FlipUD <- function(matrix) {
       apply(matrix, 2, rev)
}

ReadSwe_Multi <- function(file, mskvar.lsm) {
  lsmVar <- GetNcdfFile(file, variables = c("SNEQV"), quiet = TRUE)
  meanAreal <- basin_avg(lsmVar$SNEQV, mskvar.lsm)
  map <- data.frame(mod = meanAreal, POSIXct = as.POSIXct(strsplit(basename(file),"[.]")[[1]][1], format = "%Y%m%d%H%M", tz = "UTC"))
  return(map)
}


noZeroFunction_snow = function(mod, obs, period){
  zmin = min(mod, obs)
  if (zmin ==0) {
    #Following Push2012, though I don't get identical results, they are very close
    epsilon = mean(obs, na.rm=T)/100
    obs = obs + epsilon
    mod = mod + epsilon
  } # end if (zmin =0)
  df = data.table(mod = mod, obs = obs, period = period)
  return(df)
} # end function

ReadSm_Multi <- function(file, mskvar.lsm) {
  lsmVar <- GetNcdfFile(file, variables = c("SOIL_M"), quiet = TRUE)
  meanAreal <- basin_avg(lsmVar$SOIL_M[,1,], mskvar.lsm)
  map <- data.frame(mod = meanAreal, POSIXct = as.POSIXct(strsplit(basename(file),"[.]")[[1]][1], format = "%Y%m%d%H%M", tz = "UTC"))
  return(map)
}


# a function for calculating the soil moisture anomalies 
CalcSmAnomaly <- function(obs.obj.soil, window_days) {

          # calculate mean of obs/mod over a 7 or 15 days averging window
          setkey(mod_soil.obj, "site_no", "Date")
          mod_soil.obj[, `:=` (mod_window_averaged = rollapply(mod.d, window_days,  align='center', function(x) mean(x, na.rm=TRUE), by.column=TRUE, partial=T),
                               obs_window_averaged = rollapply(obs, window_days,  align='center', function(x) mean(x, na.rm=TRUE), by.column=TRUE, partial=T))]

          # Now let s calculate the anomaly of soil moisture using the 31 day window
          #1. calculate the mean of the 31 days around the date
          mod_soil.obj[, `:=` (mod_clim_mean_dummy = rollapply(mod_window_averaged, 31,  align='center', function(x) mean(x, na.rm=TRUE), by.column=TRUE, partial=T),
                               obs_clim_mean_dummy = rollapply(obs_window_averaged, 31,  align='center', function(x) mean(x, na.rm=TRUE), by.column=TRUE, partial=T))]

          #2. add the day of the year
          mod_soil.obj[, yday := lubridate::yday(Date)]

          #3. calculate the mean of the 31 averaged data over the years
          clim_mean <- mod_soil.obj[, .(mod_clim_mean = mean(mod_clim_mean_dummy, na.rm=TRUE), obs_clim_mean = mean(obs_clim_mean_dummy,na.rm=TRUE)), by = c("site_no", "yday")]
          mod_soil.obj <- merge(mod_soil.obj, clim_mean, by = c("site_no", "yday"))
          mod_soil.obj[, `:=` (mod_anomaly = mod_window_averaged - mod_clim_mean, obs_anomaly = obs_window_averaged - obs_clim_mean)]

          # lets clean the unnecessay columns
          mod_soil.obj[, `:=`(mod_window_averaged = NULL, obs_window_averaged = NULL, mod_clim_mean_dummy = NULL, obs_clim_mean_dummy = NULL, mod_clim_mean = NULL, obs_clim_mean = NULL)]

          # let s do the set key
          setkey(mod_soil.obj, "site_no", "Date")
          return(mod_soil.obj)
}


CalcSmCDF <- function(obs.obj.soil, window_days) {
  
  # calculate mean of obs/mod over a 7 or 15 days averging window
  setkey(mod_soil.obj, "site_no", "Date")
  mod_soil.obj[, `:=` (mod_window_averaged = rollapply(mod.d, window_days,  align='center', function(x) mean(x, na.rm=TRUE), by.column=TRUE, partial=T),
                       obs_window_averaged = rollapply(obs, window_days,  align='center', function(x) mean(x, na.rm=TRUE), by.column=TRUE, partial=T))]
  
  # Now lets do the CDF matching, we want to match the SMAP to the model 
  # first remove all the NA, NaNs from the dataset both model and obs
  mod_soil.obj.nona <- mod_soil.obj[!is.na(mod_window_averaged) & !is.na(obs_window_averaged),]
  mod_soil.obj.nona <- mod_soil.obj[!is.nan(mod_window_averaged) & !is.nan(obs_window_averaged),]
  
  library(qmap)
  #model_fitobj <- fitQmapQUANT(obs = mod_soil.obj.nona$mod_window_averaged, mod = mod_soil.obj.nona$obs_window_averaged, wet.day = FALSE)
  #mod_soil.obj.nona$obs_cdf_matched <- doQmapQUANT(x = mod_soil.obj.nona$obs_window_averaged , fobj = model_fitobj)
  
 
  # let s remove the non necessary columns and do the renaming 
  mod_soil.obj.nona[, mod.d := NULL]
  mod_soil.obj.nona[, obs := NULL]
  mod_soil.obj.nona[, obs_window_averaged := NULL]

  setnames(mod_soil.obj.nona, c("mod_window_averaged", "obs_cdf_matched"), c("mod", "obs"))
  
  # let s do the set key
  setkey(mod_soil.obj.nona, "site_no", "Date")
  return(mod_soil.obj.nona)
}


