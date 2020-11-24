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

# Convert to daily flow

Convert2Daily <- function(str) {
   #str$Date <- rwrfhydro::CalcDateTrunc(str$POSIXct)
   str$Date <- CalcDateTrunc(str$POSIXct)
   setkey(str, Date)
   if ("q_cms" %in% names(str)) {
      str.d <- str[, list(q_cms=mean(q_cms, na.rm=TRUE)), by = c("Date", "site_no")]
   } else if ("obs" %in% names(str)) {
      str.d <- str[, list(obs=mean(obs, na.rm=TRUE)), by = c("Date", "site_no")]
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
 
eventIdentification <- function(data,nwinSpan=36,threshold_prob=TRUE,
  threshPeak=0.9,threshLowflow=0.5,threshFlowRange=0.3,maxGapFill=5,
  minEventDuration=6,maxRiseDuration=120,maxRecessionDuration=240,
  maxPeakDistCompound=120, nwinShift=12,minInterval=6,minLengthData=6) {

########### Inputs #####################
# data: data.frame for streamflow, 1st column is time in POSIXct format, 
#       2nd column is the flow values
# nwinSpan: window size (in hours) for smoothing; consider starting with
#       240 hours for snow-driven streamflow with diel fluctuations and
#       36 hours for non-snow-driven streamflow  
# nwinShift: size of window (nwinShift*2+1) around smoothed peak to 
#       identify actual peak, as there often exists a shift between smoothed
#       and actual peaks (hours)
# minInterval: minimum seperation between peaks; peaks that are too close 
#       will be combined
# minEventDuration: minimum event duration in hours; events with duration 
#       shorter than the threshold will be combined with neighbour events
# maxRiseDuration: maximum duration in hours for the rise limb
# maxGapFill: maximum duration (in hours) of data gaps to be filled with 
#       spline interpolation; gaps larger than this threshold will not be filled
# maxRecessionDuration: maximum duration in hours for the recession limb
# maxPeakDistCompound: maximum distance between adjacent peaks that can be
#       considered combined into compound events
# threshold_prob: logical varible to indicate whether the three threhold 
#       parametes (threshPeak, threshLowflow, threshFlowRange) are climatological
#       probabilities or the actual values in units of streamflow data 
# threshPeak: threshold for event peaks; peaks below threshold are 
#       disgarded
# threshLowflow: threshold for low flows, used to determine event 
#       start/end points
# threshFlowRange: probability threshold for event flow range (i.e., flow 
#       difference between peak and start point, or between peak and end point).
#       Events with flow ranges below this threshold are discarded
# minLengthData: mininum length of record to perform event separation (hours)
#       flow time series is first devided into chunks with no missing values;
#       chunks too short are ignored for event detection 

######## outputs ##############
# Output is a list of three data frames
# 1st data frame (eventsAll): all single-peak events identified, with three
#       POSIXct columns indicating the start, peak, and end hours of all events
# 2nd data frame (eventsCompound): final list of events identified where adjcent 
#       single-peak events are combined into a compound event
# 3rd data frame: the original streamflow data frame with short gap filled 
#       with spline interpolation, as well as the smoothed time series   


# function to get the index of the last minimum value
# which.min gets the first minimum value index
which.min2 <- function(x, last.index = FALSE, ...){
  if(last.index) max(which(x == min(x, ...))) else which.min(x)
}

# function to fine tune start and end points
# start point should have the minimum flow value on the rising limb
# end point should have the minimum flow value on the recession limb
tuneEventStartEnd <- function(events, df1) {
   for (k1 in 1:nrow(events)) {

     iStart <- match(events$start[k1],df1$time)
     iPeak <- match(events$peak[k1],df1$time)
     iEnd <- match(events$end[k1],df1$time)

     idx1 <- which.min2(df1$value[iStart:iPeak],last.index=TRUE)
     if (idx1!=1) events$start[k1] <- events$start[k1]+(idx1-1)*3600

     idx1 <- which.min(df1$value[iPeak:iEnd])
     if (idx1!=(iEnd-iPeak+1)) events$end[k1] <- events$peak[k1]+(idx1-1)*3600
   }
   events
}

# function to identify event start/end points given peak time and data
getEvents <- function(tPeaks,df1,thresh) {

iLowflows <- which(df1$value < thresh)
iPeaks <- match(tPeaks,df1$time)
iStarts0 <- sapply(iPeaks, function(x) max(iLowflows[iLowflows<x]-1))
iEnds0 <- sapply(iPeaks, function(x) min(iLowflows[iLowflows>x]+1))
iStarts0[is.infinite(iStarts0) | is.na(iEnds0) | iStarts0<1] <- 1
iEnds0[is.infinite(iEnds0) | is.na(iEnds0) | iEnds0>nrow(df1)] <- nrow(df1)

n1 <- length(iPeaks)
iStarts1 <- iEnds1 <- rep(NA,n1)
for (k1 in 1:n1) {
  if (k1 == 1) {
    iStarts1[k1] <- iStarts0[k1]
    if (n1==1) iEnds1[k1] <- iEnds0[k1]
  } else {
    j1 <- iPeaks[k1-1]-1+which.min(df1$value[iPeaks[k1-1]:iPeaks[k1]])
    iStarts1[k1] <- j1
    iEnds1[k1-1] <- j1
    if (k1 == n1) iEnds1[k1] <- iEnds0[k1]
}}

iStarts <- sapply(1:n1, function(x) max(iStarts0[x],iStarts1[x]))
iEnds <- sapply(1:n1, function(x) min(iEnds0[x],iEnds1[x]))
iPeaks <- sapply(1:length(iPeaks),function(x) which.max(df1$value[iStarts[x]:iEnds[x]])+iStarts[x]-1)

# events (start time, peak time, end time)
eventDt <- data.frame(start=df1$time[iStarts],
                      peak=df1$time[iPeaks],
                      end=df1$time[iEnds])

# fine tune start and end points 
eventDt <- tuneEventStartEnd(eventDt, df1)

eventDt
}

# function to identify compound events given a series single events
compoundEvents <- function(events,df1,thresh,ix_cpd0) {

ne1 <- nrow(events)
events$ix_cpd <- NA
events$start_cpd <- events$start
events$peak_cpd <- events$peak
events$end_cpd <- events$end

# first label all the single events that belong to compound events
if (ne1>1) {
kk <- ix_cpd0
for (k1 in 1:(ne1-1)) 
  if (as.integer(difftime(events$start[k1+1],events$end[k1],units="hours"))<=5) {
  if (as.integer(difftime(events$peak[k1+1],events$peak[k1],units="hours"))<=maxPeakDistCompound) {
    min0 <- min(subset(df1,time %in% seq(events$end[k1],events$start[k1+1],by="hour"))$value,na.rm=T) 
    if (min0 > thresh) {
      if (!is.na(events$ix_cpd[k1])) {
        events$ix_cpd[k1+1] <- events$ix_cpd[k1]
      } else {
        kk <- kk + 1; events$ix_cpd[k1:(k1+1)] <- kk
      }}}}

# then identify the start, peak, and end points of compound events
if (kk>ix_cpd0) {
  for (k1 in (ix_cpd0+1):kk) {
    
     ix1 <- which(!is.na(events$ix_cpd) & events$ix_cpd==k1)
     events$start_cpd[ix1] <- events$start[min(ix1)]
     events$end_cpd[ix1] <- events$end[max(ix1)]
     ipeaks <- events$peak[ix1]
     events$peak_cpd[ix1] <- ipeaks[which.max(df1$value[match(ipeaks,df1$time)])]

}}
}
events
}

############## processing starts here #############
data <- na.omit(data)
names(data) <- c("time","value")
if (threshold_prob) {
thresh1 <- quantile(data$value, threshPeak)
thresh2 <- quantile(data$value, threshLowflow)
thresh3 <- quantile(data$value, threshFlowRange)
} else {
thresh1 <- threshPeak
thresh2 <- threshLowflow
thresh3 <- threshFlowRange
}

# minor adjustments to peak threshold
if (thresh1 <= max(data$value)*0.01) thresh1 <- max(data$value)*0.01
#if (thresh1 <= 1.0) thresh1 <- 1.0 #cms, convert to cfs if flow unit is cfs

# fill short data gaps with spline interpolation
dates <- seq(min(data$time),max(data$time), by="hour")
dates1 <- dates[!dates %in% data$time]
if (length(dates1)>0) {
data <- rbind(data,data.frame(time=dates1,value=NA))
data <- data[order(data$time),]
data$value <- zoo::na.approx(data$value,maxgap=maxGapFill,na.rm=FALSE)
data <- subset(data,!is.na(value))
}

# identify remaining data gaps and break into chunks with no missing data
dates <- seq(min(data$time),max(data$time), by="hour")
dates1 <- dates[!dates %in% data$time]
chunks <- match(dates1, dates)
nchunk <- length(chunks)+1

# loop through chunks to identy peaks for each chunk and then put them back together
dataAll <- eventsAll <- data.frame()
for (i1 in 1:nchunk) {

   # start index of current non-missing period
   if (i1==1) { j1 <- 1
   } else { j1 <- chunks[i1-1]+1 }

   # end index of current non-missing period
   if (i1==nchunk) { j2 <- length(dates)
   } else { j2 <- chunks[i1]-1 }

   if (j1>j2) next
   if (length(j1:j2) < minLengthData) next 

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
   for (i2 in 2:(nrow(data2)-1)) 
     if (d1[i2]>=0 & d1[i2+1]<=0) ipeak <- c(ipeak,i2)
   if (length(ipeak)==0) next

   # identify corresponding peaks in the original data
   ipeak1 <- rep(NA, length(ipeak))
   for (i2 in 1:length(ipeak)) {
     j1 <- ipeak[i2]-nwinShift
     j2 <- ipeak[i2]+nwinShift
     if (j1<1) j1 <- 1
     if (j2>nrow(data2)) j2 <- nrow(data2)
     ix2 <- which.max(data2$value[j1:j2])
     ipeak1[i2] <- ipeak[i2]-nwinShift-1+ix2
   }
   ipeak1 <- unique(ipeak1)
   ipeak1[ipeak1 < 1] <- 1
   ipeak1[ipeak1 > nrow(data2)] <- nrow(data2)

   # peaks identified
   peaks <- data2[ipeak1,]

   # remove those below the threshold
   peaks <- subset(peaks, value >= thresh1)
   if (nrow(peaks)==0) next

   # combine those peaks that are too close
   int1 <- which(as.numeric(diff(peaks$time),"hours") < minInterval)
   rowIdx <- 1:nrow(peaks)
   rowIdx <- rowIdx[!rowIdx %in% (int1+1)]
   peaks1 <- data.frame()
   for (k1 in rowIdx) {
      if (k1 %in% int1) {
        if (peaks$value[k1] > peaks$value[k1+1]) {
          peaks1 <- rbind(peaks1,peaks[k1,])
        } else {
          peaks1 <- rbind(peaks1,peaks[k1+1,])
        }
      } else {
        peaks1 <- rbind(peaks1,peaks[k1,])
      }
   }
   if (nrow(peaks1)==0) next

   # identify start and end points of all events
   events1 <- getEvents(peaks1$time, data2,thresh2)

   # remove events with a rising/recession limb that is too short (verticallly)
   iPeaks <- match(events1$peak,data2$time)
   iStarts <- match(events1$start,data2$time)
   iEnds <- match(events1$end,data2$time)
   ix1 <- NULL
   n1 <- length(iPeaks)
   for (k1 in 1:n1) 
     if (min(data2$value[iPeaks[k1]]-data2$value[iStarts[k1]],data2$value[iPeaks[k1]]-data2$value[iEnds[k1]]) >= thresh3) ix1 <- c(ix1,k1)
   if (length(ix1)==0) next
   events2 <- getEvents(events1$peak[ix1], data2,thresh2)

   # remove events that are too short in duration
   ix1 <- which(as.integer(difftime(events2$end,events2$start,units="hours")) >= minEventDuration)
   if (length(ix1)==0) next
   events3 <- getEvents(events2$peak[ix1], data2,thresh2)

   # adjust long starts and long tails
   for (k1 in 1:nrow(events3)) {
      t1 <- as.integer(difftime(events3$peak[k1],events3$start[k1],units="hours"))
      if (t1>maxRiseDuration) events3$start[k1] <- events3$peak[k1]-maxRiseDuration*3600
      t1 <- as.integer(difftime(events3$end[k1],events3$peak[k1],units="hours"))
      if (t1>maxRecessionDuration) events3$end[k1] <- events3$peak[k1]+maxRecessionDuration*3600
  }

  # final adjustments to start and end points
  events3 <- tuneEventStartEnd(events3,data2)

  # identify compound events
  ix_cpd0 <- 0
  if (nrow(eventsAll)>=1) ix_cpd0 <- max(eventsAll$ix_cpd,na.rm=T)
  if (is.infinite(ix_cpd0)) ix_cpd0 <- 0
  events3 <- compoundEvents(events3, data2, thresh2,ix_cpd0)

  # put them back together
  data2$hour <- NULL
  dataAll <- rbind(dataAll, data2)
  eventsAll <- rbind(eventsAll, events3)
}

# add in those chunks that have no peaks
data1 <- subset(data, ! time %in% dataAll$time)
if (nrow(data1)>0) {
data1$smooth <- NA
dataAll <- rbind(dataAll, data1)
dataAll <- dataAll[order(dataAll$time),]
}

if (nrow(eventsAll)==0) {
  eventsCompound <- eventsAll
} else {
# compile event list that combines regular events with compound events
eventsAll$start0 <- eventsAll$peak0 <- eventsAll$end0 <- eventsAll$end
ix1 <- which(is.na(eventsAll$ix_cpd))
eventsAll$start0[ix1] <- eventsAll$start[ix1]
eventsAll$peak0[ix1] <- eventsAll$peak[ix1]
eventsAll$end0[ix1] <- eventsAll$end[ix1]
ix1 <- which(!is.na(eventsAll$ix_cpd))
eventsAll$start0[ix1] <- eventsAll$start_cpd[ix1]
eventsAll$peak0[ix1] <- eventsAll$peak_cpd[ix1]
eventsAll$end0[ix1] <- eventsAll$end_cpd[ix1]

eventsCompound <- eventsAll[,c("start0","peak0","end0")]
eventsCompound <- eventsCompound[!duplicated(eventsCompound),]
names(eventsCompound) <- c("start","peak","end")

eventsAll <- eventsAll[,c("start","peak","end")]

# discard events at the start or end of periods with data missing
ix1 <- match(eventsAll$start,dataAll$time)
ix2 <- which(ix1 != 1 & !is.na(dataAll$value[ix1-1]))
ix2 <- c(ix2,which(ix1==1 & dataAll$value[ix1]<=thresh2))
eventsAll <- eventsAll[ix2,]
ix1 <- match(eventsAll$end,dataAll$time)
ix2 <- c(ix2, which(ix1==nrow(dataAll) & dataAll$value[ix1]<=thresh2))
ix2 <- which(ix1 != nrow(dataAll) & !is.na(dataAll$value[ix1+1]))
eventsAll <- eventsAll[ix2,]

ix1 <- match(eventsCompound$start,dataAll$time)
ix2 <- which(ix1 != 1 & !is.na(dataAll$value[ix1-1]))
ix2 <- c(ix2,which(ix1==1 & dataAll$value[ix1]<=thresh2))
eventsCompound <- eventsCompound[ix2,]
ix1 <- match(eventsCompound$end,dataAll$time)
ix2 <- which(ix1 != nrow(dataAll) & !is.na(dataAll$value[ix1+1]))
ix2 <- c(ix2, which(ix1==nrow(dataAll) & dataAll$value[ix1]<=thresh2))
eventsCompound <- eventsCompound[ix2,]
}

list(eventsAll,eventsCompound,dataAll)

}

# Match observed (compound) events with model events (written by Yuqiong)

matchEvents <- function(data_mod, eventsAllMod, eventsCompoundMod,
  eventsCompoundObs,maxDist=48) {

###### Inputs ##############
# data_mod: data.frame for model streamflow, 1st column is time in POSIXct format,
#       2nd column is the flow values
# eventsAllMod: data frame listing all single-peak events for model
#    streamflow (identified by eventIdentification.R)
# eventsCompoundMod: data frame of compound events for model streamflow
# eventsCompoundObs: data frame of compound events for observed streamflow
# maxDist: window size (in hours) for identifying model events for
#    a given observed event. Consider starting with 48 hours for
#    non-snow basins and 7*24 (i.e., one week) for snow basins

##### Output ##############
# data frame with columns corresponding to the start, peak, and end times of
# matched observed and model events. It has the same number of rows as 
#   eventsCompoundObs
#   1st - 3rd columns: start, peak and end times of observed events
#   4th - 6th columns: start, peak and end times of model events 
#   7th column: match category 
#       1 = matched with eventsCompoundMod
#       2 = matched with eventsAllMod
#       3 = missed by model

no1 <- nrow(eventsCompoundObs)
nm1 <- nrow(eventsCompoundMod)
nm2 <- nrow(eventsAllMod) 

if (no1==0) { 
  print("WARNING: no events to match")
  return(data.frame())
}
 
dfMatch <- data.frame(ix_obs=1:no1,match=rep(NA,no1),
  ix_mod1=rep(NA,no1), ix_mod2=rep(NA,no1))

for (k1 in 1:no1) {

  # first assume obs event is unmatched
  dfMatch$match[k1] <- 3

  # first round: match observed compound events with model compound events
  if (nm1>0) {
  idx1 <- which(! (1:nm1) %in% dfMatch$ix_mod1)
  dist1 <- abs(as.integer(difftime(eventsCompoundObs$peak[k1],eventsCompoundMod$peak[idx1],units="hours")))
  mdist1 <- min(dist1)
  i1 <- idx1[which.min(dist1)]
  if (mdist1 <= maxDist) {
     dfMatch$match[k1] <- 1
     dfMatch$ix_mod1[k1] <- i1
     next
  }} else {
    if (nm2>0) {
  # 2nd round: match remaining observed events with model single-peak events
    idx1 <- which(! (1:nm2) %in% dfMatch$ix_mod2[k1])
    dist1 <- abs(as.integer(difftime(eventsCompoundObs$peak[k1],eventsAllMod$peak[idx1],units="hours")))
    mdist1 <- min(dist1)
    i1 <- idx1[which.min(dist1)]
    if (mdist1 <= maxDist) {
       dfMatch$match[k1] <- 2
       dfMatch$ix_mod2[k1] <- i1
       next
    }}
}}

# assemble matched events of obs and mod
peak_obs <- eventsCompoundObs$peak
start_obs <- eventsCompoundObs$start
end_obs <- eventsCompoundObs$end

peak_mod <- peak_obs
start_mod <- start_obs
end_mod <- end_obs

ix1 <- which(dfMatch$match==1)
if (length(ix1)>0) {
  peak_mod[ix1] <- eventsCompoundMod$peak[dfMatch$ix_mod1[ix1]]
  start_mod[ix1] <- eventsCompoundMod$start[dfMatch$ix_mod1[ix1]]
  end_mod[ix1] <- eventsCompoundMod$end[dfMatch$ix_mod1[ix1]]
}
ix1 <- which(dfMatch$match==2)
if (length(ix1)>0) {
  peak_mod[ix1] <- eventsAllMod$peak[dfMatch$ix_mod2[ix1]]
  start_mod[ix1] <- eventsAllMod$start[dfMatch$ix_mod2[ix1]]
  end_mod[ix1] <- eventsAllMod$end[dfMatch$ix_mod2[ix1]]
}
ix1 <- which(dfMatch$match==3)
if (length(ix1)>0) {
names(data_mod) <- c("time","value")
for (k1 in ix1) {
tmp <- subset(data_mod,time>=eventsCompoundObs$start[k1] & time<=eventsCompoundObs$end[k1])
peak_mod[k1] <- tmp$time[which.max(tmp$value)]
}}

eventsMatched <- data.frame(start_obs,peak_obs,end_obs,start_mod,peak_mod,end_mod)
eventsMatched$match <- dfMatch$match
eventsMatched

}

# Xia added EventMultiObj on 20200626 and modified on 20200723.

EventMultiObj <- function(m, o, weight1, weight2, period, siteId) {

# Function to calculate event-based performance measure  
# The subroutines were provided by Yuqiong on 25 June 2020 and updated in July 2020.
# Xia adjsuted and incorporated scripts into calibration workflow on 26 June 2020, and then updated in July 2020.
# Input: 
#       m: model simulated streamflow time series
#       o: observed streamflow time series
#       w1: weight for percentage bias of peak flow
#       w2: weight for percentage bias of flow volume 
#       period: time and date for streamflow time series  
#       siteId: site no used for differentiating snowy or snowy basins
# Ouput:
#      obj: performance measure  


# construct data frame
data1 <- data.frame(mod = m, obs = o, Date = period)
data1 <- data1[!duplicated(data1$Date), ]

# subjective parameters for snowy or non-snowy basin 
# we will set the parameters for the non snowy basins for now, needs to be namelist options later
#load("/glade/p/cisl/nwc/nwmv30_calibration/Task_2/setup_files/files/calib_shp_conus_oconus.Rdata")
#if (df.shp[df.shp$site_no%in%siteId,"snowy"] == 0 ){  

# parameters for non-snow basins
nwinSpan1 <- 36
minEventDuration1 <- 12
maxRiseDuration1 <- 2*24
maxRecessionDuration1 <- 5*24
maxPeakDistCompound1 <- 5*24
maxDist1 <- 2*24

# parameters for snow basins
#}else{
#nwinSpan1 <- 240
#minEventDuration1 <- 36
#maxRiseDuration1 <- 30*24
#maxRecessionDuration1 <- 60*24
#maxPeakDistCompound1 <- 15*24
#maxDist1 <- 7*24
#}

# smoothing time window (Xia added on 20200723)
nwinSpan1 <- 36

# peak flow threshold (Xia added on 20200723)
th1 <- 0.9

# identify events for observed streamflow
print("identify events for observed streamflow")
listObs <- eventIdentification(data1[,c("Date","obs")],nwinSpan=nwinSpan1,minEventDuration=minEventDuration1, maxRiseDuration=maxRiseDuration1,maxPeakDistCompound=maxPeakDistCompound1,maxRecessionDuration=maxRecessionDuration1,threshPeak=th1)

# identify events for model streamflow
print("identify events for model streamflow")
listMod <- eventIdentification(data1[,c("Date","mod")],nwinSpan=nwinSpan1,minEventDuration=minEventDuration1, maxRiseDuration=maxRiseDuration1,maxPeakDistCompound=maxPeakDistCompound1,maxRecessionDuration=maxRecessionDuration1,threshPeak=th1)

# match observed events with model events
print("match observed events with model events")
eventsMatched <- matchEvents(data1[,c("Date","mod")],listMod[[1]], listMod[[2]], listObs[[2]], maxDist=maxDist1)

n1 <- sum(eventsMatched$match %in% c(1,2), na.rm=T)
n2 <- sum(eventsMatched$match==3, na.rm=T)
print(paste0("+++++ Number of events detected and matched: ", n1))
print(paste0("+++++ Number of events detected but missed by model: ", n2))
ne <- nrow(eventsMatched)

if (ne>0) {
# peak timing error
#time_err <- mean(abs(as.integer(difftime(eventsMatched$peak_mod,eventsMatched$peak_obs,units="hour"))))
#print(paste0("timing error (hours): ", round(time_err,2)))

# raw or interpolated streamflow 
#datafm1 <- data1; datafm2 <- data1 
datafm1 <- listObs[[3]]; colnames(datafm1)[1]<-"Date"; colnames(datafm1)[2]<-"obs"
datafm2 <- listMod[[3]]; colnames(datafm2)[1]<-"Date"; colnames(datafm2)[2]<-"mod"

# peak bias (%)
obs_peak <- datafm1$obs[match(eventsMatched$peak_obs, datafm1$Date)]
mod_peak <- datafm2$mod[match(eventsMatched$peak_mod, datafm2$Date)]
#peak_bias <- mean((mod_peak-obs_peak)/obs_peak*100) 
peak_bias <- mean(abs(mod_peak-obs_peak)/abs(obs_peak)*100) #revised 20200929
print(paste0("peak bias (%): ", round(peak_bias,2)))

# volume bias (%)
volume_bias <- 0
for (i1 in 1:ne) {  
  k1 <- match(eventsMatched$start_obs[i1], datafm1$Date)
  k2 <- match(eventsMatched$end_obs[i1], datafm1$Date)
  if (!is.na(k1) & !is.na(k2)) obs1 <- data1$obs[k1:k2] else obs1 <- NA
  k1 <- match(eventsMatched$start_mod[i1], datafm2$Date)
  k2 <- match(eventsMatched$end_mod[i1], datafm2$Date)
  if (!is.na(k1) & !is.na(k2)) mod1 <- data1$mod[k1:k2] else mod1 <- NA
  if (!is.na(sum(mod1)) & !is.na(sum(obs1))) volume_bias <- volume_bias + abs(sum(mod1)-sum(obs1))/abs(sum(obs1))*100 #revised 20200929
  #if (!is.na(sum(mod1)) & !is.na(sum(obs1))) volume_bias <- volume_bias + (sum(mod1)-sum(obs1))/sum(obs1)*100
}

volume_bias <- volume_bias/ne
print(paste0("volume bias (%): ", round(volume_bias,2)))

# event-based objective function (Xia added w1 and w2 as input variables) 
obj <- weight1*peak_bias + weight2*volume_bias #revised 20200929
#obj <- w1*abs(peak_bias) + w2*abs(volume_bias)
print(paste0("event-based objective function (%): ", round(obj,2)))
}else{
obj=NA
}
return(obj)
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
