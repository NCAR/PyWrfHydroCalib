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
  zmin = min(mod, obs)
  if (zmin ==0) {
    #Following Push2012, though I don't get identical results, they are very close
    epsilon = mean(obs, na.rm=T)/100
    obs = obs + epsilon
    mod = mod + epsilon
  } # end if (zmin =0)
  df = data.table(q_cms = mod, obs = obs, period = period)
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
  # Nash-Sutcliffe Efficiency (NSE)
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

parms <- function(data){
  S <- data[,2]   # Specify similuations
  O <- data[,1]   # Specify observations
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


# Line to be added to /glade/u/home/arezoo/wrf_hydro/PyWrfHydroCalib/core/calib_workflow.R 
#statLBEms = LBEms_function(chrt.obj.nona.nozeros$mod, chrt.obj.nona.nozeros$obs, chrt.obj.nona.nozeros$period)

# Function to be added to /glade/u/home/arezoo/wrf_hydro/PyWrfHydroCalib/core/calib_utils.R
LBEms_function <- function(mod, obs, period) { # Function reads in paired model and obs without any NAs.
  
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
  
  for (m in 1:12){ # For calculations based on monthly data
    oneSite_month <- subset(allData, allData$month== m) # Pull data for individual site's month
    LN3params_month <- parms(oneSite_month[,1:2])
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
  }
  
  # mixture moments from RAW data 
  mu_mix_O <- 1/12*sum(mu_mix_O_month)
  var_mix_O <- 1/12*sum(var_mix_O_month+mu_mix_O_month^2)-mu_mix_O^2
  mu_mix_S <- 1/12*sum(mu_mix_S_month)
  var_mix_S <- 1/12*sum(var_mix_S_month+mu_mix_S_month^2)-mu_mix_S^2
  
  mu_mix_SO <- 1/12*sum(mu_mix_SO_month)
  
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
