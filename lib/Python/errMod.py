# Library for handling error trapping and reporting for the user.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import sys
from email.mime.text import MIMEText
import smtplib
import shutil
import pwd
import os
import glob
import subprocess

import warnings
warnings.filterwarnings("ignore")

uid = pwd.getpwuid(os.getuid()).pw_name

def errOut(jobData):
    # Error function for handling communicating error messages
    # to user. If email was provided, an error email
    # is sent out based on the job name. A custom error message
    # passed in by the function will go into the email.

    msgContent = jobData.errMsg
    
    if jobData.email:
        # Send error email out
        msg = MIMEText(msgContent)
        emailTitle = "Errors in WRF-Hydro Calibration for Job: " + str(jobData.jobID) + " for Owner: " + str(jobData.owner)
        msg['Subject'] = emailTitle
        msg['From'] = jobData.email
        msg['To'] = jobData.email
        try:
            s = smtplib.SMTP('localhost',1025)
            s.sendmail(jobData.email,[jobData.email],msg.as_string())
            s.quit()
        except:
            print msgContent
            print "ERROR: Unable to send email from workflow."
            print "SMTP on a port needs to be activated from this host machine."
    #if jobData.slackObj:
    #    msg1 = "ERROR in Job: " + str(jobData.jobID) + " for Owner: " + str(jobData.owner)
    #    jobData.slackObj.chat.post_message(str(jobData.slChan),msg1,as_user=str(jobData.slUser))
    #    jobData.slackObj.chat.post_message(str(jobData.slChan),jobData.errMsg,as_user=str(jobData.slUser))
    if not jobData.email and not jobData.slackObj:
        print msgContent
    sys.exit(1)
        
def wipeJobDir(jobData):
    # Generic function to remove job directory that was not successfully 
    # created during initialization routines.
    jobDir = jobData.outDir + "/" + jobData.jobName
    try:
        shutil.rmtree(jobDir)
    except:
        print "ERROR: Failure to remove: " + jobDir + " Please remove manually."
        raise
        
def removeOutput(jobData,runDir):
    """
    Generic function to clean up wrfHydro output. This is used specifically
    between calibration simulations.
    """
    filesCheck = glob.glob(runDir + "/diag_hydro.*")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/diag_hydro.*"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove hydro diagnostic files from: " + runDir
            raise
            
    filesCheck = glob.glob(runDir + "/*.err")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.err"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove error diagnostic files from: " + runDir
            raise
            
    filesCheck = glob.glob(runDir + "/*.out")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.out"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove misc diagnostic files from: " + runDir
            raise
            
    filesCheck = glob.glob(runDir + "/*.LDASOUT_DOMAIN1")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.LDASOUT_DOMAIN1"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove LDASOUT files from: " + runDir
            raise
    
    filesCheck = glob.glob(runDir + "/*.CHRTOUT_DOMAIN1")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.CHRTOUT_DOMAIN1"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove CHRTOUT files from: " + runDir
    
    filesCheck = glob.glob(runDir + "/HYDRO_RST.*")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/HYDRO_RST.*"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove Hydro restart files from: " + runDir
    
    filesCheck = glob.glob(runDir + "/RESTART.*_DOMAIN1")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/RESTART.*_DOMAIN1"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove LSM restart files from: " + runDir
    
    if os.path.isfile(runDir + "/namelist.hrldas"):
        try:
            os.remove(runDir + "/namelist.hrldas")
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + runDir + "/namelist.hrldas"
            raise
            
    if os.path.isfile(runDir + "/hydro.namelist"):
        try:
            os.remove(runDir + "/hydro.namelist")
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + runDir + "/hydro.namelist"
            raise
            
def cleanCalib(jobData,workDir,runDir):
    """
    Generic function to cleanup calibration-related output, such as text files,
    COMPLETE flags, etc in preparation for the next iteration.
    """
    
    calibCompleteFlag = workDir + "/CALIB_ITER.COMPLETE"
    calibTbl = workDir + "/params_new.txt"
    statsTbl = workDir + "/params_stats.txt"
    
    if os.path.isfile(calibCompleteFlag):
        try:
            os.remove(calibCompleteFlag)
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + calibCompleteFlag
            raise
            
    if os.path.isfile(calibTbl):
        try:
            os.remove(calibTbl)
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + calibTbl
            raise
            
    if os.path.isfile(statsTbl):
        try:
            os.remove(statsTbl)
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + statsTbl
            raise
            
def scrubParams(jobData,runDir):
    """
    Generic function to remove parameter files generated after calibration.
    This is done to remove the risk of a model being ran with the improper
    file. It also allows the workflow to remove model output prior to a 
    simulation but not the new parameter files needed.
    """
    fullDomFile = runDir + "/Fulldom.nc"
    hydroTbl = runDir + "/HYDRO_TBL_2D.nc"
    soilFile = runDir + "/soil_properties.nc"
    gwFile = runDir + '/GWBUCKPARM.nc'

    if os.path.isfile(fullDomFile):
        try:
            os.remove(fullDomFile)
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + fullDomFile
            raise
    
    if os.path.isfile(hydroTbl):
        try:
            os.remove(hydroTbl)
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + hydroTbl
            raise
            
    if os.path.isfile(soilFile):
        try:
            os.remove(soilFile)
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + soilFile
            raise
            
    if os.path.isfile(gwFile):
        try:
            os.remove(gwFile)
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + gwFile
            raise

def cleanRunDir(jobData,runDir):
    """
    Generic function to clear out any misc old diagnostic files that may be around.
    """
    filesCheck = glob.glob(runDir + "/diag_hydro.*")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/diag_hydro.*"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove hydro diagnostic files from: " + runDir
            raise
            
def sendMsg(jobData):
    # Generic function for sending general messages out. This could be useful
    # if there is a desire to send out messages for updates, etc. Not necessarily
    # for error messages.
    
    msgContent = jobData.genMsg
    
    if jobData.email:
        # Send error email out
        msg = MIMEText(msgContent)
        emailTitle = "General Message For Job: " + str(jobData.jobID) + " for Owner: " + str(jobData.owner)
        msg['Subject'] = emailTitle
        msg['From'] = jobData.email
        msg['To'] = jobData.email
        try:
            s = smtplib.SMTP('localhost')
            s.sendmail(jobData.email,[jobData.email],msg.as_string())
            s.quit()
        except:
            print msgContent
            print "ERROR: Unable to send email from workflow."
            print "SMTP on a port needs to be activated from this host machine."
    #if jobData.slackObj:
    #    msg1 = "MESSAGE for Job: " + str(jobData.jobID) + " for Owner: " + str(jobData.owner)
    #    jobData.slackObj.chat.post_message(str(jobData.slChan),msg1,as_user=str(jobData.slUser))
    #    jobData.slackObj.chat.post_message(str(jobData.slChan),jobData.genMsg,as_user=str(jobData.slUser))
    if not jobData.email and not jobData.slackObj:
        print msgContent
        
def CleanSpinup(jobData,runDir):
    """
    Generic function to clean up wrfHydro output from the spinup. This will not
    remove RESTART files as those are needed by the calibrations.
    """
    filesCheck = glob.glob(runDir + "/diag_hydro.*")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/diag_hydro.*"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove hydro diagnostic files from: " + runDir
            raise
            
    filesCheck = glob.glob(runDir + "/*.err")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.err"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove error diagnostic files from: " + runDir
            raise
            
    filesCheck = glob.glob(runDir + "/*.out")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.out"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove misc diagnostic files from: " + runDir
            raise
            
    filesCheck = glob.glob(runDir + "/*.LDASOUT_DOMAIN1")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.LDASOUT_DOMAIN1"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove LDASOUT files from: " + runDir
            raise
    
    filesCheck = glob.glob(runDir + "/*.CHRTOUT_DOMAIN1")
    
    if len(filesCheck) > 0:
        cmd = "rm " + runDir + "/*.CHRTOUT_DOMAIN1"
        try:
            subprocess.call(cmd,shell=True)
        except:
            jobData.errMsg = "ERROR: Unable to remove CHRTOUT files from: " + runDir
            
    if os.path.isfile(runDir + "/namelist.hrldas"):
        try:
            os.remove(runDir + "/namelist.hrldas")
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + runDir + "/namelist.hrldas"
            raise
            
    if os.path.isfile(runDir + "/hydro.namelist"):
        try:
            os.remove(runDir + "/hydro.namelist")
        except:
            jobData.errMsg = "ERROR: Failure to remove: " + runDir + "/hydro.namelist"
            raise
            
def check_pid(pid):
     """ Check For the existence of a unix pid. """
     try:
         os.kill(pid, 0)
     except OSError:
         return False
     else:
         return True