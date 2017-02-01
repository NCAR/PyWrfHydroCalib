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
        emailTitle = "Errors in NWM Calibration for Job: " + str(jobData.jobID)
        msg['Subject'] = emailTitle
        msg['From'] = jobData.email
        msg['To'] = jobData.email
        s = smtplib.SMTP('localhost')
        s.sendmail(jobData.email,[jobData.email],msg.as_string())
        s.quit()
        sys.exit(1)
    elif jobData.slackObj:
        msg1 = "ERROR in Job: " + str(jobData.jobID)
        jobData.slackObj.chat.post_message(str(jobData.slChan),msg1,as_user=str(jobData.slUser))
        jobData.slackObj.chat.post_message(str(jobData.slChan),jobData.errMsg,as_user=str(jobData.slUser))
        sys.exit(1)
    else:
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
            
def sendMsg(jobData):
    # Generic function for sending general messages out. This could be useful
    # if there is a desire to send out messages for updates, etc. Not necessarily
    # for error messages.
    
    msgContent = jobData.genMsg
    
    if jobData.email:
        # Send error email out
        msg = MIMEText(msgContent)
        emailTitle = "General Message For Job: " + str(jobData.jobID)
        msg['Subject'] = emailTitle
        msg['From'] = jobData.email
        msg['To'] = jobData.email
        s = smtplib.SMTP('localhost')
        s.sendmail(jobData.email,[jobData.email],msg.as_string())
        s.quit()
    elif jobData.slackObj:
        msg1 = "MESSAGE for Job: " + str(jobData.jobID)
        jobData.slackObj.chat.post_message(str(jobData.slChan),msg1,as_user=str(jobData.slUser))
        jobData.slackObj.chat.post_message(str(jobData.slChan),jobData.errMsg,as_user=str(jobData.slUser))
    else:
        print msgContent
