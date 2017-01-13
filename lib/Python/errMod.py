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
        emailTitle = "Errors in NWM Calibration for Job: " + jobData.jobName
        msg['Subject'] = emailTitle
        msg['From'] = jobData.email
        msg['To'] = jobData.email
        s = smtplib.SMTP('localhost')
        s.sendmail(jobData.email,[jobData.email],msg.as_string())
        s.quit()
        sys.exit(1)
    elif jobData.slackObj:
        msg1 = "ERROR in Job: " + jobData.jobName
        chStrTmp = "#" + str(jobData.slChan)
        print chStrTmp
        jobData.slackObj.chat.post_message(str(jobData.slChan),msg1,as_user='Logan Karsten')
        jobData.slackObj.chat.post_message(str(jobData.slChan),jobData.errMsg,as_user='Logan_Karsten')
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