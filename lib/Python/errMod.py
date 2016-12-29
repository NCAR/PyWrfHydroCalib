# Library for handling error trapping and reporting for the user.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import sys
from email.mime.text import MIMEText
import smtplib

def errOut(jobData,msgContent):
    # Error function for handling communicating error messages
    # to user. If email was provided, an error email
    # is sent out based on the job name. A custom error message
    # passed in by the function will go into the email.

    if jobData.report == 1:
        # Send error email out
        msg = MIMEText(msgContent)
        emailTitle = "Errors in NWM Calibration for Job: " + jobData.jobName[0]
        msg['Subject'] = emailTitle
        msg['From'] = jobData.email
        msg['To'] = jobData.email
        s = smtplib.SMTP('localhost')
        s.sendmail(jobData.email,[jobData.email],msg.as_string())
        s.quit()
        sys.exit(1)
    else:
        print msgContent
        sys.exit(1)