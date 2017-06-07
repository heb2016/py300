
 
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import sys
import os
from os.path  import join, dirname
from pandas   import DataFrame


# 
smtpserver=ordpsmtp.cdk.com
addr_from
subject, body_message, file_to_attach

class MyEmail:
    def __init__ (self, smtpserver, RECIPIENTS, addr_from, subject, body_message, file_to_attach):  
        self.smtpserver = smtpserver 
        self.RECIPIENTS =  RECIPIENTS
        self.addr_from = addr_from
        self.subject = subject
        self.body_message =body_message
        self.file_to_attach = file_to_attach

    def send_mail_app1(self):
    
        msg = MIMEMultipart()
        msg['From'] = self.addr_from
        msg['To'] = ', '.join(self.RECIPIENTS)
        msg['Subject'] = self.subject
        body = self.body_message 

        msg.attach(MIMEText(body, 'plain')) 
        part = MIMEBase('application', 'octet-stream')  
        part.set_payload(open(self.file_to_attach, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=self.file_to_attach)
        msg.attach(part)

 #   server = smtplib.SMTP('dmlpsmtp.cdk.com') 
        server = smtplib.SMTP( self.smtpserver)   

        server.set_debuglevel(1)
        server.sendmail(self.addr_from, RECIPIENTS, msg.as_string())
        server.quit()


    def send_mail_app0(self):
    '''
    Purpose: Prepare and send email message with attachments
    Preconditions: recipients is a well-formed list, file to attach exists, permissions are sufficient.
    Postconditions: none.
    Returns: none.
    '''
        msg = MIMEMultipart()
        msg['From'] = self.addr_from
        msg['To'] = ', '.join(self.RECIPIENTS)
        msg['Subject'] = self.subject
        body = self.body_message 
        msg.attach(MIMEText(self.body, 'plain'))

#   part = MIMEBase('application', 'octet-stream')
#   part.set_payload(open(file_to_attach, "rb").read())
#   encoders.encode_base64(part)
#   part.add_header('Content-Disposition', 'attachment', filename=file_to_attach)
#   msg.attach(part)

 #   server = smtplib.SMTP('dmlpsmtp.cdk.com') 
        server = smtplib.SMTP(self.smtpserver)   

        server.set_debuglevel(1)
        server.sendmail(self.addr_from, self.RECIPIENTS, msg.as_string())
        server.quit()


    def calledFromMain (self):

        try:
            self.send_mail_app1()  # doTheFirstPart()
            self.send_mail_app0() 
 
