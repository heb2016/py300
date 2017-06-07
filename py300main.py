
#!/usr/bin/env python3

import logging


logging.basicConfig(filename='py300.log',
                    filemode='w',
                    format='%(asctime)s %(message)s',
                    level=logging.DEBUG)


from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
import psycopg2
import sys
import os
from os.path  import join, dirname
from datetime import date, timedelta, datetime
from urllib   import parse
from pandas   import DataFrame, ExcelWriter 
import pandas as pd


from dbconnection import dbconnection_module

##import email 
##import ftplib
##from email import email_module
##from ftplib import ftpfile_module


sqlfile = 'sql.sql'   
import_dir = os.path.abspath('./import')
import_name = 'import.xlsx'

out_dir = os.path.abspath('./output') 
out_edw = 'output.csv'
 
output_name = 'output-{0:%Y}-{0:%m}-{0:%d}'.format(date.today()) + '.xlsx'


def createConnString(host, dbname, msg_username, msg_password, port):
    username = str.join('', ('user=', msg_username))
    password = str.join('', ('password=', msg_password))
    dbconnstring = str.join(' ', (host, dbname, username, password, port))
    return (dbconnstring)


def getConnString():
    print('Please input your username:')
    msg_username = input().strip()
    print('Please input your password:')
    msg_password = input().strip()

    host= 'host=gpdb.prod.cdk.com'
    dbname = 'dbname=fm01'
    port = 'port=5432' 
    dbconnstring = createConnString(host, dbname, msg_username, msg_password, port)
    return(dbconnstring) 
     


def getDBData():

    sqldir = os.path.abspath('./sql') 
    dbconnstring =getConnString()
    myDB = dbconnection_module.MyDbConnection(sqldir, sqlfile, dbconnstring)
    dbdata = myDB.calledFromMain()
    dbrowcount = myDB.getDBRowcount() 
    return(dbrowcount)
 
 

## import Infiniti dealer file  Excel based on Susan's email attachment            
def get_import ():  
    print('Excel Import Log')
    import_loc = import_dir + '/' + import_name
    print('import_loc: ', import_loc)
    print('\n')
    
    df_excel = pd.read_excel(import_loc,  sheetname= 'Master',
           header=1,  
           parse_cols = "A:F,O,P" )   

    df_excel.columns= [
            'Dealer Code',  
            'Region', 
            'Area',
            'District', 
            'District_name',
            'Dealer_name',
            'web_id' ,
            'Target_Live_Date']    ## only select these columns

    if len(df_excel) ==0:
        print('Infiniti Dealer File has no record')    

    # print the column names
    print('Excel Import counts: ', len(df_excel) )
    print('Excel Import fields: ', df_excel.columns)
    print('Excel Import 5 Rows: ') 
    print(df_excel.head(5))
    print('\n')
 
    return(df_excel)




# create region_new column to sync wiht EDW request
def new_region(row):
   if row['Region'] == 62 :
      return ('62 - South')
   if row['Region'] == 72 :
      return ('72 - East')
   if row['Region'] == 82 :
      return ('82 - North')
   if row['Region'] == 92 : 
      return ('92 - West')
  

## merge EDW and Excel to get final
def merge_edw_excel():
    print('EDW & Excel Merge Log')  
    df_edw   = getDBData()    
    df_excel = get_import() 
##    df_merge_result_tmp =df_excel   ## -- test purpose 

    df_merge_result_tmp = pd.merge(df_edw, df_excel, how='inner', on=['web_id'])    
    print('df_merge_result_tmp', df_merge_result_tmp)
    print(type(df_merge_result_tmp['Region']))

    print(type(df_merge_result_tmp['District']))
        
    if len(df_merge_result_tmp)==0: 
        return (df_merge_result_tmp)


# Hierarchy Display Name  Hier Type   Web Id                  Region     Zone    Area    Dealer Code OEM Owner   Is Active
# Infiniti                Tier 3      infiniti-competition    72 - East   NY    1 - NYC   70016       infiniti    Is Active


    df_merge_result_tmp['District_name'] = df_merge_result_tmp['District_name'].str.replace('Georgia/TN','GA TN')   ## manually update George to sync EDW
    df_merge_result_tmp['District_name'] = df_merge_result_tmp['District_name'].str.replace('/',' ')                ## replace '/' to space ' ' to sync EDW
 
    df_merge_result_tmp['Region_new']=df_merge_result_tmp.apply (lambda row: new_region(row),axis=1)                ## manually update region to sync EDW
    df_merge_result_tmp['Area_New'] =df_merge_result_tmp['District'].map(str)+' - '+df_merge_result_tmp['District_name']   ## manually update Area to sync EDW

    
    del df_merge_result_tmp['Region']

    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'Region_new': 'Region'})
    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'web_id': 'Web Id'})
    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'Area': 'Zone'})
    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'Area_New': 'Area'})
    df_merge_result_tmp['Hierarchy Display Name'] = 'Infiniti'
    df_merge_result_tmp['Hier Type'] = 'Tier 3'
    df_merge_result_tmp['OEM Owner'] = 'infiniti'
    df_merge_result_tmp['Is Active'] = 'Y' 
 
    del df_merge_result_tmp['District_name']
    del df_merge_result_tmp['Dealer_name']
    del df_merge_result_tmp['District']



 ##   df_merge_result = df_merge_result_tmp[['Hierarchy Display Name', 'Hier Type', 'Web Id', 'Region', 'Zone', 'Area', 'Dealer Code', 
 ##                                               'OEM Owner', 'Is Active', 'Target_Live_Date']]   -- test purpose 

    df_merge_result = df_merge_result_tmp[['Hierarchy Display Name', 'Hier Type', 'Web Id', 'Region', 'Zone', 'Area', 'Dealer Code', 
                                                'OEM Owner', 'Is Active', 'Target_Live_Date', 'total_page_views', 'total_visits', 'total_vdp_views']]    
     
    print('EDW & Excel Merge counts: ', len(df_merge_result) )
    print('EDW & Excel Merge 5 Rows: ') 
    print(df_merge_result.head(5))
    print('\n') 
    return (df_merge_result)


## figure out output file format
def write_output():   
    print('Write Output Log\n')
    df_merge_result = merge_edw_excel()   
    
    if len(df_merge_result) == 0:
        print('There is no new dealer') 
        return (0)

## figure out out put formating, twist df_merge_result result to combine some columns to create output columns
#   out_merge_result = df_merge_result  twist

    out_merge_result = df_merge_result 

    out_loc = out_dir + '/' + output_name
    print('out_dir:', out_loc)
    print('\n')
    print('Output has records: ', len(out_merge_result))   
    print('Merged DataFrame out_merge_result: ')
    print(out_merge_result)
    print('\n')

    writer = pd.ExcelWriter(out_loc, engine='xlsxwriter', date_format='YYYY mmmm',
                            datetime_format='YYYY mmmm') 
    out_merge_result.to_excel(writer, sheet_name='Infiniti_New_Dealer') 
    writer.save()
    return (1)



  #  out_merge_result.to_csv(out_loc, sep = '\t', encoding = 'utf-8')
    print('\n')
  #  return (1)

def send_mail_app1(addr_from, subject, body_message, file_to_attach):
    '''
    Purpose: Prepare and send email message with attachments
    Preconditions: recipients is a well-formed list, file to attach exists, permissions are sufficient.
    Postconditions: none.
    Returns: none.
    '''
    msg = MIMEMultipart()
    msg['From'] = addr_from
    msg['To'] = ', '.join(RECIPIENTS)
    msg['Subject'] = subject
    body = body_message 

    msg.attach(MIMEText(body, 'plain')) 
    part = MIMEBase('application', 'octet-stream')  
    part.set_payload(open(file_to_attach, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=file_to_attach)
    msg.attach(part)

 #   server = smtplib.SMTP('dmlpsmtp.cdk.com') 
    server = smtplib.SMTP('ordpsmtp.cdk.com')   

    server.set_debuglevel(1)
    server.sendmail(addr_from, RECIPIENTS, msg.as_string())
    server.quit()


def send_mail_app0(addr_from, subject, body_message):
    '''
    Purpose: Prepare and send email message with attachments
    Preconditions: recipients is a well-formed list, file to attach exists, permissions are sufficient.
    Postconditions: none.
    Returns: none.
    '''
    msg = MIMEMultipart()
    msg['From'] = addr_from
    msg['To'] = ', '.join(RECIPIENTS)
    msg['Subject'] = subject
    body = body_message 
    msg.attach(MIMEText(body, 'plain'))

#   part = MIMEBase('application', 'octet-stream')
#   part.set_payload(open(file_to_attach, "rb").read())
#   encoders.encode_base64(part)
#   part.add_header('Content-Disposition', 'attachment', filename=file_to_attach)
#   msg.attach(part)

 #   server = smtplib.SMTP('dmlpsmtp.cdk.com') 
    server = smtplib.SMTP('ordpsmtp.cdk.com')   

    server.set_debuglevel(1)
    server.sendmail(addr_from, RECIPIENTS, msg.as_string())
    server.quit()


 
def send_email(output_flag):
    print('output_flag: ', output_flag)  
    try:
        if output_flag ==1: 
            print('Call-send_mail_app1')
            print('Message: Sending email with Attachment')
            os.chdir(out_dir)
            print('Message: Current working directory is: %s' % os.getcwd()) 
            print('\n')
            send_mail_app1('beatrice.he@cdk.com',
              'New',
              'The attached file contains New',
              output_name)
            print('\n')

        if output_flag ==0:
            print('Call-send_mail_app0')
            print('Message: Sending email without Attachment')
            print('\n')
            send_mail_app0('beatrice.he@cdk.com',
              'There is No New',
              'There is no New.' )
            print('\n')
        print('Message: Mail successfully sent')
    except ConnectionRefusedError as e:
        errno, strerror = e.args
        print('Error: Connection error {0}: {1}'.format(errno, strerror))
    except:
        print('Error: Unexpected error; check distribution, SMTP relay name, other settings:', sys.exc_info()[0])


def main():

    # Set up logging
    try: 
        output_flag=write_output()
        send_email(output_flag)
    except (SystemExit, KeyboardInterrupt):
        raise
 #   except Exception, e:
  #      logger.error('Failed to open file', exc_info=True) 
    return 1


if __name__ == '__main__':
 
    sys.exit(main())
