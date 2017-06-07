
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
    sqlfile = 'infiniti_webpage_render.sql'   ## argv
    dbconnstring =getConnString()
    myDB = dbconnection_module.MyDbConnection(sqldir, sqlfile, dbconnstring)
    dbdata = myDB.calledFromMain()
    dbrowcount = myDB.getDBRowcount() 
    return(dbrowcount)


def main():
    dbrowcount=getDBData()
    print('dbrowcount: ', dbrowcount)
    if dbrowcount >0:
        return 1
    if dbrowcount ==0:
        return 0

 
if __name__ == '__main__':
 
    sys.exit(main())