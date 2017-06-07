#!/usr/bin/env python3
import pytest


import logging
import traceback
# create logger with 'test_name'
logfile = 'test_log.log'
logger = logging.getLogger('test_log')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('test_log.log')
fh.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler())



import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import main




def test_createConnString():
    
    host= 'host=test.com'
    dbname = 'dbname=test'
    port = 'port=test'
    msg_username ='test'
    msg_password = 'test'
    expect_ConnString ='host=test.com dbname=test user=test password=test port=test'
    
    myConnString = main.createConnString(host, dbname, msg_username, msg_password, port)
    print('myConnString:', myConnString)
    assert  myConnString==expect_ConnString

#def test_getConnstringsample():
#   myConnString = main.createConnString


def test_sqlstring():
    assert sqlstring is not '' 'SQL function return sql string'


def test_getDbDataFrame():
    assert c==c
    assert dbrowcount>0,  'DB return Data'



if __name__ == '__main__':
    
    test_createConnString()
    test_getsql()
    test_getDbDataFrame()
