
#!/usr/bin/env python3

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
import psycopg2 

import sys
import os
from os.path  import join, dirname
from pandas   import DataFrame
import pandas as pd
import psycopg2.extensions

# Get database connection 

class MyDbConnection:
    def __init__ (self, sqldir, sqlfile, connstring):
        self.connstring = connstring 
        self.sqlloc =  sqldir+ '/' + sqlfile
        self.sqlstring = ""
        self.dbrowcount = 0
        self.dbdata =[]
     
    def getDBRowcount(self):
        return self.dbrowcount  
  
    def getDBData(self):
        return self.dbdata  

    def getsqlstring(self):
        self.sqlstring = open(self.sqlloc).read()

    def getDbDataFrame (self):
        
        try:
            print('connstring:', self.connstring)
            connection = psycopg2.connect(self.connstring) 

            print('EDW connection Break', connection) 
            cursor = connection.cursor()  
            print('EDW cursor Break',cursor) 

            cursor.execute(self.sqlstring)
            names = [ x[0] for x in cursor.description]
            records = cursor.fetchall()  
            self.dbdata =  DataFrame(records, columns = names)
            self.dbrowcount= len(self.dbdata)

        except psycopg2.InterfaceError as e:
            print('InterfaceError')

        except psycopg2.Error as e:
            print('Unable to connect!')

            print ('pgcode:', e.pgcode)
            print ('pgerror:', e.pgerror)
            print ('cursor:', e.cursor)
            print ('message_detail:', e.diag.message_detail) 
 
        finally:  
            if cursor is not None:
                cursor.close()
                connection.close()   

    def calledFromMain (self):

        try:
            self.getsqlstring()  # doTheFirstPart()
            self.getDbDataFrame()
            self.getDBRowcount()
            self.getDBData()

        except:
            if self.dbrowcount ==0:
                print('DB Return Dataframe is Empty')
                return
            
        finally:
            print('DB Return Records: ', self.dbrowcount)       
            print('dbdata:',self.dbdata)

 