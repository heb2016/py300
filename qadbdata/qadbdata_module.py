
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


## Workflow checker
etl_sql <- paste0("select
                  w.click
                  , case
                  when w.click > 0 then 'ready'
                  else 'wait'
                  end as status

                  from (

                  select
                  max(case when upper(dw.workflow_name)='WF_CLICKSTREAM_AGGREGATES' then f.workflow_end_time else NULL end) as click
                  from wca_app.fact_workflow_events f
                  join wca_app.dim_workflows dw on f.dim_workflow_key = dw.dim_workflow_key
                  join wca_app.dim_dates dd on f.dim_date_key = dd.dim_date_key
                  where 1 = 1
                  and dd.cal_date = (current_date)
                  and f.dim_status_code_key = 1
                  ) w
                  ", collapse = " ")

# set up loop
# check <- dbGetQuery(fm01, sql)
etl_check <- dbGetQuery(fm01, etl_sql)
bk <- 36
msg <- "stopped checking"
# loop untill workflows complete or 3 hours. Which ever comes firts
for (i in 1:bk) {
    if((etl_check$status == "wait")) {
        Sys.sleep(300)
        etl_status <- paste0("etl status: ",etl_check$status)
        print(etl_status)
        etl_check <- dbGetQuery(fm01, etl_sql)
        i <- i+1
    }
    else {
        i <- bk
        msg <- "Complete"
    }
}




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

 