'''                   SuperRugbyPredictor_v1.py
Creates machine learning models to predict the results of the Super Rugby Competition.
Created: 10/05/2016
    Loads data from the superRugbyPredictor MySQL database (locally hosted)

'''

import numpy as np
import pandas as pd
import sklearn as sk
import re
import pymysql.cursors
import pymysql

# readDatabase(connect)
def readDatabase(connect, sqlQuery):
    # Uses the query sqlQuery to read the database specified in connect
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])               
    dataFrame = pd.read_sql(sqlQuery, conn)
    conn.close()
    return dataFrame 


host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"
connect = [host, user, passwd, db]

sqlQuery = '''SELECT * FROM seasonResults'''    #283 rows total
#sqlQuery = '''SELECT * FROM seasonResults WHERE Year = 1996''' 
#sqlQuery = '''SELECT * FROM seasonResults WHERE TeamName LIKE '%kin%' OR TeamName LIKE '%souk%' '''
#sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total
rugbyDat = readDatabase(connect,sqlQuery)
rugbyDat 