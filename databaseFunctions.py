'''                   databaseFunctions.py
Formats super rugby data into a format that can be used for making machine learning predictions
Created: 24/09/2016
    Python script that holds the functions for creating, reading and loading from 
    MySQL databases.
'''

import pymysql.cursors
import pymysql
import pandas as pd

#createTable()
def createTable(tableName, rowList, connect):
    # function which creates the table (tableName) in the database db
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
    cursor = conn.cursor()        
    #Created database
    sql_command = """ CREATE TABLE {} ({}) """.format(tableName, rowList)
    print sql_command    
    cursor.execute(sql_command)
    conn.commit()
    conn.close()
    print "Database created" 

        
#removeTable()
def removeTable(tableName, connect):
    # function which removes the table (tableName) from the database db
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
    cursor = conn.cursor()        
    #Remove database
    sql_command = """ DROP TABLE IF EXISTS {} """.format(tableName) 
    cursor.execute(sql_command)
    conn.commit()
    conn.close()
    print "Database removed" 

        
# addToDatabase()
def addToDatabase(dataFrame, tableName, connect):
    # function which adds scraped data to database db
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
    dataFrame.to_sql(name = tableName, con = conn, flavor ='mysql', if_exists = 'append', index=False)       
    conn.commit()
    conn.close()

        
# readDatabase(connect, sqlQuery)
def readDatabase(connect, sqlQuery):
    # Uses the query sqlQuery to read the database specified in connect
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])               
    dataFrame = pd.read_sql(sqlQuery, conn)
    conn.close()
    return dataFrame
    
