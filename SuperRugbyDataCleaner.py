'''                   SuperRugbyDataCleaner.py
Formats super rugby data into a format that can be used for making machine learning predictions
Created: 24/09/2016
    Loads data from the superRugbyPredictor MySQL database (locally hosted)
    
    Data to process, previous 3 years + current season, total won, total lost, points for, points against for both teams
    Also, is it a home game? total record against team, total home record against team, total away record team

Modified 24/09/2016:
    * Moved data cleaning scripts from SuperRugbyDownloader to this new file.
'''

import pandas as pd
import numpy as np
import re
import pymysql.cursors
import pymysql

# Makes dataframes display better on my PC.
pd.set_option('display.width', 190)
pd.options.display.max_columns = 50

# SQL Connection
#host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"            # Surface
host = "localhost"; user = "root"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"               # Desktop
connect = [host, user, passwd, db]


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

##################

    # normalizeData()
def normalizeData(dataFrame):
    # normalizes the data in seasonResults to the number of
    # games played and returns the new dataframe.
    for colName in ["BonusP", "Draw", "Lost", "Points", "PAgainst", "PDiff","PFor","Won"]:
        dataFrame[colName] = dataFrame[colName]/dataFrame["Played"]
    return dataFrame


# reSort()
def reSort(dataFrame, yearVect):
    # Changes the value of the position column for later years where the conference system was in place 
    # so that the teams are ranked properly.
    # Loop over years
    for year in yearVect:
        # Create a temporary dataframe containing only that years data, sorted by Points, Games Won, Point Differential
        tmp = dataFrame.loc[dataFrame.loc[:,"Year"] == year,:].sort_values(by=['Points', 'Won', 'PDiff'], ascending = False)
        # Iterate over the teams in tmp (which has been sorted) and change the corresponding position
        # in dataframe
        n=0
        for team in tmp["TeamName"]:
            n+=1
            dataFrame.loc[(dataFrame["TeamName"] == team) & (dataFrame["Year"] == year),"Position"] = n      #Probably need a year selecter in here too
    return dataFrame    

# createPastData()
def createPastData(connect):
    # Reads the data from the seasonResults table, normalizes it, then adds columns
    # that give the data for the teams results the previous 2 years, e.g. Win_1, Win_2.
    # This data is then added to the seasonResultsPastResults table.
    # Read in data
    sqlQuery = '''SELECT * FROM seasonResults'''
    dataFrame = readDatabase(connect, sqlQuery)
    # Clean the Position column.
    dataFrame = reSort(dataFrame, range(2012,2017))
    # Give data frame containing normalized data
    dataFrameNorm = normalizeData(dataFrame)
    # Set up a temporary dataframe
    dataFrameTmp = dataFrameNorm
    # Column names to iterate over
    colNames =["Won", "Draw", "Lost", "PFor", "PAgainst", "PDiff", "BonusP", "Points"]
    # Loop over the different columns we want to add
    for colName in colNames:
        # Loop over the rows in the dataFrame adding the new columns from past data
        for n in xrange(len(dataFrameTmp)):
            team = dataFrameTmp.loc[n, "TeamName"]
            year = dataFrameTmp.loc[n, "Year"]
            # The previous two years data
            tmpDat = dataFrameNorm.loc[(dataFrameNorm["Year"]==year-1) & (dataFrameNorm["TeamName"]==team),colName].values
            tmpDat2 = dataFrameNorm.loc[(dataFrameNorm["Year"]==year-2) & (dataFrameNorm["TeamName"]==team),colName].values
            # If the data doesn't exist, i.e. before the competition began, or the team didn't play the last year set as NaN
            if len(tmpDat) == 0:
                tmpDat = np.nan
            if len(tmpDat2) == 0:
                tmpDat2 = np.nan
            # Add to the dataFrame
            dataFrameTmp.loc[(dataFrameTmp["Year"]==year) & (dataFrameTmp["TeamName"]==team),colName+"_1"] = tmpDat
            dataFrameTmp.loc[(dataFrameTmp["Year"]==year) & (dataFrameTmp["TeamName"]==team),colName+"_2"] = tmpDat2
    #print dataFrameTmp
    # Add to database seasonResultsPastResults
    addToDatabase(dataFrameTmp, 'seasonResultsPastResults', connect)



tf2 = 0
#Set to True to remove the table
if tf2:
    removeTable('seasonResultsPastResults', connect)
#Set to True to create the table
if tf2:
    rowList = 'BonusP FLOAT, BonusP_1 FLOAT, BonusP_2 FLOAT, \
    Draw FLOAT, Draw_1 FLOAT, Draw_2 FLOAT, Played INT, Lost FLOAT, Lost_1 FLOAT, Lost_2 FLOAT, \
    Points FLOAT, Points_1 FLOAT, Points_2 FLOAT, PAgainst FLOAT, PAgainst_1 FLOAT, PAgainst_2 FLOAT, \
    PDiff FLOAT, PDiff_1 FLOAT, PDiff_2 FLOAT, \
    PFor FLOAT, PFor_1 FLOAT, PFor_2 FLOAT, Position INT, \
    TeamName TEXT, Won FLOAT, Won_1 FLOAT, Won_2 FLOAT, Year INT'
    createTable('seasonResultsPastResults', rowList, connect)
if tf2:
    createPastData(connect)
    
    
## Temporary code for testing
sqlQuery = '''SELECT * FROM seasonResultsPastResults'''    #283 rows total
#sqlQuery = '''SELECT TeamName, Position, Won, Points FROM seasonResultsPastResults WHERE Year >= 2015 ORDER BY Year, Position''' 
##sqlQuery = '''SELECT * FROM seasonResults WHERE TeamName LIKE '%kin%' OR TeamName LIKE '%souk%' '''
#sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total                
dataFrame = readDatabase(connect, sqlQuery)
print dataFrame