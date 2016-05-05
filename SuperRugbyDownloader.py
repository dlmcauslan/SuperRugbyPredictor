'''                   SuperRugbyDownloader.py
Downloads historical super rugby results from wikipedia.
Created: 04/05/2016
    Implemented downloading of 1996 data
    Saves data in MySQL database

TO DO:
    Change location of database so it is saved in project folder
    Put in a loop to download data from all years
'''
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import urllib2
import re
import pymysql.cursors
import pymysql


def downloader(year, host, user, passwd, db):
    #Creates dataframe
    colNames =["Position", "TeamName", "GamesPlayed", "Won", "Draw", "Lost", 
            "PointsFor", "PointsAgainst", "PointsDifferential", "BonusPoints", 
            "Points"]
    rugbyData = pd.DataFrame(dict.fromkeys(colNames,[]))


    URLPage = 'https://en.wikipedia.org/wiki/1996_Super_12_season'     
    # Creates soup and dowloads data
    soup = BeautifulSoup(urllib2.urlopen(URLPage).read())
    table = soup.find('table','wikitable sortable')
    # Iterates over row and column of the table
    for row in table('tr'):
        tempRow=[]
        for col in row.findAll("td"):
            # The team name is in a different format so requires the if statement
            if col.string == None:
                tempRow.append(col.find_all('a')[1].string)
            # The negative sign is encoded in a way that needs to be changed
            elif re.findall(u'[\u2212]',col.string) != []:
                tempRow.append(-int(re.findall(u'\d{1,3}',col.string)[0]))
            else:
                tempRow.append(int(col.string))
        # Don't include the first, empty row. Add the rest into a dataframe.
        if tempRow !=[]:
            rugbyData = rugbyData.append(pd.DataFrame([tempRow], columns=colNames), ignore_index=True)
    
    # Append a year column
    yearCol=[]
    [yearCol.append(year) for n in xrange(len(rugbyData))]
    rugbyData["Year"] = yearCol  
    # Add to SQL database
    addToDatabase(rugbyData, host, user, passwd, db)
    #print rugbyData


#createTable()
def createTable(tableName, host, user, passwd, db):
    # function which creates the table (tableName) in the database db
    conn = pymysql.connect(host, user, passwd, db)  
    cursor = conn.cursor()        
    #Created database
    sql_command = """
    CREATE TABLE seasonResults (BonusPoints INT, Draw INT, GamesPlayed INT, Lost INT, 
    Points INT, PointsAgainst INT, PointsDifferential INT, PointsFor INT, Position INT,
    TeamName TEXT, Won INT, Year INT) 
    """    
    cursor.execute(sql_command)
    conn.commit()
    conn.close()
    print "Database created" 

        
#removeTable()
def removeTable(tableName, host, user, passwd, db):
    # function which removes the table (tableName) from the database db
    conn = pymysql.connect(host, user, passwd, db)  
    cursor = conn.cursor()        
    #Remove database
    sql_command = """ DROP TABLE IF EXISTS seasonResults """ 
    cursor.execute(sql_command)
    conn.commit()
    conn.close()
    print "Database removed" 

                
# addToDatabase()
def addToDatabase(dataFrame, host, user, passwd, db):
    # function which adds scraped data to database db
    conn = pymysql.connect(host, user, passwd, db)
    dataFrame.to_sql(name = 'seasonResults', con = conn, flavor ='mysql', if_exists = 'append', index=False)       
    conn.commit()
    conn.close()



host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"

#Set to True to remove the table
if 0:
    removeTable('seasonResults', host, user, passwd, db)
#Set to True to create the table
if 0:
    createTable('seasonResults', host, user, passwd, db)
#Set to true to download data                                                 
if 0:                                                                
    downloader(1996, host, user, passwd, db)



# Temporary code to read from database
conn = pymysql.connect(host="localhost", user="dlmsql", passwd="DLMPa$$word", db="superRugbyPredictor")

sqlQuery = '''SELECT * FROM seasonResults'''                    
dataFrame = pd.read_sql(sqlQuery, conn)
conn.close()
dataFrame 
