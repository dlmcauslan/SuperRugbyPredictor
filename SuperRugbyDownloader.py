'''                   SuperRugbyDownloader.py
Downloads historical super rugby results from wikipedia.
Created: 04/05/2016
    Implemented downloading of 1996 data
    Saves data in MySQL database

TO DO:
    Put in a loop to download data from all years
'''
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import urllib2
import re
import pymysql.cursors
import pymysql


def downloader(year, connect):
    #Creates dataframe
    colNames =["Position", "TeamName", "GamesPlayed", "Won", "Draw", "Lost", 
            "PointsFor", "PointsAgainst", "PointsDifferential", "BonusPoints", 
            "Points"]
    rugbyData = pd.DataFrame(dict.fromkeys(colNames,[]))


    URLPage = 'https://en.wikipedia.org/wiki/'+str(year)+'_Super_12_season'     
    # Creates soup and dowloads data
    soup = BeautifulSoup(urllib2.urlopen(URLPage).read())
    # The table layout is different depending on the year
    if year<=2002:
        table = soup.find('table','wikitable sortable')
    elif year>=2003:
        table = soup.find_all('table','wikitable')[1]
    # Iterates over row and column of the table
    for row in table('tr'):
        tempRow=[]
        for col in row.findAll("td"):
            # Dealing with cases where col.string == None
            if col.string == None:
                # The team name is in a different format so requires the if statement
                if col.find_all('a') != []:
                    tempRow.append(col.find_all('a')[1].string)
                # In 2001 the Draws column is empty, so set as 0
                else:
                    tempRow.append(0)
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
    addToDatabase(rugbyData, connect)
    #print rugbyData


#createTable()
def createTable(tableName, connect):
    # function which creates the table (tableName) in the database db
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
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
def removeTable(tableName, connect):
    # function which removes the table (tableName) from the database db
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
    cursor = conn.cursor()        
    #Remove database
    sql_command = """ DROP TABLE IF EXISTS seasonResults """ 
    cursor.execute(sql_command)
    conn.commit()
    conn.close()
    print "Database removed" 

                
# addToDatabase()
def addToDatabase(dataFrame, connect):
    # function which adds scraped data to database db
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
    dataFrame.to_sql(name = 'seasonResults', con = conn, flavor ='mysql', if_exists = 'append', index=False)       
    conn.commit()
    conn.close()

def downloadAll(connect):
    years = range(1996,2006,1)
    for year in years:
        print year
        downloader(year, connect)

host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"
connect = [host, user, passwd, db]

#Set to True to remove the table
if 0:
    removeTable('seasonResults', connect)
#Set to True to create the table
if 0:
    createTable('seasonResults', connect)
#Set to true to download data                                                 
if 0:                                                                
    #downloader(1996, connect)
    downloadAll(connect)




# Temporary code to read from database
conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
sqlQuery = '''SELECT * FROM seasonResults'''                    
dataFrame = pd.read_sql(sqlQuery, conn)
conn.close()
dataFrame 
