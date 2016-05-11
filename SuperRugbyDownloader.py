'''                   SuperRugbyDownloader.py
Downloads historical super rugby results from wikipedia.
Created: 04/05/2016
    Implemented downloading of 1996 data
    Saves data in MySQL database
Modified: 09/05/2016
    Downloads data from all years. Had to add many exception because different
    years have the data formatted slightly differently
Modified: 10/05/2016
    Implemented function to clean the TeamName data as different years data had
    slightly different team names. And several of the south african teams have
    changed their name over the years.
'''

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
    
    # Gets different URLs because the competition changed its name over the years.
    if year <= 2005:
        URLPage = 'https://en.wikipedia.org/wiki/{}_Super_12_season'.format(year)
    elif year >= 2006 and year<=2010:
        URLPage = 'https://en.wikipedia.org/wiki/{}_Super_14_season'.format(year)
    elif year >= 2011:
        URLPage = 'https://en.wikipedia.org/wiki/{}_Super_Rugby_season'.format(year)
    # Creates soup and dowloads data
    soup = BeautifulSoup(urllib2.urlopen(URLPage).read(),"lxml")
    # The table layout is different depending on the year
    if year<=2002 or (year>=2006 and year <= 2010):
        table = soup.find('table','wikitable sortable')
    elif year>=2003 and year<=2005:
        table = soup.find_all('table','wikitable')[1]
    elif year>=2011 and year<=2013:
        table = soup.find_all('table','wikitable')[3]
    elif year>=2014 and year<=2015:
        table = soup.find_all('table','wikitable')[0]
    elif year>=2016:
        table = soup.find_all('table','wikitable')[1:3]
    rowCount = 0
        
    # Iterates over row and column of the table - In 2016 there are two seperate tables so need an extra loop
    if year >= 2016:
        for tab in table:
            for row in tab('tr'):
                tempRow=[]      
                # Don't read in title row for years later than 2011
                if (rowCount in range(0,2)):
                    pass
                else:
                    colno = 0
                    for col in row.findAll("td"):
                        # Dealing with columns years later than 2011
                        if year >=2014 and colno in [9,10]:
                            pass
                        elif year >=2014 and colno == 11:
                            tmp = int(col.string)
                        elif year >=2014 and colno == 12:
                            tempRow.append(int(col.string)+tmp)               
                        # Dealing with cases where col.string == None
                        elif col.string == None:
                            # The team name is in a different format so requires the if statement
                            if col.find_all('a') != []:
                                tempRow.append(col.find_all('a')[1].string)
                        # The negative sign is encoded in a way that needs to be changed
                        elif re.findall(u'[\u2212]',col.string) != []:
                            tempRow.append(-int(re.findall(u'\d{1,3}',col.string)[0]))
                        else:
                            tempRow.append(int(col.string))
                        # Increment colno
                        colno+=1
                # Don't include the first, empty row. Add the rest into a dataframe.
                if tempRow !=[]:
                    rugbyData = rugbyData.append(pd.DataFrame([tempRow], columns=colNames), ignore_index=True)
                rowCount+=1
    else:
        for row in table('tr'):
            tempRow=[]  
            # Don't read in title row for years later than 2011
            if year >= 2011 and rowCount == 0:
                pass
            elif year >= 2014 and (rowCount in range(1,4) or rowCount>=20):
                pass
            else:
                colno = 0
                for col in row.findAll("td"):
                    # Dealing with columns years later than 2011
                    # Don't need number of byes data
                    if year>=2011 and year <= 2013 and colno == 6:
                        pass
                    # Sum the different types of bonus point together
                    elif year==2011 and colno == 10:
                        tmp = int(col.string)
                    elif year==2011 and colno == 11:
                        tempRow.append(int(col.string)+tmp)
                    # In 2012, 2 more unnecesary columns were added, so remove these
                    elif year in [2012,2013] and colno in [10,11]:
                        pass
                    elif year in [2012,2013] and colno == 12:
                        tmp = int(col.string)
                    elif year in [2012,2013] and colno == 13:
                        tempRow.append(int(col.string)+tmp)
                    elif year >=2014 and colno in [9,10]:
                        pass
                    elif year >=2014 and colno == 11:
                        tmp = int(col.string)
                    elif year >=2014 and colno == 12:
                        tempRow.append(int(col.string)+tmp)               
                    # Dealing with cases where col.string == None
                    elif col.string == None:
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
                    # Increment colno
                    colno+=1
            # Don't include the first, empty row. Add the rest into a dataframe.
            if tempRow !=[]:
                rugbyData = rugbyData.append(pd.DataFrame([tempRow], columns=colNames), ignore_index=True)
            rowCount+=1
    
    # Append a year column
    yearCol=[]
    [yearCol.append(year) for n in xrange(len(rugbyData))]
    rugbyData["Year"] = yearCol
    # Clean the TeamName column
    rugbyData = cleanNameData(rugbyData)  
    # Add to SQL database
    addToDatabase(rugbyData, connect)


# cleanNameData(datFrame)
def cleanNameData(datFrame):
    # Create a dictionary that includes all the possible names for different teams
    hig = "Otago Highlanders"
    cru = "Canterbury Crusaders"
    chi = "Waikato Chiefs"
    hur = "Wellington Hurricanes"
    blu = "Auckland Blues"   
    red = "Queensland Reds"
    bru = "ACT Brumbies"
    war = "NSW Waratahs"
    forc = "Western Force"
    sha = "Natal Sharks"
    bul = ["Bulls","'Bulls", "Northern Bulls", "Northern Transvaal"]
    lio = ["Lions", "Cats", "Gauteng Lions", "Transvaal"]
    sto = ["Western Province", "Stormers"]
    che = ["Cheetahs", "Free State"]
    ## Kings, Rebels, Jaguares and Sunwolves do not need to be cleaned
    
    nameDict = {"Highlanders": hig, "Crusaders": cru, "Chiefs": chi, "Hurricanes": hur, "Blues": blu, "Reds": red, "Brumbies": bru, "Waratahs": war, "Force": forc,
                "Sharks": sha, "Bulls": bul, "Lions": lio, "Stormers": sto, "Cheetahs": che}
    
    # For each TeamName in the input dataframe, loop over the dictionary items, if TeamName is in the dictionary, use the corresponding dictionary
    # key as the new TeamName. Else the TeamName is unchanged.
    for i in xrange(len(datFrame.TeamName)):
        for item in nameDict.items():
            if datFrame["TeamName"][i] in item[1]:
                datFrame["TeamName"][i] = item[0]
    # Return the input data frame, with TeamNames now cleaned.
    return datFrame
  

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
    years = range(1996,2017,1)
    for year in years:
        print year
        downloader(year, connect)

host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"
connect = [host, user, passwd, db]

#Set to True to remove the table
tf = 0
if tf:
    removeTable('seasonResults', connect)
#Set to True to create the table
if tf:
    createTable('seasonResults', connect)
#Set to true to download data                                                 
if tf:                                                                
    downloadAll(connect)




# Temporary code to read from database
conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
sqlQuery = '''SELECT * FROM seasonResults'''    #283 rows total
#sqlQuery = '''SELECT * FROM seasonResults WHERE Year = 1996''' 
#sqlQuery = '''SELECT * FROM seasonResults WHERE TeamName LIKE '%kin%' OR TeamName LIKE '%souk%' '''
#sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total                
dataFrame = pd.read_sql(sqlQuery, conn)
conn.close()
dataFrame 
