'''                   SuperRugbyDownloader.py
Downloads historical super rugby results from wikipedia.

Created: 04/05/2016
    * Implemented downloading of 1996 data.
    * Saves data in MySQL database.

Modified: 09/05/2016
    * Downloads data from all years. Had to add many exception because different
    years have the data formatted slightly differently.

Modified: 10/05/2016
    * Implemented function to clean the TeamName data as different years data had
    slightly different team names. And several of the south african teams have
    changed their name over the years.

Modified: 17/05/2016
    * Implemented updateData() function, which enables the table data to be updated
    for a particular year without redownloading the entire database.
    * Implemented normalizeData() function, which normalizes the yearly data by the
    number of games played.
    * Implemented createPastData() function, which creates columns in the data set
    for the previous two years data. This will be used to see whether the results
    of a team in the past 2 years has any bearing on their performance in the current
    year.
    
Modified: 18/05/2016
    * Implemented reSort() to fix the team rankings for years where the conference system
    has been in place.
    * Implemented downloader2() for downloading individual game results for 1996 - so far.
    
Modified: 19/05/2016
    * Now downloads number of try penalty and conversion match data, which is then cleaned
    using tryPenConData(). - works for 1996 so far.
    * Downloading match data works up to and including 2011.
    
Modified: 24/09/2016
    * Created simpleDownloader2 function which downloads individual match results, but only 
    final scores, not tries etc.
    * This simple downloader function works up to and including 2016.
    
    
TO DO: Download individual game results, such as tries scored, penalties, conversions etc.
       Make SQL table of individual game results
        
'''

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import urllib2
import re
import pymysql.cursors
import pymysql

# Makes dataframes display better on my surface.
pd.set_option('display.width', 190)
pd.options.display.max_columns = 50

# SQL Connection
host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"
host = "localhost"; user = "root"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"
connect = [host, user, passwd, db]

def downloader(year, connect):
    #Creates dataframe
    colNames =["Position", "TeamName", "Played", "Won", "Draw", "Lost", 
            "PFor", "PAgainst", "PDiff", "BonusP", "Points"]
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
    addToDatabase(rugbyData, 'seasonResults', connect)

# Downloads individual match data
def downloader2(year, connect):
    #Creates dataframe
    colNames =["HomeTeam", "HomeScore", "AwayScore", "AwayTeam", "HTries", "HCons", "HPens", "HoCheck", "ATries", "ACons", "APens", "AwCheck", "Week", "Year"]
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
    if year<=2016:
        alldat = soup.find(id = "mw-content-text")
    
    
    # Find the row for week 1
    row = alldat.find("h3")
    # For 2006 there are other headings above Round 1 so need to skip over them
    if year==2006:  
        for i in xrange(5):    
            row = row.find_next_sibling()
    if year == 2012:
        row = alldat.find(id = "Round_1").find_parent("h3")
        #print row
        #row = row.find_parent("h3")
        #print row               
    week = 1

    # Iterate over the soup and if it is a week label, get the week number
    # otherwise loop over the row adding data to the data frame. Exit out of the
    # loop when it reaches the row labelled Finals[edit]
    while row.find_next_sibling().get_text() not in ["Finals[edit]", "Playoffs[edit]"]:
    #while row.find_next_sibling().get_text() != "Week 10[edit]":
        # Skip to next row of the soup
        row = row.find_next_sibling()       
        # If text or data we're not interested in, skip over it.
        if re.match('<p|<ul',str(row)) or re.findall(u'Bye',row.get_text()):
            #row = row.find_next_sibling()
            pass
        # If we've reached a week label, get the week number
        elif re.match('Week|Round',row.get_text()):
            week = int(re.findall(u'\d{1,3}',row.get_text())[0])
        # Otherwise loop over the row, putting the data into the data frame.
        else:
            n=0
            tempRow=[]
            # Loop over each column in the row.
            for col in row.findAll("td"):                
                txt = col.get_text()
                # Columns 2 and 4 are the home and away team names
                if n in [2,4]:
                    # Needed to add this if statement because there is 2 transvaal teams in 1996
                    if str.split(str(txt))[0] == 'Northern':
                        tempRow.append('Bulls')
                    else:
                        tempRow.append(str.split(str(txt))[-1])
                # Column 3 is the score. Split this up into the home and away scores and add as
                #separate columns to the data frame.
                if n == 3:
                    #print re.findall(u'\d{1,3}',txt)
                    if re.findall(u'\d{1,3}',txt) !=[]:        # Because a game in 2011 was cancelled due to ChCh earthquake
                        [hScore, aScore] = re.findall(u'\d{1,3}',txt)
                        tempRow.append(int(hScore))
                        tempRow.append(int(aScore))
                    else:
                        tempRow.append(np.nan)
                        tempRow.append(np.nan)
                # Column 8 is the home team tries, penalty, conversion data. Column 10 is the away team data.
                if n in [8,10]:
                    if txt:     # Make sure the data exists
                        nTry, nCon, nPen = tryPenConData(txt, year)
                        print txt
                        print '{} Cons, {} Pens, {} Tries'.format(nCon, nPen, nTry)
                        print ' '
                    elif (tempRow[1]==0) | (tempRow[2]==0):      # The case when a team scores 0.
                        nTry, nCon, nPen = 0, 0, 0
                    else:
                        nTry, nCon, nPen = np.nan, np.nan, np.nan                   
                    tempRow.append(int(nTry))
                    tempRow.append(int(nCon))
                    tempRow.append(int(nPen))
                    tempRow.append(int(5*nTry+2*nCon+3*nPen))
                     
                # Increment the column number by 1.
                n+=1
            # Add on the week
            tempRow.append(int(week))
            # Add on the year
            tempRow.append(int(year))
            # Add row to data frame (not including playoffs)
            if tempRow != []:
                rugbyData = rugbyData.append(pd.DataFrame([tempRow], columns=colNames), ignore_index=True)
       
    return rugbyData 
    
# Downloads individual match data (teams and score
def simpleDownloader2(year, connect):
    #Creates dataframe
    #colNames =["HomeTeam", "HomeScore", "AwayScore", "AwayTeam", "HTries", "HCons", "HPens", "HoCheck", "ATries", "ACons", "APens", "AwCheck", "Week", "Year"]
    colNames =["HomeTeam", "HomeScore", "AwayScore", "AwayTeam", "Week", "Year"]
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
    if year<=2016:
        alldat = soup.find(id = "mw-content-text")
    
    
    # Find the row for week 1
    row = alldat.find("h3")
    # For 2006 there are other headings above Round 1 so need to skip over them
    if year==2006:  
        for i in xrange(5):    
            row = row.find_next_sibling()
    if year >= 2012:
        row = alldat.find(id = "Round_1").find_parent("h3")
        #print row
        #row = row.find_parent("h3")
        #print row               
    week = 1

    # Iterate over the soup and if it is a week label, get the week number
    # otherwise loop over the row adding data to the data frame. Exit out of the
    # loop when it reaches the row labelled Finals[edit]
    while row.find_next_sibling().get_text() not in ["Finals[edit]", "Playoffs[edit]"]:
    #while row.find_next_sibling().get_text() != "Week 10[edit]":
        # Skip to next row of the soup
        row = row.find_next_sibling()       
        # If text or data we're not interested in, skip over it.
        if re.match('<p|<ul',str(row)) or re.findall(u'Bye',row.get_text()):
            #row = row.find_next_sibling()
            pass
        # If we've reached a week label, get the week number
        elif re.match('Week|Round',row.get_text()):
            week = int(re.findall(u'\d{1,3}',row.get_text())[0])
        # Otherwise loop over the row, putting the data into the data frame.
        else:
            n=0
            tempRow=[]
            # Loop over each column in the row.
            for col in row.findAll("td"):                
                txt = col.get_text()
                # Columns 2 and 4 are the home and away team names
                if n in [2,4]:
                    # Needed to add this if statement because there is 2 transvaal teams in 1996
                    if str.split(str(txt))[0] == 'Northern':
                        tempRow.append('Bulls')
                    else:
                        tempRow.append(str.split(str(txt))[-1])
                # Column 3 is the score. Split this up into the home and away scores and add as
                #separate columns to the data frame.
                if n == 3:
                    #print re.findall(u'\d{1,3}',txt)
                    if re.findall(u'\d{1,3}',txt) !=[]:        # Because a game in 2011 was cancelled due to ChCh earthquake
                        [hScore, aScore] = re.findall(u'\d{1,3}',txt)
                        tempRow.append(int(hScore))
                        tempRow.append(int(aScore))
                    else:
                        tempRow.append(np.nan)
                        tempRow.append(np.nan)
                     
                # Increment the column number by 1.
                n+=1
            # Add on the week
            tempRow.append(int(week))
            # Add on the year
            tempRow.append(int(year))
            # Add row to data frame (not including playoffs)
            if tempRow != []:
                rugbyData = rugbyData.append(pd.DataFrame([tempRow], columns=colNames), ignore_index=True)
       
    return rugbyData  
   
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

# tryPenConData()    
def tryPenConData(datString, year):
    # Takes in a datString which contains, penalty, try and conversion data for individual matches,
    # splits the string up and calculates the number of T, P and C. Returns nTries, nCons, nPens.
    strVect = re.split("[):,\n]",datString)
    nTries = 0
    nCons = 0
    nPens = 0
    flag = 'N'
    # Iterates over the split string in strVect summing up the number of tries, penalties and conversions
    # scored
    for n in strVect:
        if n in ["Try", "Tries"]:
            flag = 'T'
        elif n in ["Con", "Cons", "Cons."]:
            flag = 'C'
        elif n in ["Pen", "Pens", "Pens."]:
            flag = 'P'
        elif n in ["Drop", "Cards"]:
            flag = 'N'
        elif n =='':
            pass
        elif re.match('\([0-9]{1,2}',re.split(' ',n)[-1]):    # Adds on number of tries etc
            num = int(re.findall('[0-9]{1,2}',re.split(' ',n)[-1])[0])
            if (year >= 2012) & (num!=0):
                num-=1     
            if flag == 'T':
                nTries += num 
            elif flag == 'P':
                nPens += num
            elif flag == 'C':
                nCons += num 
        ######## THIS DOESNT WORK YET ####################
        elif not re.match(" [1-9]{1,2}'",n):   # From 2012 onwards the time the try was scored is added to the data, this stops it being counted:
            if flag == 'T':
                nTries += 1
            elif flag == 'P':
                nPens += 1
            elif flag == 'C':
                nCons += 1    
    return nTries, nCons, nPens
  

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

# Downloads all the data
def downloadAll(connect):
    years = range(1996,2017,1)
    for year in years:
        print year
        downloader(year, connect)
        
# updateData(year)
def updateData(year, connect):
    # removes data for year, and redownloads it. Useful for updating DB after a game
    # without having to redownload entire database.
    # Remove all current data for that year
    conn = pymysql.connect(connect[0], connect[1], connect[2], connect[3])
    cursor = conn.cursor()        
    #Remove database
    sql_command = """ DELETE FROM seasonResults WHERE Year = {} """.format(year)
    cursor.execute(sql_command)
    conn.commit()
    conn.close()
    # Redownload data for that year
    downloader(year, connect)
    print "{} data updated".format(year)
    
# normalizeData()
def normalizeData(dataFrame):
    # normalizes the data in seasonResults to the number of
    # games played and returns the new dataframe.
    for colName in ["BonusP", "Draw", "Lost", "Points", "PAgainst", "PDiff","PFor","Won"]:
        dataFrame[colName] = dataFrame[colName]/dataFrame["Played"]
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
    



#Set to True to remove the table
tf = 0
if tf:
    removeTable('seasonResults', connect)
#Set to True to create the table
if tf:
    rowList = 'BonusP INT, Draw INT, Played INT, Lost INT, Points INT, \
    PAgainst INT, PDiff INT, PFor INT, Position INT, \
    TeamName TEXT, Won INT, Year INT'
    createTable('seasonResults', rowList, connect)
#Set to true to download data                                                 
if tf:                                                                
    downloadAll(connect)
# Set to True to update a particular years data
if 0:
    updateData(2016, connect)

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

        
#dF = downloader2(2013,connect)
dF = simpleDownloader2(2016,connect)
print dF


## Temporary code for testing
#sqlQuery = '''SELECT * FROM seasonResultsPastResults'''    #283 rows total
sqlQuery = '''SELECT TeamName, Position, Won, Points FROM seasonResultsPastResults WHERE Year >= 2015 ORDER BY Year, Position''' 
##sqlQuery = '''SELECT * FROM seasonResults WHERE TeamName LIKE '%kin%' OR TeamName LIKE '%souk%' '''
#sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total                
dataFrame = readDatabase(connect, sqlQuery)
#print dataFrame
#dF = reSort(dataFrame, range(2012,2017))
#print dF.loc[dF["Year"]==2012,["TeamName", "Position","Points","Won", "PDiff"]]