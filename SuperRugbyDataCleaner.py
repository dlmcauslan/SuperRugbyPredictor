'''                   SuperRugbyDataCleaner.py
Formats super rugby data into a format that can be used for making machine learning predictions
Created: 24/09/2016
    Loads data from the superRugbyPredictor MySQL database (locally hosted)
    
    Data to process, previous 3 years + current season, total won, total lost, points for, points against, table points for both teams
     Head to head win ratio from home teams perspective, Home teams total home record against all teams, Away teams total away record against all teams
    
     - solving for HomeTeamWinQ (does the home team win? t/f) or HomeTeamPtsDiff (What is home team score - away team score?)
     - HomeTeam, AwayTeam, Week, Year are not included in ML model
     
    rowList = 'HomeTeamWinQ INT, HomeTeamPtsDiff Float, HomeTeam TEXT, HomeScore INT, AwayScore INT, AwayTeam TEXT, Week INT, Year INT, \
                H2HWinRat_HmeTeam FLOAT, H_HomeRec FLOAT, A_AwayRec FLOAT, H_PlayedCurr INT, A_PlayedCurr INT, \
                H2HGamesPlayed INT, H_NumHomeGames INT, A_NumHomeGames INT, \
    H_LostCurr FLOAT, H_PointsCurr FLOAT, H_PtsACurr FLOAT, H_PtsFCurr FLOAT, H_WonCurr FLOAT, \
    A_LostCurr FLOAT, A_PointsCurr FLOAT, A_PtsACurr FLOAT, A_PtsFCurr FLOAT, A_WonCurr FLOAT \
    H_Lost FLOAT, H_Lost1 FLOAT, H_Lost2 FLOAT, \
    H_Points FLOAT, H_Points1 FLOAT, H_Points2 FLOAT, 
    H_PtsA FLOAT, H_PtsA1 FLOAT, H_PtsA2 FLOAT, \
    H_PtsF FLOAT, H_PtsF1 FLOAT, H_PtsF2 FLOAT \
    H_Won FLOAT, H_Won1 FLOAT, H_Won2 FLOAT, \    
    A_Lost FLOAT, A_Lost1 FLOAT, A_Lost2 FLOAT, \
    A_Points FLOAT, A_Points1 FLOAT, A_Points2 FLOAT, \
    A_PtsA FLOAT, A_PtsA1 FLOAT, A_PtsA2 FLOAT, \
    A_PtsF FLOAT, A_PtsF1 FLOAT, A_PtsF2 FLOAT \
    A_Won FLOAT, A_Won1 FLOAT, A_Won2 FLOAT'
    }
    
    
    For every game:
        HomeScore - home teams score
        AwayScore - away teams score
        H2HWinRat_HmeTeam - Head to head win ratio from home teams perspective
        H_HomeRec - Home teams total home record against all teams
        A_AwayRec - Away teams total away record against all teams
        H_(A_)PlayedCurr - Number of games home (away) team has played so far this season - note this is different than week because teams don't play every week.
        H2HGamesPlayed - Number of times the two teams have played.
        H_(A_)NumHome(Away)Games - Total number of home (away) games the home (away) team has played.
        H_ - Referring to the home team
        A_ - Referring to the away team
        Curr, ' ', 1, 2 - Refers to currents season, last season, 2 seasons ago, 3 seasons ago
        Lost - games lost
        Won - games won
        Points - championship points
        PtsF - points scored for
        PtsA - points scored against
    
    Lost, Won, Points, PtsF, PtsA are all normalised over the number of games played that season
    H2HWinRat_HmeTeam, H_HomeRec, A_AwayRec are all normalised to 1.
        
    
Modified 24/09/2016:
    * Moved data cleaning scripts from SuperRugbyDownloader to this new file.
    
Modified 25/09/2016:
    * Added previous season results for both teams for each match using two left 
    joins on matchResults table.
    * Added columns for which team wins, points differential, Current season wins,
    loses, points for and points against for both home and away teams.
'''

import pandas as pd
import numpy as np
import databaseFunctions as DB

# Makes dataframes display better on my PC.
pd.set_option('display.width', 190)
pd.options.display.max_columns = 100
pd.options.display.max_rows = 150
pd.set_option('precision', 2)

# SQL Connection
#host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"            # Surface
host = "localhost"; user = "root"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"               # Desktop
connect = [host, user, passwd, db]

# normalizeData()
def normalizeData(dataFrame):
    # normalizes the data in seasonResults to the number of
    # games played and returns the new dataframe.
    for colName in ["BP", "Draw", "Lost", "Points", "PtsA", "PtsD","PtsF","Won"]:
        dataFrame[colName] = dataFrame[colName]/dataFrame["Played"]
    return dataFrame


# reSort()
def reSort(dataFrame, yearVect):
    # Changes the value of the position column for later years where the conference system was in place 
    # so that the teams are ranked properly.
    # Loop over years
    for year in yearVect:
        # Create a temporary dataframe containing only that years data, sorted by Points, Games Won, Point Differential
        tmp = dataFrame.loc[dataFrame.loc[:,"Year"] == year,:].sort_values(by=['Points', 'Won', 'PtsD'], ascending = False)
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
    dataFrame = DB.readDatabase(connect, sqlQuery)
    # Clean the Position column.
    dataFrame = reSort(dataFrame, range(2012,2017))
    # Give data frame containing normalized data
    dataFrameNorm = normalizeData(dataFrame)
    # Set up a temporary dataframe
    dataFrameTmp = dataFrameNorm
    # Column names to iterate over
    colNames =["Won", "Draw", "Lost", "PtsF", "PtsA", "PtsD", "BP", "Points"]
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
            dataFrameTmp.loc[(dataFrameTmp["Year"]==year) & (dataFrameTmp["TeamName"]==team),colName+"1"] = tmpDat
            dataFrameTmp.loc[(dataFrameTmp["Year"]==year) & (dataFrameTmp["TeamName"]==team),colName+"2"] = tmpDat2
    # Add to database seasonResultsPastResults
    DB.addToDatabase(dataFrameTmp, 'seasonResultsPastResults', connect)



tf2 = 0
#Set to True to remove the table
if tf2:
    DB.removeTable('seasonResultsPastResults', connect)
#Set to True to create the table
if tf2:
    rowList = 'BP FLOAT, BP1 FLOAT, BP2 FLOAT, \
    Draw FLOAT, Draw1 FLOAT, Draw2 FLOAT, Played INT, Lost FLOAT, Lost1 FLOAT, Lost2 FLOAT, \
    Points FLOAT, Points1 FLOAT, Points2 FLOAT, PtsA FLOAT, PtsA1 FLOAT, PtsA2 FLOAT, \
    PtsD FLOAT, PtsD1 FLOAT, PtsD2 FLOAT, \
    PtsF FLOAT, PtsF1 FLOAT, PtsF2 FLOAT, Position INT, \
    TeamName TEXT, Won FLOAT, Won1 FLOAT, Won2 FLOAT, Year INT'
    DB.createTable('seasonResultsPastResults', rowList, connect)
if tf2:
    createPastData(connect)
    
 
sqlQuery = '''SELECT Lost, Lost1, Lost2, Points, Points1, Points2, PtsA, PtsA1, PtsA2, PtsF, PtsF1, PtsF2, TeamName, Won, Won1, Won2 
            FROM seasonResultsPastResults WHERE Year = 1996'''   
dFPast = DB.readDatabase(connect, sqlQuery)       
print dFPast
sqlQuery = '''SELECT * FROM matchResults ORDER BY Year, Week'''   
dFMatch = DB.readDatabase(connect, sqlQuery)
print dFMatch

#sqlQuery = '''
#SELECT mR.Year, mR.Week, mR.HomeTeam, mR.AwayTeam, mR.HomeScore, mR.AwayScore,
#        sR.Won AS H_Won, Sr.Lost AS H_Lost, sR.Points AS H_Points, sR.PtsF AS H_PtsF, sR.PtsA AS H_PtsA,
#        sRA.Won AS A_Won, SrA.Lost AS A_Lost, sRA.Points AS A_Points, sRA.PtsF AS A_PtsF, sRA.PtsA AS A_PtsA,
#        sR.Won1 AS H_Won1, Sr.Lost1 AS H_Lost1, sR.Points1 AS H_Points1, sR.PtsF1 AS H_PtsF1, sR.PtsA1 AS H_PtsA1,
#        sRA.Won1 AS A_Won1, SrA.Lost1 AS A_Lost1, sRA.Points1 AS A_Points1, sRA.PtsF1 AS A_PtsF1, sRA.PtsA1 AS A_PtsA1,
#        sR.Won2 AS H_Won2, Sr.Lost2 AS H_Lost2, sR.Points2 AS H_Points2, sR.PtsF2 AS H_PtsF2, sR.PtsA2 AS H_PtsA2,
#        sRA.Won2 AS A_Won2, SrA.Lost2 AS A_Lost2, sRA.Points2 AS A_Points2, sRA.PtsF2 AS A_PtsF2, sRA.PtsA2 AS A_PtsA2
#FROM matchResults AS mR
#LEFT JOIN seasonResultsPastResults AS sR
#ON mR.HomeTeam=sR.TeamName AND mR.Year = sR.Year
#LEFT JOIN seasonResultsPastResults AS sRA
#ON mR.AwayTeam=sRA.TeamName AND mR.Year = sRA.Year
#WHERE mR.Year = 1996
#ORDER BY Year, Week           
#'''
sqlQuery = '''
SELECT mR.Year, mR.Week, mR.HomeTeam, mR.AwayTeam, mR.HomeScore, mR.AwayScore,
        sR.Won AS H_Won
FROM matchResults AS mR
LEFT JOIN seasonResultsPastResults AS sR
ON mR.HomeTeam=sR.TeamName AND mR.Year = sR.Year
LEFT JOIN seasonResultsPastResults AS sRA
ON mR.AwayTeam=sRA.TeamName AND mR.Year = sRA.Year
WHERE mR.Year <= 1997
ORDER BY Year, Week           
'''
dFJoin = DB.readDatabase(connect, sqlQuery)
print dFJoin.head()
#print dFJoin.info()
# Set up a temporary dataframe
dFTmp = dFJoin
# New columns to add
"""H2HWinRat_HmeTeam - Head to head win ratio from home teams perspective
H_HomeRec - Home teams total home record against all teams
A_AwayRec - Away teams total away record against all teams
H_(A_)PlayedCurr - Number of games home (away) team has played so far this season - note this is different than week because teams don't play every week.
H2HGamesPlayed - Number of times the two teams have played.
H_(A_)NumHome(Away)Games - Total number of home (away) games the home (away) team has played."""
colNames = ['HomeTeamWinQ', 'HomeTeamPtsDiff',
            'H_PlayedCurr', 'A_PlayedCurr',
            'H_WonCurr', 'H_LostCurr', 'A_WonCurr', 'A_LostCurr',   
            'H_PtsFCurr', 'H_PtsACurr', 'A_PtsFCurr', 'A_PtsACurr']
            #'H2HGamesPlayed', 'H_NumHomeGames', 'A_NumAwayGames', 'H2HWinRat_HmeTeam', 'H_HomeRec', 'A_AwayRec', 
            #]

def currStats(dFTmp, n, team, col):
    y = dFTmp.loc[n,"Year"]
    sfx = col.split('_')[-1]
    # tmp is all of the games the team has had so far this year
    tmp = dFTmp.loc[(dFTmp["Year"] == y) & ((dFTmp["HomeTeam"] == team) | (dFTmp["AwayTeam"] == team)),:].loc[:n]
    # If its the teams first game of the year, set to zero
    if len(tmp) <= 1:
        dFTmp.loc[n,col] = 0    
    else:
        # tmpRow is what happened the last time the team played this year
        tmpRow = tmp.iloc[-2]
        # If team is home team in the last game
        if tmpRow["HomeTeam"] == team:
            if sfx == 'PlayedCurr':
                dFTmp.loc[n,col] = tmpRow["H_{}".format(sfx)]+1
            if sfx == 'LostCurr':
                if tmpRow["HomeTeamWinQ"] == -1:
                    dFTmp.loc[n,col] = tmpRow["H_{}".format(sfx)]+1
                else:
                    dFTmp.loc[n,col] = tmpRow["H_{}".format(sfx)]
            if sfx == 'WonCurr':
                if tmpRow["HomeTeamWinQ"] == 1:
                    dFTmp.loc[n,col] = tmpRow["H_{}".format(sfx)]+1
                else:
                    dFTmp.loc[n,col] = tmpRow["H_{}".format(sfx)]
            if sfx == 'PtsFCurr':
                dFTmp.loc[n,col] = tmpRow["H_{}".format(sfx)]+tmpRow["HomeScore"]
            if sfx == 'PtsACurr':
                dFTmp.loc[n,col] = tmpRow["H_{}".format(sfx)]+tmpRow["AwayScore"]

        # Otherwise team is away team in the last game
        else:
            if sfx == 'PlayedCurr':
                dFTmp.loc[n,col] = tmpRow["A_{}".format(sfx)]+1
            if sfx == 'LostCurr':
                if tmpRow["HomeTeamWinQ"] == 1:
                    dFTmp.loc[n,col] = tmpRow["A_{}".format(sfx)]+1
                else:
                    dFTmp.loc[n,col] = tmpRow["A_{}".format(sfx)]
            if sfx == 'WonCurr':
                if tmpRow["HomeTeamWinQ"] == -1:
                    dFTmp.loc[n,col] = tmpRow["A_{}".format(sfx)]+1
                else:
                    dFTmp.loc[n,col] = tmpRow["A_{}".format(sfx)]
            if sfx == 'PtsFCurr':
                dFTmp.loc[n,col] = tmpRow["A_{}".format(sfx)]+tmpRow["AwayScore"]
            if sfx == 'PtsACurr':
                dFTmp.loc[n,col] = tmpRow["A_{}".format(sfx)]+tmpRow["HomeScore"]
                
    return dFTmp

# Loop over the rows in the dataFrame adding the new columns
for n in xrange(len(dFTmp)):
#for n in xrange(5):
    # Initialize some values
    for c in colNames:
        dFTmp.loc[n,c] = 0
    dFTmp.loc[n,"HomeTeamPtsDiff"] = dFTmp.loc[n,"HomeScore"]-dFTmp.loc[n,"AwayScore"]

    # Who wins       
    if dFTmp.loc[n,"HomeScore"] > dFTmp.loc[n,"AwayScore"]:
        dFTmp.loc[n,"HomeTeamWinQ"] = 1
    elif dFTmp.loc[n,"HomeScore"] < dFTmp.loc[n,"AwayScore"]:
        dFTmp.loc[n,"HomeTeamWinQ"] = -1
        
    #w = dFTmp.loc[n,"Week"] 
    #y = dFTmp.loc[n,"Year"] 
    ht = dFTmp.loc[n,"HomeTeam"]
    at = dFTmp.loc[n,"AwayTeam"]
    
    # The team stats for the current year ------------THIS can probably be put in a loop 
    # For Home team
    dFTmp = currStats(dFTmp, n, ht, "H_PlayedCurr")
    dFTmp = currStats(dFTmp, n, ht, "H_LostCurr")
    dFTmp = currStats(dFTmp, n, ht, "H_WonCurr")
    dFTmp = currStats(dFTmp, n, ht, "H_PtsFCurr")
    dFTmp = currStats(dFTmp, n, ht, "H_PtsACurr")
    # For Away team
    dFTmp = currStats(dFTmp, n, at, "A_PlayedCurr")
    dFTmp = currStats(dFTmp, n, at, "A_LostCurr")
    dFTmp = currStats(dFTmp, n, at, "A_WonCurr")
    dFTmp = currStats(dFTmp, n, at, "A_PtsFCurr")
    dFTmp = currStats(dFTmp, n, at, "A_PtsACurr")

        


            
print dFTmp.tail()
                
## Temporary code for testing
#sqlQuery = '''SELECT * FROM seasonResultsPastResults'''    #283 rows total
##sqlQuery = '''SELECT TeamName, Position, Won, Points FROM seasonResultsPastResults WHERE Year >= 2015 ORDER BY Year, Position''' 
###sqlQuery = '''SELECT * FROM seasonResults WHERE TeamName LIKE '%kin%' OR TeamName LIKE '%souk%' '''
##sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total                
#dataFrame = DB.readDatabase(connect, sqlQuery)
#print dataFrame