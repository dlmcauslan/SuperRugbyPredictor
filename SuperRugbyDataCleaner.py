'''                   SuperRugbyDataCleaner.py
Formats super rugby data into a format that can be used for making machine learning predictions
Created: 24/09/2016
    Loads data from the superRugbyPredictor MySQL database (locally hosted)
    
    Data to process, previous 3 years + current season, total won, total lost, points for, points against, table points for both teams
     Head to head win ratio from home teams perspective, Home teams total home record against all teams, Away teams total away record against all teams
    
     - solving for HomeTeamWinQ (does the home team win? t/f) 1 for a win, -1 for a loss, 0 for a draw or HomeTeamPtsDiff (What is home team score - away team score?)
     - Week, Year are not included in ML model
     
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
    H_HomeRec, A_AwayRec are all normalised to 1.
    H2HWinRat_HmeTeam is normalised to +-1 (negative if the away team has the better H2H record).
        
    
Modified 24/09/2016:
    * Moved data cleaning scripts from SuperRugbyDownloader to this new file.
    
Modified 25/09/2016:
    * Added previous season results for both teams for each match using two left 
    joins on matchResults table.
    * Added columns for which team wins, points differential, Current season wins,
    loses, points for and points against for both home and away teams.
    
Modified 01/10/2016:
    * Added Head to Head number of games played and home team Head to Head ratio.
    * Added home and away records for all teams.
    * Normalized current year results.
    * All new data columns have now been created.
    * Created database 'totalMatchDataCombined' which holds all of the final cleaned data.
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


def normalizeData(dataFrame):
    ''' normalizes the data in seasonResults to the number of games played and returns the new dataframe. '''
    for colName in ["BP", "Draw", "Lost", "Points", "PtsA", "PtsD","PtsF","Won"]:
        dataFrame[colName] = dataFrame[colName]/dataFrame["Played"]
    return dataFrame


def reSort(dataFrame, yearVect):
    ''' Changes the value of the position column for later years where the conference system was in place 
    so that the teams are ranked properly. '''
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


def createPastData(connect):
    ''' Reads the data from the seasonResults table, normalizes it, then adds columns
    that give the data for the teams results the previous 2 years, e.g. Win_1, Win_2.
    This data is then added to the seasonResultsPastResults table. '''
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


def currStats(dFTmp, row, team, col):
    ''' Calculates the current season statistics for each team on  a round by round basis '''
    y = dFTmp.loc[row,"Year"]
    sfx = col.split('_')[-1]
    # tmp is all of the games the team has had so far this year
    tmp = dFTmp.loc[(dFTmp["Year"] == y) & ((dFTmp["HomeTeam"] == team) | (dFTmp["AwayTeam"] == team)),:].loc[:row]
    # If its the teams first game of the year, set to zero
    if len(tmp) <= 1:
        dFTmp.loc[row,col] = 0    
    else:
        # tmpRow is what happened the last time the team played this year
        tmpRow = tmp.iloc[-2]
        # If team is home team in the last game
        if tmpRow["HomeTeam"] == team:
            if sfx == 'PlayedCurr':
                dFTmp.loc[row,col] = tmpRow["H_{}".format(sfx)]+1
            if sfx == 'LostCurr':
                if tmpRow["HomeTeamWinQ"] == -1:
                    dFTmp.loc[row,col] = tmpRow["H_{}".format(sfx)]+1
                else:
                    dFTmp.loc[row,col] = tmpRow["H_{}".format(sfx)]
            if sfx == 'WonCurr':
                if tmpRow["HomeTeamWinQ"] == 1:
                    dFTmp.loc[row,col] = tmpRow["H_{}".format(sfx)]+1
                else:
                    dFTmp.loc[row,col] = tmpRow["H_{}".format(sfx)]
            if sfx == 'PtsFCurr':
                dFTmp.loc[row,col] = tmpRow["H_{}".format(sfx)]+tmpRow["HomeScore"]
            if sfx == 'PtsACurr':
                dFTmp.loc[row,col] = tmpRow["H_{}".format(sfx)]+tmpRow["AwayScore"]

        # Otherwise team is away team in the last game
        else:
            if sfx == 'PlayedCurr':
                dFTmp.loc[row,col] = tmpRow["A_{}".format(sfx)]+1
            if sfx == 'LostCurr':
                if tmpRow["HomeTeamWinQ"] == 1:
                    dFTmp.loc[row,col] = tmpRow["A_{}".format(sfx)]+1
                else:
                    dFTmp.loc[row,col] = tmpRow["A_{}".format(sfx)]
            if sfx == 'WonCurr':
                if tmpRow["HomeTeamWinQ"] == -1:
                    dFTmp.loc[row,col] = tmpRow["A_{}".format(sfx)]+1
                else:
                    dFTmp.loc[row,col] = tmpRow["A_{}".format(sfx)]
            if sfx == 'PtsFCurr':
                dFTmp.loc[row,col] = tmpRow["A_{}".format(sfx)]+tmpRow["AwayScore"]
            if sfx == 'PtsACurr':
                dFTmp.loc[row,col] = tmpRow["A_{}".format(sfx)]+tmpRow["HomeScore"]
                
    return dFTmp


def headToHead(dFTmp, row, homeTeam, awayTeam):
    ''' Calculates the number of times the two teams have played and their head to head ratio win ratio'''
    # tmp is all of the games the two teams have played in the past
    tmp = dFTmp.loc[((dFTmp["HomeTeam"] == homeTeam) & (dFTmp["AwayTeam"] == awayTeam)) | ((dFTmp["HomeTeam"] == awayTeam) & (dFTmp["AwayTeam"] == homeTeam)),:].loc[:row]
    # If its the first time the two teams have played, set to zero
    if len(tmp) <= 1:
        dFTmp.loc[row,'H2HGamesPlayed'] = 0
        dFTmp.loc[row,'H2HWinRat_HmeTeam'] = 0
    else:
        # tmpRow is what happened the last time the two teams played
        tmpRow = tmp.iloc[-2]
        # Add on one to the number of games played
        dFTmp.loc[row,'H2HGamesPlayed']=tmpRow.H2HGamesPlayed+1
        # Update the head to head ratio - (oldH2Hratio * oldH2Hplayed + oldHomeTeamWinQ)/newH2Hplayed
        newHeadToHead = (tmpRow.H2HWinRat_HmeTeam * tmpRow.H2HGamesPlayed + tmpRow.HomeTeamWinQ)/dFTmp.loc[row,'H2HGamesPlayed']
        # If the current home team was the home team last time
        if homeTeam == tmpRow["HomeTeam"]:
            dFTmp.loc[row,'H2HWinRat_HmeTeam'] = newHeadToHead
        else:
            dFTmp.loc[row,'H2HWinRat_HmeTeam'] = -newHeadToHead    
    return dFTmp

        
def homeAwayRecord(dFTmp, row, team, prefix):
    ''' Calculates the teams home and away record '''
    homeOrAwayColumns = {'H': ('HomeTeam', 'H_NumHomeGames', 'H_HomeRec'), 'A': ('AwayTeam', 'A_NumAwayGames', 'A_AwayRec')}
    # tmp is all of the games the home(away) team has played home(away) in the past
    tmp = dFTmp.loc[dFTmp[homeOrAwayColumns[prefix][0]] == team ,:].loc[:row]
    # If its the first time the team has played at home or away, set to zero
    if len(tmp) <= 1:
        dFTmp.loc[row, homeOrAwayColumns[prefix][1]] = 0
        dFTmp.loc[row, homeOrAwayColumns[prefix][2]] = 0
    else:
        # tmpRow is what happened the last time the two teams played
        tmpRow = tmp.iloc[-2]
        # Increase the number of home (away) games by 1
        dFTmp.loc[row, homeOrAwayColumns[prefix][1]] = tmpRow[homeOrAwayColumns[prefix][1]] + 1
        # New Home(away) record = (oldHomeRecord * oldNumHomeGames +- oldHomeTeamWinQ)/newNumHomeGames
        if prefix == 'H':
            dFTmp.loc[row, homeOrAwayColumns[prefix][2]] = (tmpRow[homeOrAwayColumns[prefix][1]] * tmpRow[homeOrAwayColumns[prefix][2]] + .5*(tmpRow["HomeTeamWinQ"]+1))/dFTmp.loc[row, homeOrAwayColumns[prefix][1]]
        else:
            dFTmp.loc[row, homeOrAwayColumns[prefix][2]] = (tmpRow[homeOrAwayColumns[prefix][1]] * tmpRow[homeOrAwayColumns[prefix][2]] + .5*(1-tmpRow["HomeTeamWinQ"]))/dFTmp.loc[row, homeOrAwayColumns[prefix][1]]
    return dFTmp


def currNormalize(dFTmp, prefix, suffix):
    ''' Normalizes current year data to the number of games played '''
    # We only want to adjust rows where the current number of games played is > 0 to avoid divide by 0 errors
    rows = dFTmp[prefix + "PlayedCurr"] > 0
    # Normalize the column
    dFTmp.loc[rows, prefix + suffix] = dFTmp.loc[rows, prefix + suffix]/dFTmp.loc[rows, prefix + "PlayedCurr"]
    ## Set Columns where games played = 0 to NaN
    #dFtmp.loc[dFTmp[prefix + "PlayedCurr"]==0, prefix + suffix] = np.nan
    return dFTmp


def createNewColumns():
    ''' Loop over the rows in the dataFrame adding the new columns '''
    sqlQuery = '''
    SELECT mR.Year, mR.Week, mR.HomeTeam, mR.AwayTeam, mR.HomeScore, mR.AwayScore,
            sR.Won AS H_Won1, Sr.Lost AS H_Lost1, sR.Points AS H_Points1, sR.PtsF AS H_PtsF1, sR.PtsA AS H_PtsA1,
            sRA.Won AS A_Won1, SrA.Lost AS A_Lost1, sRA.Points AS A_Points1, sRA.PtsF AS A_PtsF1, sRA.PtsA AS A_PtsA1,
            sR.Won1 AS H_Won2, Sr.Lost1 AS H_Lost2, sR.Points1 AS H_Points2, sR.PtsF1 AS H_PtsF2, sR.PtsA1 AS H_PtsA2,
            sRA.Won1 AS A_Won2, SrA.Lost1 AS A_Lost2, sRA.Points1 AS A_Points2, sRA.PtsF1 AS A_PtsF2, sRA.PtsA1 AS A_PtsA2,
            sR.Won2 AS H_Won3, Sr.Lost2 AS H_Lost3, sR.Points2 AS H_Points3, sR.PtsF2 AS H_PtsF3, sR.PtsA2 AS H_PtsA3,
            sRA.Won2 AS A_Won3, SrA.Lost2 AS A_Lost3, sRA.Points2 AS A_Points3, sRA.PtsF2 AS A_PtsF3, sRA.PtsA2 AS A_PtsA3
    FROM matchResults AS mR
    LEFT JOIN seasonResultsPastResults AS sR
    ON mR.HomeTeam=sR.TeamName AND mR.Year = sR.Year+1
    LEFT JOIN seasonResultsPastResults AS sRA
    ON mR.AwayTeam=sRA.TeamName AND mR.Year = sRA.Year+1
    WHERE mR.Year <= 2016
    ORDER BY Year, Week           
    '''
    #sqlQuery = '''
    #SELECT mR.Year, mR.Week, mR.HomeTeam, mR.AwayTeam, mR.HomeScore, mR.AwayScore
    #FROM matchResults AS mR
    #WHERE mR.Year <= 2016
    #ORDER BY Year, Week           
    #'''
    dFTmp = DB.readDatabase(connect, sqlQuery)
    print dFTmp.head()

    # In 2011 the hurricanes crusaders game got cancelled because of the earthquake. So lets just set
    # that game as a 0-0 draw.
    dFTmp["HomeScore"] = dFTmp["HomeScore"].fillna(0)
    dFTmp["AwayScore"] = dFTmp["AwayScore"].fillna(0)
    for row in xrange(len(dFTmp)):             
        # Calculate the game points difference
        dFTmp.loc[row,"HomeTeamPtsDiff"] = dFTmp.loc[row,"HomeScore"]-dFTmp.loc[row,"AwayScore"]
    
        # Which team wins       
        if dFTmp.loc[row,"HomeScore"] > dFTmp.loc[row,"AwayScore"]:
            dFTmp.loc[row,"HomeTeamWinQ"] = 1
        elif dFTmp.loc[row,"HomeScore"] < dFTmp.loc[row,"AwayScore"]:
            dFTmp.loc[row,"HomeTeamWinQ"] = -1
        else:
            dFTmp.loc[row,"HomeTeamWinQ"] = 0
            
        homeTeam = dFTmp.loc[row,"HomeTeam"]
        awayTeam = dFTmp.loc[row,"AwayTeam"]
        
        # The team stats for the current year
        suffixes = ['PlayedCurr', 'WonCurr', 'LostCurr', 'PtsFCurr', 'PtsACurr']
        for suffix in suffixes:
            # For the home team
            dFTmp = currStats(dFTmp, row, homeTeam, "H_"+suffix)
            # For the away team
            dFTmp = currStats(dFTmp, row, awayTeam, "A_"+suffix)
            
        # Calculating head to head number of times played and win ratio
        dFTmp = headToHead(dFTmp, row, homeTeam, awayTeam)
        
        # Calculating the teams home and away record
        dFTmp = homeAwayRecord(dFTmp, row, homeTeam, 'H')
        dFTmp = homeAwayRecord(dFTmp, row, awayTeam, 'A')
        
    # Normalize current year stats to the number of games played
    suffixes = ['WonCurr', 'LostCurr', 'PtsFCurr', 'PtsACurr']
    for suffix in suffixes:
        # For the home team
        dFTmp = currNormalize(dFTmp, "H_", suffix)
        # For the away team
        dFTmp = currNormalize(dFTmp, "A_", suffix)
        
    return dFTmp
    
    
''' Creates a table of the past 3 seasons results  '''  
tf = 0
#Set to True to remove the table
if tf:
    DB.removeTable('seasonResultsPastResults', connect)
#Set to True to create the table
if tf:
    rowList = 'BP FLOAT, BP1 FLOAT, BP2 FLOAT, \
    Draw FLOAT, Draw1 FLOAT, Draw2 FLOAT, Played INT, Lost FLOAT, Lost1 FLOAT, Lost2 FLOAT, \
    Points FLOAT, Points1 FLOAT, Points2 FLOAT, PtsA FLOAT, PtsA1 FLOAT, PtsA2 FLOAT, \
    PtsD FLOAT, PtsD1 FLOAT, PtsD2 FLOAT, \
    PtsF FLOAT, PtsF1 FLOAT, PtsF2 FLOAT, Position INT, \
    TeamName TEXT, Won FLOAT, Won1 FLOAT, Won2 FLOAT, Year INT'
    DB.createTable('seasonResultsPastResults', rowList, connect)
if tf:
    createPastData(connect)
    
 
''' Creates a table of the past 3 seasons results  '''  
tf2 = 0
#Set to True to remove the table
if tf2:
    DB.removeTable('totalMatchDataCombined', connect)
#Set to True to create the table
if tf2:
    rowList = ' Year INT, Week INT, HomeTeam TEXT, AwayTeam TEXT, HomeScore INT, AwayScore INT,\
                H_Won1 FLOAT, H_Lost1 FLOAT, H_Points1 FLOAT, H_PtsF1 FLOAT, H_PtsA1 FLOAT, \
                A_Won1 FLOAT, A_Lost1 FLOAT, A_Points1 FLOAT, A_PtsF1 FLOAT, A_PtsA1 FLOAT, \
                H_Won2 FLOAT, H_Lost2 FLOAT, H_Points2 FLOAT, H_PtsF2 FLOAT, H_PtsA2 FLOAT, \
                A_Won2 FLOAT, A_Lost2 FLOAT, A_Points2 FLOAT, A_PtsF2 FLOAT, A_PtsA2 FLOAT, \
                H_Won3 FLOAT, H_Lost3 FLOAT, H_Points3 FLOAT, H_PtsF3 FLOAT, H_PtsA3 FLOAT, \
                A_Won3 FLOAT, A_Lost3 FLOAT, A_Points3 FLOAT, A_PtsF3 FLOAT, A_PtsA3 FLOAT, \
                HomeTeamPtsDiff INT, HomeTeamWinQ INT, \
                H_PlayedCurr INT, A_PlayedCurr INT, H_WonCurr FLOAT, A_WonCurr FLOAT, \
                H_LostCurr FLOAT, A_LostCurr FLOAT, H_PtsFCurr FLOAT, A_PtsFCurr FLOAT, \
                H_PtsACurr FLOAT,  A_PtsACurr FLOAT, H2HGamesPlayed INT, H2HWinRat_HmeTeam FLOAT,\
                H_NumHomeGames INT, H_HomeRec FLOAT, A_NumAwayGames INT, A_AwayRec FLOAT'
    DB.createTable('totalMatchDataCombined', rowList, connect)
# Set to True to create the total data table
if tf2:
    dFTmp = createNewColumns()
    # Add to database
    DB.addToDatabase(dFTmp, 'totalMatchDataCombined', connect) 

              
## Temporary code for testing
team1 = 'Highlanders'
team2 = 'Crusaders' 
sqlQuery = '''SELECT * FROM totalMatchDataCombined WHERE (HomeTeam = '{}' AND AwayTeam = '{}') OR (HomeTeam = '{}' AND AwayTeam = '{}')'''.format(team1, team2, team2, team1)
#sqlQuery = '''SELECT * FROM seasonResultsPastResults'''    #283 rows total
##sqlQuery = '''SELECT TeamName, Position, Won, Points FROM seasonResultsPastResults WHERE Year >= 2015 ORDER BY Year, Position''' 
###sqlQuery = '''SELECT * FROM seasonResults WHERE TeamName LIKE '%kin%' OR TeamName LIKE '%souk%' '''
##sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total                
dataFrame = DB.readDatabase(connect, sqlQuery)
print dataFrame
#dataFrame.info()