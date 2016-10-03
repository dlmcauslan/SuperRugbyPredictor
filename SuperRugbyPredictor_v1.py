'''                   SuperRugbyPredictor_v1.py
Creates machine learning models to predict the results of the Super Rugby Competition.
Created: 10/05/2016
    Loads data from the totalMatchDataCombined MySQL database (locally hosted)
    
    Trying to figure out whether home team wins
    
Modified: 02/10/2016
    * Had a first pass at scaling the data using sklearn module. Need to think about it though
    to make sure I do the most sensible scaling for each data type.

'''
# Import packages
import numpy as np
import pandas as pd
import sklearn as sk
from sklearn import preprocessing
import databaseFunctions as DB



# SQL Connection
#host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"            # Surface
host = "localhost"; user = "root"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"               # Desktop
connect = [host, user, passwd, db]


## Test code
#sqlQuery = '''SELECT * FROM totalMatchDataCombined'''    #1850 rows total
##sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total
#rugbyDat = DB.readDatabase(connect,sqlQuery)
#rugbyDat.info() 
# For the first test lets only include the current years data and last years data
sqlQuery = '''SELECT HomeTeamWinQ, H_Won1, H_Lost1, H_Points1, H_PtsF1, H_PtsA1, \
                A_Won1, A_Lost1, A_Points1, A_PtsF1, A_PtsA1, H_WonCurr, A_WonCurr, H_LostCurr, A_LostCurr, \
                H_PtsFCurr, A_PtsFCurr, H_PtsACurr, A_PtsACurr, H2HWinRat_HmeTeam, H_HomeRec, A_AwayRec FROM totalMatchDataCombined'''    #1850 rows total
rugbyDat = DB.readDatabase(connect,sqlQuery)
rugbyDat.info() 
rugbyDat.describe()

predictors = ['H_Won1', 'H_Lost1', 'H_Points1', 'H_PtsF1', 'H_PtsA1', 'A_Won1', 'A_Lost1', 'A_Points1', \
            'A_PtsF1', 'A_PtsA1', 'H_WonCurr', 'A_WonCurr', 'H_LostCurr', 'A_LostCurr', 'H_PtsFCurr', \
            'A_PtsFCurr', 'H_PtsACurr', 'A_PtsACurr', 'H2HWinRat_HmeTeam', 'H_HomeRec', 'A_AwayRec']
#rugbyDataScaled = pd.DataFrame(dict.fromkeys(predictors,[]))
#rugbyData = rugbyData.append(pd.DataFrame([tempRow], columns=colNames), ignore_index=True)
rugbyDatScaled = pd.DataFrame(preprocessing.scale(rugbyDat[predictors]), columns = predictors)

'''------------------- Need to think about this, instead of mean = 0, std = 1 in some cases its better to scale in range of 0 - -
as this preserves 0s -------------------'''