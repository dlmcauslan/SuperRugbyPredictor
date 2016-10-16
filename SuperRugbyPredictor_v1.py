'''                   SuperRugbyPredictor_v1.py
Creates machine learning models to predict the results of the Super Rugby Competition.
Created: 10/05/2016
    Loads data from the totalMatchDataCombined MySQL database (locally hosted)
    
    Trying to figure out whether home team wins
    
    Normalize Points, Won, Lost, Home(Away)Rec data to the range 0 - 1.
    ###Normalize PtsF, PtsA to mean = 0, std = 1.
    Normalize PtsF, PtsA data to the range 0 - 1.
    Normalize H2HWinRat_HmeTeam to the range -1 - +1.
    
Modified: 02/10/2016
    * Had a first pass at scaling the data using sklearn module. Need to think about it though
    to make sure I do the most sensible scaling for each data type.
    
Modified: 15/10/2016
    * Normalized all data colums to the range 0 - 1 to help the machine learning models.
    * Did a first model using logistic regression. Using the data from 1996-2015 to train
    the data I get 69.6% prediction accuracy for 2016.

'''
# Import packages
import numpy as np
import pandas as pd
import sklearn as sk
from sklearn import preprocessing
import sklearn.linear_model as sklm
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


def loadDataAndNormalize(connect):
    # For the first test lets only include the current years data and last years data
    sqlQuery = '''SELECT Year, HomeTeamWinQ, H_Won1, H_Lost1, H_Points1, H_PtsF1, H_PtsA1, \
                    A_Won1, A_Lost1, A_Points1, A_PtsF1, A_PtsA1, H_WonCurr, A_WonCurr, H_LostCurr, A_LostCurr, \
                    H_PtsFCurr, A_PtsFCurr, H_PtsACurr, A_PtsACurr, H2HWinRat_HmeTeam, H_HomeRec, A_AwayRec FROM totalMatchDataCombined'''    #1850 rows total
    rugbyData = DB.readDatabase(connect,sqlQuery)
    #rugbyData.info() 
    #rugbyData.describe()
    
    frameColumns = ['Year','HomeTeamWinQ','H_Won1', 'H_Lost1', 'H_Points1', 'H_PtsF1', 'H_PtsA1', 'A_Won1', 'A_Lost1', 'A_Points1', \
                'A_PtsF1', 'A_PtsA1', 'H_WonCurr', 'A_WonCurr', 'H_LostCurr', 'A_LostCurr', 'H_PtsFCurr', \
                'A_PtsFCurr', 'H_PtsACurr', 'A_PtsACurr', 'H2HWinRat_HmeTeam', 'H_HomeRec', 'A_AwayRec']
    #predictorsToNormalize = ['H_PtsF1', 'H_PtsA1', 'A_PtsF1', 'A_PtsA1', 'H_PtsFCurr', 'H_PtsACurr', 'A_PtsFCurr', 'A_PtsACurr']
    #rugbyDataScaled = pd.DataFrame(dict.fromkeys(predictors,[]))
    #rugbyData = rugbyData.append(pd.DataFrame([tempRow], columns=colNames), ignore_index=True)
    #rugbyDataScaled = pd.DataFrame(preprocessing.scale(rugbyData[predictorsToNormalize]), columns = predictorsToNormalize)
    
    #pointsToNormalize = ['H_Points1','A_Points1']
    predictorsToNormalize = ['H_PtsF1', 'H_PtsA1', 'A_PtsF1', 'A_PtsA1', 'H_PtsFCurr', 'H_PtsACurr', 'A_PtsFCurr', 'A_PtsACurr','H_Points1','A_Points1']
    min_max_scaler = preprocessing.MinMaxScaler()
    rugbyDataScaled = pd.DataFrame(min_max_scaler.fit_transform(rugbyData[predictorsToNormalize]), columns = predictorsToNormalize)
    
    predictorsExtra = [x for x in frameColumns if x not in predictorsToNormalize]
    scaledDataCombined = pd.concat([rugbyData[predictorsExtra], rugbyDataScaled], axis=1)
    
    return scaledDataCombined, frameColumns


def createTrainAndTest(rugbyData):
    rugbyTrain = rugbyData.loc[rugbyData["Year"] <= 2015, :]
    rugbyTest = rugbyData.loc[rugbyData["Year"] >= 2016, :]
    
    return rugbyTrain, rugbyTest

rugbyData, frameColumns = loadDataAndNormalize(connect)
rugbyTrain, rugbyTest = createTrainAndTest(rugbyData)


# The columns we'll use to predict the target
predictors = [columnName for columnName in frameColumns if columnName not in ["Year", "HomeTeamWinQ"]]

# Initialize our algorithm class
logisticRegression = sklm.LogisticRegression()

# The predictors we're using the train the algorithm.  Note how we only take the rows in the train folds.
trainPredictors = rugbyTrain[predictors]
# The target we're using to train the algorithm.
trainTarget = rugbyTrain["HomeTeamWinQ"]
# Training the algorithm using the predictors and target.
logisticRegression.fit(trainPredictors, trainTarget)
# We can now make predictions on the test fold
trainPredictions = logisticRegression.predict(rugbyTrain[predictors])
testPredictions = logisticRegression.predict(rugbyTest[predictors])

testTarget = np.array(rugbyTest["HomeTeamWinQ"])

accuracy = sum(testPredictions == testTarget)/float(len(testPredictions))
print(accuracy)







## Test code
#sqlQuery = '''SELECT * FROM totalMatchDataCombined'''    #1850 rows total
##sqlQuery = '''SELECT DISTINCT TeamName FROM seasonResults'''       #18 rows total
#rugbyDat = DB.readDatabase(connect,sqlQuery)
#rugbyDat.info() 