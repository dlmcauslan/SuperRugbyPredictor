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
    
Modified: 16/10/2016
    * Created buildModelAndPredict method which creates a model, trains it on the training set of data,
    then calculates the accuracy of the model on both the test and train sets.
    * Calculated the baseline accuracy by using a test where the hometeam always wins.
    * Built test models using logistic regression, Linear SVC, Decision Tree, RandomForest, ExtraRandomForest and Nearest Neighbours.
    * Currently logistic regression and the RandomForest models have the best accuracy.
    * Performed an optimization on logistic regression.
    * Optimized the random forest model over max_depth and min_samples_split.

'''
# Import packages
import numpy as np
import pandas as pd
import sklearn as sk
from sklearn import preprocessing
import sklearn.linear_model as sklm
from sklearn import svm
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
from sklearn.grid_search import GridSearchCV
import databaseFunctions as DB
import seaborn as sns
import matplotlib.pyplot as plt

# Makes dataframes display better on my PC.
pd.set_option('display.width', 190)
pd.options.display.max_columns = 100
pd.options.display.max_rows = 150
pd.set_option('precision', 2)
np.set_printoptions(precision=4, threshold=2000, edgeitems=None, linewidth=200, suppress=None, nanstr=None, infstr=None, formatter=None)

# SQL Connection
#host = "localhost"; user = "dlmsql"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"            # Surface
host = "localhost"; user = "root"; passwd = "DLMPa$$word"; db = "superRugbyPredictor"               # Desktop
connect = [host, user, passwd, db]


def loadDataAndNormalize(connect):
    # For the first test lets only include the current years data and last years data
    sqlQuery = '''SELECT Year, HomeTeamWinQ, HomeTeamWinSplit, H_Won1, H_Lost1, H_Points1, H_PtsF1, H_PtsA1, \
                    A_Won1, A_Lost1, A_Points1, A_PtsF1, A_PtsA1, H_WonCurr, A_WonCurr, H_LostCurr, A_LostCurr, \
                    H_Won2, H_Lost2, H_Points2, H_PtsF2, H_PtsA2, A_Won2, A_Lost2, A_Points2, A_PtsF2, A_PtsA2, \
                    H_Won3, H_Lost3, H_Points3, H_PtsF3, H_PtsA3, A_Won3, A_Lost3, A_Points3, A_PtsF3, A_PtsA3, \
                    H_PtsFCurr, A_PtsFCurr, H_PtsACurr, A_PtsACurr, H2HWinRat_HmeTeam, H_HomeRec, A_AwayRec FROM totalMatchDataCombined'''    #1850 rows total
    rugbyData = DB.readDatabase(connect,sqlQuery)
    
    frameColumns = ['Year','HomeTeamWinQ', 'HomeTeamWinSplit', 'H_Won1', 'H_Lost1', 'H_Points1', 'H_PtsF1', 'H_PtsA1', 'A_Won1', 'A_Lost1', 'A_Points1', \
                'A_PtsF1', 'A_PtsA1', 'H_WonCurr', 'A_WonCurr', 'H_LostCurr', 'A_LostCurr', 'H_PtsFCurr', \
                'A_PtsFCurr', 'H_PtsACurr', 'A_PtsACurr', 'H2HWinRat_HmeTeam', 'H_HomeRec', 'A_AwayRec']# , \
                #'H_Won2', 'H_Lost2', 'H_Points2', 'H_PtsF2', 'H_PtsA2', 'A_Won2', 'A_Lost2', 'A_Points2', 'A_PtsF2', 'A_PtsA2', \
                #'H_Won3', 'H_Lost3', 'H_Points3', 'H_PtsF3', 'H_PtsA3', 'A_Won3', 'A_Lost3', 'A_Points3', 'A_PtsF3', 'A_PtsA3']

    predictorsToNormalize = ['H_PtsF1', 'H_PtsA1', 'A_PtsF1', 'A_PtsA1', 'H_PtsFCurr', 'H_PtsACurr', 'A_PtsFCurr', 'A_PtsACurr','H_Points1','A_Points1'] #, \
                            #'H_Points2', 'H_PtsF2', 'H_PtsA2','A_Points2', 'A_PtsF2', 'A_PtsA2', \
                            #'H_Points3', 'H_PtsF3', 'H_PtsA3','A_Points3', 'A_PtsF3', 'A_PtsA3']
    min_max_scaler = preprocessing.MinMaxScaler()
    rugbyDataScaled = pd.DataFrame(min_max_scaler.fit_transform(rugbyData[predictorsToNormalize]), columns = predictorsToNormalize)
    
    predictorsExtra = [x for x in frameColumns if x not in predictorsToNormalize]
    scaledDataCombined = pd.concat([rugbyData[predictorsExtra], rugbyDataScaled], axis=1)
    
    return scaledDataCombined, frameColumns


def createTrainAndTest(rugbyData):
    rugbyTrain = rugbyData.loc[rugbyData["Year"] <= 2014, :]
    rugbyTest = rugbyData.loc[rugbyData["Year"] >= 2015, :]
    
    return rugbyTrain, rugbyTest

rugbyData, frameColumns = loadDataAndNormalize(connect)
rugbyTrain, rugbyTest = createTrainAndTest(rugbyData)

def buildModelAndPredict(algorithm, rugbyTrain, rugbyTest):
    # The columns we'll use to predict the target
    predictors = [columnName for columnName in frameColumns if columnName not in ["Year", "HomeTeamWinQ", "HomeTeamWinSplit"]]
    # The predictors we're using the train the algorithm.  Note how we only take the rows in the train folds.
    trainPredictors = rugbyTrain[predictors]
    # The target we're using to train the algorithm.
    trainTarget = np.array(rugbyTrain["HomeTeamWinQ"])
    testTarget = np.array(rugbyTest["HomeTeamWinQ"])
    # Training the algorithm using the predictors and target.
    algorithm.fit(trainPredictors, trainTarget)
    # We can now make predictions on the test and train data
    trainPredictions = algorithm.predict(rugbyTrain[predictors])
    testPredictions = algorithm.predict(rugbyTest[predictors])
    # Model accuracy
    accuracyTest = accuracy_score(testTarget, testPredictions)
    accuracyTrain = accuracy_score(trainTarget, trainPredictions)
    return accuracyTest, accuracyTrain, testPredictions

def buildModelAndPredict2(algorithm, rugbyTrain, rugbyTest):
    ''' Same as buildModelAndPredict, but aims to predict the winning margin, whether its 12- or 13+'''
    # The columns we'll use to predict the target
    predictors = [columnName for columnName in frameColumns if columnName not in ["Year", "HomeTeamWinQ", "HomeTeamWinSplit"]]
    # The predictors we're using the train the algorithm.  Note how we only take the rows in the train folds.
    trainPredictors = rugbyTrain[predictors]
    # The target we're using to train the algorithm.
    trainTarget = np.array(rugbyTrain["HomeTeamWinSplit"])
    testTarget = np.array(rugbyTest["HomeTeamWinSplit"])
    # Training the algorithm using the predictors and target.
    algorithm.fit(trainPredictors, trainTarget)
    # We can now make predictions on the test and train data
    trainPredictions = algorithm.predict(rugbyTrain[predictors])
    testPredictions = algorithm.predict(rugbyTest[predictors])
    # Model accuracy
    accuracyTest = accuracy_score(testTarget, testPredictions)
    accuracyTrain = accuracy_score(trainTarget, trainPredictions)
    return accuracyTest, accuracyTrain, testPredictions

## Baseline test, if homeTeam wins every game.
trainTarget = np.array(rugbyTrain["HomeTeamWinQ"])
testTarget = np.array(rugbyTest["HomeTeamWinQ"])
accuracyTest = sum(testTarget == 1)/float(len(testTarget))
accuracyTrain = sum(trainTarget == 1)/float(len(testTarget))
print(accuracyTest, accuracyTrain, "Baseline")

accuracyTest, accuracyTrain, logPredictions = buildModelAndPredict(sklm.LogisticRegression(max_iter = 100, C = 0.82), rugbyTrain, rugbyTest)
print(accuracyTest, accuracyTrain, "Logistic Regression")

#accuracyTest, accuracyTrain = buildModelAndPredict(svm.SVC(kernel = 'linear', C = 100), rugbyTrain, rugbyTest)
#print(accuracyTest, accuracyTrain, "Linear SVC")

#accuracyTest, accuracyTrain = buildModelAndPredict(DecisionTreeClassifier(max_features = len(frameColumns)-2, max_depth = 6, min_samples_split=25, random_state=0), rugbyTrain, rugbyTest)
#print(accuracyTest, accuracyTrain, "DecisionTree")
#
#accuracyTest, accuracyTrain = buildModelAndPredict(RandomForestClassifier(n_estimators = 500, max_features = len(frameColumns)-2, max_depth = 6, min_samples_split=16, random_state=0, n_jobs=4), rugbyTrain, rugbyTest)
#print(accuracyTest, accuracyTrain, "RandomForest")

accuracyTest, accuracyTrain, forestPredictions = buildModelAndPredict(ExtraTreesClassifier(n_estimators = 500, max_features = len(frameColumns)-3, max_depth = 6, min_samples_split=16, random_state=0, n_jobs=4), rugbyTrain, rugbyTest)
print(accuracyTest, accuracyTrain, "ExtraRandomForest")

accuracyTest, accuracyTrain, neighboursPredictions = buildModelAndPredict(KNeighborsClassifier(n_neighbors=30, algorithm='auto', leaf_size=30), rugbyTrain, rugbyTest)
print(accuracyTest, accuracyTrain, "NearestNeighbours")

agreePred = []
agreeTest = []
for n in range(len(forestPredictions)):
    if forestPredictions[n]==logPredictions[n]:
        agreePred.append(forestPredictions[n])
        agreeTest.append(testTarget[n])

print(accuracy_score(agreeTest, agreePred))

accuracyTest, accuracyTrain, logPredictions = buildModelAndPredict2(sklm.LogisticRegression(max_iter = 100, C = 0.82), rugbyTrain, rugbyTest)
print(accuracyTest, accuracyTrain, "Logistic Regression")

accuracyTest, accuracyTrain, forestPredictions = buildModelAndPredict2(ExtraTreesClassifier(n_estimators = 500, max_features = len(frameColumns)-3, max_depth = 6, min_samples_split=16, random_state=0, n_jobs=4), rugbyTrain, rugbyTest)
print(accuracyTest, accuracyTrain, "ExtraRandomForest")

def optimizeRandomForest(rugbyTrain, rugbyTest):
    ''' Perform a gridsearch to optimize the logistic regression model '''
    param_grid = {'maxDepth': np.arange(5,16,1), 'minSamplesSplit': np.arange(4,45,2)}
    scoresGrid = np.zeros((len(param_grid['maxDepth'])+1, len(param_grid['minSamplesSplit'])+1))
    scoresGrid[0][1:]= param_grid['minSamplesSplit']
    for n in range(len(param_grid['maxDepth'])):
        depthValue = param_grid['maxDepth'][n]
        scoresGrid[n+1][0] = depthValue
        for m in range(len(param_grid['minSamplesSplit'])):            
            samplesSplit = param_grid['minSamplesSplit'][m]
            accuracyTest, accuracyTrain = buildModelAndPredict(ExtraTreesClassifier(n_estimators = 500, max_features = len(frameColumns)-2, max_depth = depthValue, min_samples_split=samplesSplit, random_state=0, n_jobs=4), rugbyTrain, rugbyTest)
            scoresGrid[n+1][m+1] = accuracyTest
    print(scoresGrid)
    return scoresGrid

#scoresGrid = optimizeRandomForest(rugbyTrain, rugbyTest)

def optimizeLogisticRegression(rugbyTrain, rugbyTest):
    ''' Perform a gridsearch to optimize the logistic regression model '''
    param_grid = {'C': np.arange(0.1,2,.01)}
    #param_grid = {'C': [.1, 1, 10, 100, 1000]}
    testScores = []
    for cValue in param_grid['C']:
        accuracyTest, accuracyTrain = buildModelAndPredict(sklm.LogisticRegression(max_iter = 500, C = cValue), rugbyTrain, rugbyTest)
        testScores.append((accuracyTest, accuracyTrain, cValue))
    #print(testScores)
    
    print('C = {}, accuracy = {}'.format(max(testScores)[2],max(testScores)[0]))
    
    fig1 = plt.figure(1)
    fig1.clf()
    plt.plot([x[2] for x in testScores], [x[0] for x in testScores])
    plt.plot([x[2] for x in testScores], [x[1] for x in testScores])
    fig1.show()

#optimizeLogisticRegression(rugbyTrain, rugbyTest)
