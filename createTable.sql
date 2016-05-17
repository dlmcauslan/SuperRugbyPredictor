CREATE DATABASE superRugbyPredictor

USE superRugbyPredictor

DROP TABLE IF EXISTS seasonResults

CREATE TABLE seasonResults (BonusPoints INT, Draw INT, 
GamesPlayed INT, Lost INT, Points INT, PointsAgainst INT, 
PointsDifferential INT, PointsFor INT, Position INT,
TeamName TEXT, Won INT, Year INT)

DROP TABLE IF EXISTS table2

CREATE TABLE table2 (BonusPoints INT) 

SELECT * FROM seasonResultsPastResults
SELECT * FROM seasonResults

