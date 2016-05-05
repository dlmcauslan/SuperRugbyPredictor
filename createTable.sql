CREATE DATABASE superRugbyPredictor

USE superRugbyPredictor

DROP TABLE IF EXISTS seasonResults

CREATE TABLE seasonResults (BonusPoints INT, Draw INT, 
GamesPlayed INT, Lost INT, Points INT, PointsAgainst INT, 
PointsDifferential INT, PointsFor INT, Position INT,
TeamName TEXT, Won INT, Year INT)

SELECT * FROM seasonResults

