-- SQL commands to load the 15-column flights data into mysql
CREATE DATABASE IF NOT EXISTS flights;
use flights;

DROP TABLE flights;
CREATE TABLE flights (
     id INT NOT NULL AUTO_INCREMENT,
     DayOfWeek INT,
     FlightDate DATE NOT NULL,
     UniqueCarrier VARCHAR(2) NOT NULL,
     Origin VARCHAR(3),
     OriginCityName VARCHAR(34) NOT NULL,
     OriginState VARCHAR(2) NOT NULL,
     Dest VARCHAR(3) NULL,
     DestState VARCHAR(2) NOT NULL,
     DepTime INT NULL,
     DepDelay Double NULL,
     ArrTime Double NULL,
     ArrDelay Double NULL,
     Cancelled Double NULL,
     ActualElapsedTime Double NULL,
     Distance Double NULL,
     PRIMARY KEY (id)
);

LOAD DATA INFILE '/var/lib/mysql-files/ontime.csv'
INTO TABLE flights
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(DayOfWeek,@flightdate,UniqueCarrier,Origin,OriginCityName,OriginState,@dest,DestState,@deptime,@depdelay,@arrtime,@arrdelay,@cancelled,@actualelapsedtime,@distance)
SET FlightDate=STR_TO_DATE(@flightdate, '%Y-%m-%dT%H:%i:%sZ'),
Dest=nullif(@dest,''),
DepTime=nullif(@deptime,''),
DepDelay=nullif(@depdelay,''),
ArrTime=nullif(@arrtime,''),
ArrDelay=nullif(@arrdelay,''),
Cancelled=nullif(@cancelled,''),
ActualElapsedTime=nullif(@actualelapsedtime,''),
Distance=nullif(@distance,'')
;
