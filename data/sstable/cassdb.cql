DROP KEYSPACE IF EXISTS CassDB;

CREATE KEYSPACE CassDB
   WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 2};

DESCRIBE KEYSPACES;

USE CassDB;

CREATE TABLE users(
   id UUID PRIMARY KEY,
   name text,
   address text,
   salary int,
   phone text
   );

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'ram', 'Hyderabad', 50000, '9848022338');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'robin', 'Hyderabad', 40000, '9848022339');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'rahman', 'Chennai', 45000, '9848022330');
INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'andi', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'azam', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'boni', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'paijo', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'sule', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'andre', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'taulani', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'sukiman', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'paijah', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'susi', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'rina', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'wati', 'Chennai', 45000, '9848022330');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'address null', '', 76833, '4324234233');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'', '', 57800, '9848022338');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'', '', 29000, '');

INSERT INTO users (id, name, address,
   salary, phone) VALUES(now(),'', '',0, '');


CREATE TABLE flights(
   ID int PRIMARY KEY,
   actualelapsedtime float,
   arrdelay float,
   arrtime int,
   cancelled float,
   dayofweek int,
   depdelay float,
   deptime text,
   dest text,
   deststate text,
   distance float,
   flightdate text,
   origin text,
   origincityname text,
   originstate text,
   uniquecarrier text
   );

COPY CassDB.flights (ID,DayOfWeek,FlightDate,UniqueCarrier,Origin,OriginCityName,OriginState,Dest,DestState,DepTime,DepDelay,ArrTime,ArrDelay,Cancelled,ActualElapsedTime,Distance)  FROM '/Users/daniar/Documents/GitPR/hillview/bin/../data/sstable/flights_data.csv' WITH DELIMITER=',' AND HEADER=TRUE; exit;
