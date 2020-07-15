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

CREATE TABLE test(
   id UUID PRIMARY KEY,
   name text,
   salary int,
   text text,
   int int,
   boolean boolean,
   ascii ascii,
   inet inet,
   timeuuid timeuuid,
   smallint smallint,
   tinyint tinyint,
   varint varint,
   bigint bigint,
   decimal decimal,
   double double,
   float float,
   time time,
   timestamp timestamp,
   date date,
   duration duration,
   blob blob
   );

INSERT INTO test (id, name, salary, text, int, boolean, ascii, inet, timeuuid, smallint,
   tinyint, varint, bigint, decimal, double, float, time, timestamp, date, duration, blob)
   VALUES(now(),'Mr. NULL',null, null, null, null, null, null, null, null, null, null, null, null, null,
   null, null, null, null, null, null
);

INSERT INTO test (id, name, salary, text, int, boolean, ascii, inet, timeuuid, smallint,
   tinyint, varint, bigint, decimal, double, float, time, timestamp, date, duration, blob)
   VALUES(now(),'Mr. Test', 45000, null, null, true, '35', '127.0.0.1', 50554d6e-29bb-11e5-b345-feff819cdc9f,
    1, 2, 10, toTimestamp(now()), 3.7875, 6714592679.3400884579357, 3.14753, '13:30:23.123',
    '2017-05-05 15:00:00.000', '2020-07-14', P4Y6M3DT12H30M5S, textAsBlob('adb14fbe076f6b94444c660e36a400151f26fc6f')
);


CREATE TABLE counter(
   id int PRIMARY KEY,
   counter counter
   );

UPDATE counter SET counter = counter + 1 WHERE id = 2;

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

COPY CassDB.flights (ID,DayOfWeek,FlightDate,UniqueCarrier,Origin,OriginCityName,OriginState,Dest,DestState,DepTime,DepDelay,ArrTime,ArrDelay,Cancelled,ActualElapsedTime,Distance)  FROM './flights_data.csv' WITH DELIMITER=',' AND HEADER=TRUE; exit;
