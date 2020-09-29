-- Script to restore data into Postgress or Greenplum

DROP DATABASE flights;
CREATE DATABASE flights WITH TEMPLATE = template0 ENCODING = 'UTF8';

\set ON_ERROR_STOP on
\connect flights

CREATE TABLE public.flights (
    year smallint,
    quarter smallint,
    month smallint,
    dayofmonth smallint,
    dayofweek smallint,
    flightdate timestamp without time zone,
    reporting_airline varchar(50),
    ID_reporting_airline int,
    IATA_CODE_Reporting_Airline varchar(50),
    Tail_Number varchar(50),
    Flight_Number_Reporting_Airline int,
    OriginAirportID int,
    OriginAirportSeqID int,
    OriginCityMarketID int,
    Origin varchar(50),
    OriginCityName varchar(50),
    OriginState varchar(50),
    OriginStateFips int,
    OriginStateName varchar(50),
    OriginWac int,
    DestAirportID int,
    DestAirportSeqID int,
    DestCityMarketID int,
    Dest varchar(50),
    DestCityName varchar(50),
    DestState varchar(50),
    DestStateFips int,
    DestStateName varchar(50),
    DestWac int,
    CRSDepTime varchar(50),  -- mostly int, but there are some files where it's ""
    DepTime int,
    DepDelay float8,
    DepDelayMinutes float8,
    DepDel15 float8,
    DepartureDelayGroups int,
    DepTimeBlk varchar(50),
    TaxiOut float8,
    WheelsOff varchar(50),  -- some files have strings here, most are int
    WheelsOn varchar(50),
    TaxiIn float8,
    CRSArrTime int,
    ArrTime int,
    ArrDelay float8,
    ArrDelayMinutes float8,
    ArrDel15 float8,
    ArrivalDelayGroups int,
    ArrTimeBlk varchar(50),
    Cancelled float8,
    CancellationCode varchar(50),
    Diverted float8,
    CRSElapsedTime float8,
    ActualElapsedTime float8,
    AirTime float8,
    Flights float8,
    Distance float8,
    DistanceGroup int,
    CarrierDelay float8,
    WeatherDelay float8,
    NASDelay float8,
    SecurityDelay float8,
    LateAircraftDelay float8,
    FirstDepTime int,
    TotalAddGTime float8,
    LongestAddGTime float8,
    DivAirportLandings int,
    DivReachedDest float8,
    DivActualElapsedTime float8,
    DivArrDelay float8,
    DivDistance float8,
    Div1Airport varchar(50),
    Div1AirportID int,
    Div1AirportSeqID int,
    Div1WheelsOn int,
    Div1TotalGTime float8,
    Div1LongestGTime float8,
    Div1WheelsOff int,
    Div1TailNum varchar(50),
    Div2Airport varchar(50),
    Div2AirportID int,
    Div2AirportSeqID int,
    Div2WheelsOn int,
    Div2TotalGTime float8,
    Div2LongestGTime float8,
    Div2WheelsOff int,
    Div2TailNum varchar(50),
    Div3Airport varchar(50),
    Div3AirportID int,
    Div3AirportSeqID int,
    Div3WheelsOn int,
    Div3TotalGTime float8,
    Div3LongestGTime float8,
    Div3WheelsOff int,
    Div3TailNum varchar(50),
    Div4Airport  varchar(1),
    Div4AirportID  varchar(1),
    Div4AirportSeqID  varchar(1),
    Div4WheelsOn  varchar(1),
    Div4TotalGTime  varchar(1),
    Div4LongestGTime  varchar(1),
    Div4WheelsOff  varchar(1),
    Div4TailNum  varchar(1),
    Div5Airport  varchar(1),
    Div5AirportID  varchar(1),
    Div5AirportSeqID  varchar(1),
    Div5WheelsOn  varchar(1),
    Div5TotalGTime  varchar(1),
    Div5LongestGTime  varchar(1),
    Div5WheelsOff  varchar(1),
    Div5TailNum  varchar(1),
    Column_109  varchar(1)
) DISTRIBUTED BY (flightDate);

-- iconv is necessary because some files have illegal utf-8 characters
COPY flights FROM PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1987_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1987_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1987_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1987_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1988_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1989_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1990_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1991_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1992_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1993_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1994_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1995_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1996_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1997_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1998_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_1999_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2000_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2001_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2002_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2003_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2004_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2005_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2006_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2007_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2008_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2009_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2010_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2011_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2012_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2013_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2014_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2015_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2016_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2017_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2018_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_10.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_11.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_12.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_6.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_7.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_8.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2019_9.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2020_1.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2020_2.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2020_3.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2020_4.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));
COPY flights from PROGRAM 'gunzip </home/gpadmin/flights/On_Time_On_Time_Performance_2020_5.csv.gz | iconv -c' WITH (FORMAT CSV, DELIMITER ',', HEADER,
             FORCE_NULL (WheelsOff,WheelsOn,FirstDepTime,Div1WheelsOn,Div1WheelsOff,Div2WheelsOn,Div2WheelsOff,Div3WheelsOn,
                         Div3WheelsOff,DepTime,ArrTime,OriginStateFips,DestStateFips,CrsArrTime));

CREATE TABLE f AS SELECT(DayOfWeek, FlightDate, reporting_airline, Origin, OriginCityName, OriginState, Dest, DestCityName, DestState, DepTime, DepDelay, ArrTime, ArrDelay, Cancelled, ActualElapsedTime, Distance) FROM flights DISTRIBUTED BY (FlightDate);
