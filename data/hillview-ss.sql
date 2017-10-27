/*
This file is a benchmarking script for SQL Server running on linux.
It creates a memory-resident table with 100M rows and then computes
a histogram on a numeric column of that table.  The histogram
is repeated 10 times.
*/

:on error exit

USE [master];
DROP DATABASE IF EXISTS hillview
GO

CREATE DATABASE hillview
  ON
  PRIMARY (NAME=hillview_data, FILENAME='/hillview/ss/hillview_data.mdf'),
  FILEGROUP hillview_fg CONTAINS MEMORY_OPTIMIZED_DATA
     (NAME = hillview_mod, FILENAME = '/hillview/ss/hillview_mod');
GO

USE hillview
DROP TABLE IF EXISTS rand
GO

DROP PROCEDURE IF EXISTS InsertRand
GO

CREATE PROCEDURE InsertRand @NumRows INT, @MinVal REAL, @MaxVal REAL
AS
BEGIN
  DECLARE @i INT;
  SET NOCOUNT ON
  SET @i = 1;
  WHILE @i <= @NumRows
  BEGIN
    INSERT INTO rand (number) VALUES
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal)),
       (@MinVal + RAND() * (@MaxVal - @MinVal));
    SET @i = @i + 50;
  END
END
GO

CREATE TABLE rand (
    k      INT  NOT NULL IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
    number REAL NOT NULL
) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);

DECLARE @beginTime AS DATETIME, @rangeTime AS DATETIME, @startTime AS DATETIME, @endTime as DATETIME
SELECT @beginTime = SYSDATETIME();

DECLARE @rows INT = 1 * 1000 * 1000
EXEC InsertRand @rows, 0, 100000

SELECT COUNT(*) FROM rand

DECLARE @buckets INT = 40
SELECT @startTime = SYSDATETIME()

DECLARE @min REAL, @max REAL, @scale REAL
SELECT @min = MIN(number), @max = MAX(number) FROM rand
SELECT @scale = @buckets / (@max - @min)

SELECT @rangeTime = SYSDATETIME()
select datediff(millisecond, @beginTime, @startTime) as tinsert
select datediff(millisecond, @startTime, @rangeTime) as trange

DECLARE @repeats INT = 10
WHILE @repeats > 0
BEGIN
  SELECT @rangeTime = SYSDATETIME()
  SELECT COUNT(FLOOR((number - @min) * @buckets/(@max-@min)))
  FROM rand
  GROUP BY FLOOR((number - @min) * @buckets / (@max - @min));
  set @endTime = SYSDATETIME()
  select datediff(millisecond, @rangeTime, @endTime) as thisto;
  SET @repeats = @repeats - 1
END

GO
