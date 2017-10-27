/*
This file is a benchmarking script for MySql running on linux.
It creates a memory-resident table with 100M rows and then computes
a histogram on a numeric column of that table.  The histogram
is repeated 10 times.
*/
set tmp_table_size = 1024 * 1024 * 1024 * 10;
set max_heap_table_size = 1024 * 1024 * 1024 * 10;
set default_storage_engine = memory;

DROP DATABASE IF EXISTS hillview;
CREATE DATABASE IF NOT EXISTS hillview;
USE hillview;

DROP TABLE IF EXISTS rand;

select CONCAT('storage engine: ', @@default_storage_engine) as INFO;

CREATE TABLE rand (
    k      INT    NOT NULL AUTO_INCREMENT,
    number DOUBLE NOT NULL,
    PRIMARY KEY (k)
) ENGINE = MEMORY;

DELIMITER $$
CREATE PROCEDURE InsertRand(IN NumRows INT, IN MinVal DOUBLE, IN MaxVal DOUBLE)
BEGIN
  DECLARE i INT;
  SET FOREIGN_KEY_CHECKS = 0;
  SET UNIQUE_CHECKS = 0;
  SET AUTOCOMMIT = 0;
  SET i = 1;
  START TRANSACTION;
  WHILE i <= NumRows DO
    INSERT INTO rand (number) VALUES
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal)),
       (MinVal + RAND() * (MaxVal - MinVal));
    SET i = i + 50;
  END WHILE;
  COMMIT;
  END$$
DELIMITER ;

SELECT @beginTime := NOW(4);
CALL InsertRand(100 * 1000 * 1000, 0, 100000);

SELECT COUNT(*) FROM rand;

SELECT @buckets := 40;

SELECT @startTime := NOW(4);

SELECT @min := MIN(number), @max := MAX(number) FROM rand;
SELECT @rangeTime := NOW(4);
SELECT @scale = @buckets / (@max - @min);

select timestampdiff(microsecond, @beginTime, @startTime) / 1000 as tinsert;
select timestampdiff(microsecond, @startTime, @rangeTime) / 1000 as trange;

DELIMITER $$
CREATE PROCEDURE measureHisto(IN count INT)
BEGIN
  DECLARE i INT;
  SET i = 1;
  WHILE i <= count DO
    SELECT @rangeTime := NOW(4);
    SELECT COUNT(FLOOR((number - @min) * @buckets/(@max-@min)))
    FROM rand
    GROUP BY FLOOR((number - @min) * @buckets / (@max - @min));
    set @endTime = NOW(4);
    select timestampdiff(microsecond, @rangeTime, @endTime) / 1000 as thisto;
    SET i = i + 1;
  END WHILE;
  END$$
DELIMITER ;

CALL measureHisto(10);

/*
select timediff(
    (select update_time from information_schema.tables where table_name='rand'),
    (select create_time from information_schema.tables where table_name='rand')
) as data_load_time_diff;
*/
