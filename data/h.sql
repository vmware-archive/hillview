DROP TABLE IF EXISTS flights;

CREATE TABLE flights (
    k                 INT      NOT NULL AUTO_INCREMENT,
    FlightDate        DATETIME NOT NULL,
    UniqueCarrier     NCHAR(2) NOT NULL,
    Origin            NCHAR(3) NOT NULL,
    Cancelled         INT      NOT NULL,
    ActualElapsedTime TIME,
    Distance          INT,
    PRIMARY KEY (k)
);

INSERT INTO flights (FlightDate, UniqueCarrier, Origin, Cancelled, ActualElapsedTime, Distance)
VALUES
  ("2011-11-05", "AA", "SFO", 1, "11:23", 102),
  ("2011-11-05", "AA", "XXX", 1, "11:00", 3),
  ("2011-11-05", "F9", "TTT", 0, "11:23", 1),
  ("2011-11-03", "EV", "SEA", 1, "11:24", 108),
  ("2011-11-10", "AA", "SFO", 0, "10:23", 102),
  ("2011-11-01", "AA", "CMU", 0, "10:23", 12),
  ("2011-11-11", "EV", "SEA", 0, "10:24", 108);

SELECT * FROM flights;

SELECT UniqueCarrier, Cancelled, COUNT(*) FROM flights
WHERE (Cancelled = 0 AND UniqueCarrier = 'EV') OR (Cancelled > 0)
GROUP BY UniqueCarrier, Cancelled
ORDER BY Cancelled ASC, UniqueCarrier ASC
LIMIT 0, 19
;
