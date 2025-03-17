ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

SET hive.exec.max.dynamic.partitions=116;
SET hive.exec.max.dynamic.partitions.pernode=116;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

USE hlapovasv;

DROP TABLE IF EXISTS Logs1;
DROP TABLE IF EXISTS IpRegions;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Subnets;

CREATE EXTERNAL TABLE Logs1 (
    ip STRING,
    date_ts STRING,
    request STRING,
    page_size SMALLINT,
    status_code INT,
    browser STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^((?:\\d{1,3}\\.){3}\\d{1,3})\\s+(\\d{8})\\d+\\s+((?:ht|f)tps?://(?:w{3})?\\S+)\\s+(\\d{1,4})\\s+(\\d{3})\\s+(\\w+\\S+)\\s+.*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';

DROP TABLE IF EXISTS Logs;

CREATE EXTERNAL TABLE Logs (
    ip STRING,
    request STRING,
    page_size SMALLINT,
    status_code INT,
    browser STRING
)
PARTITIONED BY (date_ts STRING)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs
PARTITION (date_ts)
SELECT ip, request, page_size, status_code,
       browser,
       date_ts
FROM Logs1;


CREATE EXTERNAL TABLE IpRegions(
    ip STRING,
    region STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M/';


CREATE EXTERNAL TABLE Users (
    ip STRING,
    browser STRING,
    gender STRING,
    age INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M/';


CREATE EXTERNAL TABLE Subnets (
     ip STRING,
     mask STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

SELECT * FROM Logs LIMIT 10;
SELECT * FROM IpRegions LIMIT 10;
SELECT * FROM Users LIMIT 10;
SELECT * FROM Subnets LIMIT 10;