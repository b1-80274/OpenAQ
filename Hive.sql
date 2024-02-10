CREATE EXTERNAL TABLE latest (
  city STRING,
  local_date TIMESTAMP,
  parameter STRING,
  unit STRING,
  reading DOUBLE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES('separatorChar'=',')
LOCATION '/user/OpenAQ/data/latest';

SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(local_date, 'yyyy-MM-dd\'T\'HH:mm:ssXXX')) FROM staging;


CREATE VIEW max_year AS
SELECT MAX(YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(local_date, 'yyyy-MM-dd\'T\'HH:mm:ssXXX')))) FROM staging;

CREATE TABLE data (
  city STRING,
  local_date TIMESTAMP,
  parameter STRING,
  unit STRING,
  reading DOUBLE )
STORED AS ORC;
  
CREATE VIEW max_year AS
SELECT MAX(YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(local_date, 'yyyy-MM-dd\'T\'HH:mm:ssXXX')))) FROM staging;



INSERT INTO data
SELECT city,from_unixtime(unix_timestamp(local_date, 'yyyy-MM-dd\'T\'HH:mm:ssXXX')) AS local_date,parameter,unit,reading
FROM staging;




CREATE MATERIALIZED VIEW dataset AS 

