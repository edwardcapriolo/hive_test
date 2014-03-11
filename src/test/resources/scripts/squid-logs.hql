-- Create a database and load squid log metrics
-- select newest URL entriy with GET method.

CREATE TABLE IF NOT EXISTS squidlogs (time DOUBLE, elapsed INT, remotehost STRING, codestatus STRING, bytes INT, method STRING, url STRING, rfc931 STRING, peerstatushost STRING, type STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ';
LOAD DATA LOCAL INPATH '$INPUT1' INTO TABLE squidlogs;
SELECT url, method, time FROM squidlogs WHERE method='GET' SORT BY time DESC LIMIT 1;