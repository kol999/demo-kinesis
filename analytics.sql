

-- ** Aggregate (COUNT, AVG, etc.) + Sliding Row Window **
-- Performs function on the aggregate rows over a 10 row sliding window for a specified column. 
--          .----------.   .----------.   .----------.              
--          |  SOURCE  |   |  INSERT  |   |  DESTIN. |              
-- Source-->|  STREAM  |-->| & SELECT |-->|  STREAM  |-->Destination
--          |          |   |  (PUMP)  |   |          |              
--          '----------'   '----------'   '----------'               
-- STREAM (in-application): a continuously updated entity that you can SELECT from and INSERT into like a TABLE
-- PUMP: an entity used to continuously 'SELECT ... FROM' a source STREAM, and INSERT SQL results into an output STREAM
-- Create output stream, which can be used to send to a destination
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (sensor VARCHAR(8), temperature INTEGER);
-- Create pump to insert into output 
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
-- COUNT|AVG|MAX|MIN|SUM|STDDEV_POP|STDDEV_SAMP|VAR_POP|VAR_SAMP
SELECT STREAM sensor, AVG(temperature) OVER TEN_ROW_SLIDING_WINDOW AS avg_temp
FROM "SOURCE_SQL_STREAM_001"
-- Results partitioned by ticker and a 10-row sliding row window 
WINDOW TEN_ROW_SLIDING_WINDOW AS (PARTITION BY sensor ROWS 10 PRECEDING);
