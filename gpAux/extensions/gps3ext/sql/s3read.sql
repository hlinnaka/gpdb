-- ========
-- PROTOCOL
-- ========

-- create the database functions
CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS
   '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS
    '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;

-- declare the protocol name along with in/out funcs
CREATE PROTOCOL s3 (
    readfunc  = read_from_s3,
    writefunc = write_to_s3
);

--
-- This fetches everything from the bucket
CREATE READABLE EXTERNAL TABLE s3test_all(id int4, t text)
    LOCATION('s3://s3-external-1.amazonaws.com/gps3ext-test/ config=s3.conf')
FORMAT 'text';

CREATE TABLE all_from_s3 AS SELECT * FROM s3test_all;

-- Check that we got everything. (Selecting everything would create an excessive amount
-- of output)
select * from all_from_s3 where id < 10;
select * from all_from_s3 where id % 100 = 1
select * from all_from_s3 where id > 999990;



-- Fetch objects starting with "small"
CREATE READABLE EXTERNAL TABLE s3test_small(id int4, t text)
    LOCATION('s3://s3-external-1.amazonaws.com/gps3ext-test/small config=s3.conf')
FORMAT 'text';

SELECT * FROM s3test_small;



-- =======
-- CLEANUP
-- =======
--DROP TABLE all_from_s3;

--DROP EXTERNAL TABLE s3test_all;
--DROP EXTERNAL TABLE s3test_small;

--DROP PROTOCOL s3ext;
