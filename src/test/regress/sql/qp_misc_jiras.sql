set DateStyle="ISO, MDY";
set gp_create_table_random_default_distribution=off;
set optimizer_print_missing_stats = off;
create schema qp_misc_jiras;


-- Test that "INSERT INTO SELECT * FROM <external table>" works.
create table qp_misc_jiras.tbl3301_foo (c1 int);
create external web table qp_misc_jiras.tbl3301_bar(like qp_misc_jiras.tbl3301_foo) execute 'echo 1' on master format 'csv';

insert into qp_misc_jiras.tbl3301_foo select * from qp_misc_jiras.tbl3301_bar;
select * from qp_misc_jiras.tbl3301_foo;

-- Test that altering the datatype of an indexed column works
create table qp_misc_jiras.tbl1318(dummy integer, aa text not null);
create index tbl1318_daa on qp_misc_jiras.tbl1318(dummy,aa);
alter table qp_misc_jiras.tbl1318 alter column aa type integer using bit_length(aa);
drop index qp_misc_jiras.tbl1318_daa;
create index tbl1318_daa on qp_misc_jiras.tbl1318 using bitmap(dummy,aa);
alter table qp_misc_jiras.tbl1318 alter column aa type bigint using bit_length(aa::text);
drop index qp_misc_jiras.tbl1318_daa;
drop table qp_misc_jiras.tbl1318;

-- Test for the upstream bug with combocids:
-- https://www.postgresql.org/message-id/48B87164.4050802%40soe.ucsc.edu
CREATE TABLE qp_misc_jiras.tbl3403_tab(a INT);
begin;
INSERT INTO qp_misc_jiras.tbl3403_tab VALUES(1);
INSERT INTO qp_misc_jiras.tbl3403_tab VALUES(2);
INSERT INTO qp_misc_jiras.tbl3403_tab VALUES(3);
SAVEPOINT s; DELETE FROM qp_misc_jiras.tbl3403_tab; ROLLBACK TO SAVEPOINT s;
SELECT a FROM qp_misc_jiras.tbl3403_tab ORDER BY a;
SAVEPOINT s; DELETE FROM qp_misc_jiras.tbl3403_tab; ROLLBACK TO SAVEPOINT s;
SAVEPOINT s; DELETE FROM qp_misc_jiras.tbl3403_tab; ROLLBACK TO SAVEPOINT s;
SAVEPOINT s; DELETE FROM qp_misc_jiras.tbl3403_tab; ROLLBACK TO SAVEPOINT s;
SELECT a FROM qp_misc_jiras.tbl3403_tab ORDER BY a;
COMMIT;
SELECT a FROM qp_misc_jiras.tbl3403_tab ORDER BY a;
drop table qp_misc_jiras.tbl3403_tab;

-- Check that CTAS chooses a sensible distribution key.
create table qp_misc_jiras.tbl2788 as select * from generate_series(1, 1000);
\d qp_misc_jiras.tbl2788;
drop table qp_misc_jiras.tbl2788;

-- Test CTAS, with a subquery in the select list.
create table qp_misc_jiras.tbl2859 (domain integer, class integer, attr text, value integer) distributed by (domain);
insert into qp_misc_jiras.tbl2859 values(1, 1, 'A', 1);
insert into qp_misc_jiras.tbl2859 values(2, 1, 'A', 0);
insert into qp_misc_jiras.tbl2859 values(3, 0, 'B', 1);

SELECT attr, class, (select count(distinct class) from qp_misc_jiras.tbl2859) as dclass FROM qp_misc_jiras.tbl2859 GROUP BY attr, class ORDER BY attr;

create table qp_misc_jiras.tbl2859a as 
SELECT attr, class, (select count(distinct class) from qp_misc_jiras.tbl2859) as dclass FROM qp_misc_jiras.tbl2859 GROUP BY attr, class
distributed by (attr);

drop table qp_misc_jiras.tbl2859;
drop table qp_misc_jiras.tbl2859a;

create table qp_misc_jiras.tbl3511 (i int);
insert into qp_misc_jiras.tbl3511 values (1),(2),(3),(4),(5);
select * FROM qp_misc_jiras.tbl3511 where i = (select distinct max(i) from qp_misc_jiras.tbl3511);
drop table qp_misc_jiras.tbl3511;
-- 
-- Create parent qp_misc_jiras.tbl1544 table
-- 
CREATE TABLE qp_misc_jiras.tbl1544
( pid INTEGER UNIQUE,
  pdate DATE NOT NULL,
  pval TEXT
)
;

-- 
-- Child tables
-- 
CREATE TABLE qp_misc_jiras.tbl1544_child_depth_1_y_2000_year (
CONSTRAINT tbl1544_child_depth_1_y_2000_year_pdate_check
CHECK (pdate >= '2000-01-01'::date AND pdate < '2001-01-01'::date))
INHERITS (qp_misc_jiras.tbl1544)
;
CREATE TABLE qp_misc_jiras.tbl1544_child_depth_1_y_2001_year (
CONSTRAINT tbl1544_child_depth_1_y_2001_year_pdate_check
CHECK (pdate >= '2001-01-01'::date AND pdate < '2002-01-01'::date))
INHERITS (qp_misc_jiras.tbl1544)
;

-- 
-- Inserting redirection triggers
-- 
CREATE RULE rule_tbl1544_child_depth_1_y_2000_year AS
ON INSERT TO qp_misc_jiras.tbl1544
WHERE new.pdate >= '2000-01-01'::date AND new.pdate < '2001-01-01'::date
DO INSTEAD
INSERT INTO qp_misc_jiras.tbl1544_child_depth_1_y_2000_year (pid, pdate, pval)
VALUES (new.pid, new.pdate, new.pval)
;
CREATE RULE rule_tbl1544_child_depth_1_y_2001_year AS
ON INSERT TO qp_misc_jiras.tbl1544
WHERE new.pdate >= '2001-01-01'::date AND new.pdate < '2002-01-01'::date
DO INSTEAD
INSERT INTO qp_misc_jiras.tbl1544_child_depth_1_y_2001_year (pid, pdate, pval)
VALUES (new.pid, new.pdate, new.pval)
;

-- 
-- Add Data
-- 
INSERT INTO qp_misc_jiras.tbl1544 VALUES(1,'2000-01-01','xxx');
INSERT INTO qp_misc_jiras.tbl1544 VALUES(2,'2001-01-01','xxx');

delete from qp_misc_jiras.tbl1544 where pdate='2000-01-01';
update qp_misc_jiras.tbl1544 set pval='yyy' where pdate='2000-01-01';

drop table qp_misc_jiras.tbl1544 cascade;
CREATE TYPE qp_misc_jiras.tbl4009_mediageoitem AS ( loctn_type character varying,
loctn_nm character varying,
loctn_desc character varying,
tot_trk_impsn bigint,
all_impsn bigint, 
tot_trk_click bigint, 
all_click bigint,
tot_trk_conv bigint,
all_conv bigint,
rank integer);
create role tbl4009_gpadmin;
alter type qp_misc_jiras.tbl4009_mediageoitem owner to tbl4009_gpadmin;
select typname, rolname as owner from pg_type,pg_authid where typname='tbl4009_mediageoitem' and pg_type.typowner=pg_authid.oid;
select relname, rolname as owner from pg_class,pg_authid where relname='tbl4009_mediageoitem' and pg_class.relowner=pg_authid.oid;
drop type qp_misc_jiras.tbl4009_mediageoitem;
drop role tbl4009_gpadmin;
-- create schemas

create schema tbl4105_sdc_stage;
create schema tbl4105_sandbox_gp;

-- create the tables
CREATE TABLE tbl4105_sdc_stage.curr_cust_cmunty_membr (
    cust_cmunty_user_id integer,
    user_friend_id integer,
    user_hash_key smallint,
    days_visited bigint,
    page_views bigint,
    is_friend boolean
) distributed by (user_hash_key);

CREATE TABLE tbl4105_sandbox_gp.curr_user_profl (
    myspace_id integer,
    user_hash_key smallint,
    gender smallint,
    drinker smallint,
    age_band integer,
    children smallint,
    education smallint,
    ethnicity smallint,
    maritalstatus smallint,
    smoker smallint,
    sexpref smallint,
    utype smallint,
    religion smallint,
    income smallint,
    bodytype smallint,
    dating smallint,
    networking smallint,
    culturepref smallint
) distributed by (user_hash_key);

CREATE TABLE tbl4105_sandbox_gp.curr_user_geo (
    user_id integer,
    user_hash_key smallint,
    ip_geo_sk integer,
    first_event timestamp without time zone,
    last_event timestamp without time zone,
    tot_trk_impsn bigint,
    tot_trk_click bigint,
    tot_trk_conv bigint,
    tot_trk_ut bigint,
    tot_hrs bigint,
    tot_days bigint,
    visits_last_30_days bigint,
    visits_last_60_days bigint,
    visits_last_90_days bigint
) distributed by (user_hash_key);

CREATE TABLE tbl4105_sandbox_gp.ip_geo (
    ip_geo_sk integer DEFAULT 1,
    srce_ip_geo_cntry character varying(65) NOT NULL,
    srce_ip_geo_rgn character varying(135) NOT NULL,
    srce_ip_geo_state character varying(65) NOT NULL,
    srce_ip_geo_dma integer DEFAULT 0 NOT NULL,
    srce_ip_geo_city character varying(100) NOT NULL,
    srce_ip_geo_zip character varying(30) NOT NULL,
    srce_ip_geo_time_zone character varying(5) NOT NULL,
    ip_geo_cntry character varying(65) NOT NULL,
    ip_geo_rgn character varying(135) NOT NULL,
    ip_geo_state character varying(65) NOT NULL,
    ip_geo_dma integer DEFAULT 0 NOT NULL,
    ip_geo_dma_nm character varying(65) NOT NULL,
    ip_geo_city character varying(100) NOT NULL,
    ip_geo_zip character varying(30) NOT NULL,
    ip_geo_time_zone character varying(5) NOT NULL,
    create_dttm timestamp without time zone NOT NULL,
    ip_geo_cntry_abbr character varying
) distributed randomly;

-- sql
   --     CREATE TABLE med.footest as 
        SELECT GROUPING( cust_cmunty_user_id, IS_FRIEND
                        ,ip_geo_cntry, ip_geo_cntry_abbr, ip_geo_state, ip_geo_city
                        ,GENDER,DRINKER,AGE_BAND,CHILDREN,EDUCATION,ETHNICITY,MARITALSTATUS,SMOKER,SEXPREF,UTYPE,RELIGION,INCOME,BODYTYPE,DATING,NETWORKING,CULTUREPREF)::INT as group_ind
              ,cust_cmunty_user_id 
              ,is_friend
              ,ip_geo_cntry
              ,ip_geo_cntry_abbr
              ,ip_geo_state
              ,ip_geo_city
              ,gender
              ,drinker
              ,age_band
              ,children
              ,education
              ,ethnicity
              ,maritalstatus
              ,smoker
              ,sexpref
              ,utype
              ,religion 
              ,income
              ,bodytype
              ,dating
              ,networking
              ,culturepref
              ,SUM( page_views )::BIGINT as page_views
              ,SUM( visit )::BIGINT as visitors 
              ,count(*)::BIGINT as uniques      
        FROM (
        SELECT f.cust_cmunty_user_id 
              ,f.is_friend
              ,g.ip_geo_cntry
              ,g.ip_geo_cntry_abbr
              ,g.ip_geo_state
              ,g.ip_geo_city
              ,coalesce(up.gender,-99) as gender
              ,coalesce(up.drinker,-99) as drinker
              ,coalesce(up.age_band,-99) as age_band
              ,coalesce(up.CHILDREN,-99) as children
              ,coalesce(up.EDUCATION,-99) as education
              ,coalesce(up.ETHNICITY,-99) as ethnicity
              ,coalesce(up.MARITALSTATUS,-99) as maritalstatus
              ,coalesce(up.SMOKER,-99) as smoker
              ,coalesce(up.SEXPREF,-99) as sexpref
              ,coalesce(up.UTYPE,-99) as utype
              ,coalesce(up.RELIGION,-99) as religion 
              ,coalesce(up.INCOME,-99) as income
              ,coalesce(up.BODYTYPE,-99) as bodytype
              ,coalesce(up.DATING,-99) as dating
              ,coalesce(up.NETWORKING,-99) as networking
              ,coalesce(up.CULTUREPREF,-99) as culturepref
              ,f.page_views
              ,CASE WHEN COALESCE( page_views,0 ) > 0 THEN 1 ELSE 0 END as visit 
        FROM tbl4105_sdc_stage.curr_cust_cmunty_membr f 
             LEFT OUTER JOIN tbl4105_sandbox_gp.curr_user_profl up
              ON ( f.user_hash_key = up.user_hash_key 
             AND   f.user_friend_id = up.myspace_id )
             LEFT OUTER JOIN ( SELECT user_hash_key
                                  , user_id
                                  , geo.ip_geo_cntry
                                  , geo.ip_geo_cntry_abbr
                                  , geo.ip_geo_state
                                  , geo.ip_geo_city
                              FROM tbl4105_sandbox_gp.curr_user_geo 
                                   LEFT OUTER JOIN tbl4105_sandbox_gp.ip_geo geo
                                   USING ( ip_geo_sk ) 
                              ) g
              ON ( f.user_hash_key = g.user_hash_key 
             AND   f.user_friend_id = g.user_id )
        ) foo
        GROUP BY cust_cmunty_user_id,
                 GROUPING SETS ( ( is_friend ,ip_geo_cntry, ip_geo_cntry_abbr, ip_geo_state, ip_geo_city )
                               , ( is_friend ,gender )
                               , ( is_friend ,drinker )
                               , ( is_friend ,age_band )
                               , ( is_friend ,children )
                               , ( is_friend ,education )
                               , ( is_friend ,ethnicity )
                               , ( is_friend ,maritalstatus )
                               , ( is_friend ,smoker )
                               , ( is_friend ,sexpref )
                               , ( is_friend ,utype )
                               , ( is_friend ,religion )
                               , ( is_friend ,income )
                               , ( is_friend ,bodytype )
                               , ( is_friend ,dating )
                               , ( is_friend ,networking )
                               , ( is_friend ,culturepref ) );
   --     DISTRIBUTED RANDOMLY;

-- drop the tables
drop table tbl4105_sdc_stage.curr_cust_cmunty_membr;
drop table tbl4105_sandbox_gp.curr_user_profl;
drop table tbl4105_sandbox_gp.curr_user_geo;
drop table tbl4105_sandbox_gp.ip_geo;

-- drop the schemas
drop schema tbl4105_sdc_stage;
drop schema tbl4105_sandbox_gp;

set enable_nestloop=on;

CREATE TABLE qp_misc_jiras.satelliteupdatelog (
    id integer NOT NULL,
    "type" character varying NOT NULL,
    "action" character varying NOT NULL,
    scheduled timestamp without time zone NOT NULL,
    idrep integer,
    idaffiliate integer,
    idadvertiser integer,
    CONSTRAINT satelliteupdatelog_id_xor CHECK ((((((idrep IS NOT NULL) AND (idaffiliate IS NULL)) AND (idadvertiser IS NULL)) OR (((idrep IS NULL) AND (idaffiliate IS NOT NULL)) AND (idadvertiser IS NULL))) OR (((idrep IS NULL) AND (idaffiliate IS NULL)) AND (idadvertiser IS NOT NULL))))
);


CREATE SEQUENCE qp_misc_jiras.satelliteupdatelog_id_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

ALTER SEQUENCE qp_misc_jiras.satelliteupdatelog_id_seq OWNED BY qp_misc_jiras.satelliteupdatelog.id;

ALTER TABLE qp_misc_jiras.satelliteupdatelog ALTER COLUMN id SET DEFAULT nextval('qp_misc_jiras.satelliteupdatelog_id_seq'::regclass);
ALTER TABLE ONLY qp_misc_jiras.satelliteupdatelog ADD CONSTRAINT satelliteupdatelog_pk PRIMARY KEY (id);
CREATE INDEX fki_satelliteupdatelog_idadvertiser_fk ON qp_misc_jiras.satelliteupdatelog USING btree (idadvertiser);
CREATE INDEX fki_satelliteupdatelog_idaffiliate_fk ON qp_misc_jiras.satelliteupdatelog USING btree (idaffiliate);
CREATE INDEX fki_satelliteupdatelog_idrep_fk ON qp_misc_jiras.satelliteupdatelog USING btree (idrep);

CREATE TABLE qp_misc_jiras.satelliteupdatelogkey (
    idsatelliteupdatelog integer NOT NULL,
    columnname character varying NOT NULL,
    value character varying
);

ALTER TABLE ONLY qp_misc_jiras.satelliteupdatelogkey ADD CONSTRAINT satelliteupdatelogkey_pk PRIMARY KEY (idsatelliteupdatelog, columnname);
ALTER TABLE ONLY qp_misc_jiras.satelliteupdatelogkey ADD CONSTRAINT satelliteupdatelogkey_idsatelliteupdatelog_fk FOREIGN KEY (idsatelliteupdatelog) REFERENCES qp_misc_jiras.satelliteupdatelog(id);
CREATE TABLE qp_misc_jiras.satellite (
    id integer NOT NULL,
    name character varying NOT NULL,
    "domain" character varying NOT NULL,
    "index" character varying NOT NULL,
    enabled boolean NOT NULL,
    isnewsatellite boolean NOT NULL
);


CREATE SEQUENCE qp_misc_jiras.satellite_id_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

ALTER SEQUENCE qp_misc_jiras.satellite_id_seq OWNED BY qp_misc_jiras.satellite.id;

ALTER TABLE qp_misc_jiras.satellite ALTER COLUMN id SET DEFAULT nextval('qp_misc_jiras.satellite_id_seq'::regclass);


CREATE TABLE qp_misc_jiras.satelliteupdatelogserver (
    idsatelliteupdatelog integer NOT NULL,
    idsatellite integer NOT NULL,
    retrys integer NOT NULL,
    completed timestamp without time zone,
    failurereason character varying
);

ALTER TABLE ONLY qp_misc_jiras.satelliteupdatelogserver ADD CONSTRAINT satelliteupdatelogserver_pk PRIMARY KEY (idsatelliteupdatelog, idsatellite); 
CREATE INDEX fki_satelliteupdatelogserver_idsatelliteupdatelog_fk ON qp_misc_jiras.satelliteupdatelogserver USING btree (idsatelliteupdatelog);
ALTER TABLE ONLY qp_misc_jiras.satelliteupdatelogserver ADD CONSTRAINT satelliteupdatelogserver_idsatellite_fk FOREIGN KEY (idsatellite) REFERENCES qp_misc_jiras.satellite(id);
ALTER TABLE ONLY qp_misc_jiras.satelliteupdatelogserver ADD CONSTRAINT satelliteupdatelogserver_idsatelliteupdatelog_fk FOREIGN KEY (idsatelliteupdatelog) REFERENCES qp_misc_jiras.satelliteupdatelog(id);

SELECT /* gptest */ s.id, s.action, s.type, sk.columnName AS "columnName", sk.value
FROM qp_misc_jiras.satelliteUpdateLog AS s
LEFT JOIN qp_misc_jiras.satelliteUpdateLogKey AS sk ON s.id = sk.idSatelliteUpdateLog
JOIN qp_misc_jiras.satelliteUpdateLogServer AS ss ON s.id = ss.idSatelliteUpdateLog
JOIN qp_misc_jiras.satellite AS sat ON sat.id = ss.idSatellite
WHERE ss.completed IS NULL
AND ss.retrys > 0
AND sat.enabled
GROUP BY s.id, s.action, s.type, sk.columnName, sk.value, s.scheduled
ORDER BY s.scheduled; 

drop table qp_misc_jiras.satelliteupdatelog cascade;
drop table qp_misc_jiras.satelliteupdatelogkey cascade;
drop table qp_misc_jiras.satellite cascade;
drop table qp_misc_jiras.satelliteupdatelogserver cascade;
create table qp_misc_jiras.tbl3183_t1 (i int);
insert into qp_misc_jiras.tbl3183_t1 values (1), (1);
select * into qp_misc_jiras.tbl3183_t2 from qp_misc_jiras.tbl3183_t1;
select * into qp_misc_jiras.tbl3183_t3 from qp_misc_jiras.tbl3183_t1;
select i from (select i from qp_misc_jiras.tbl3183_t2 union all select i from qp_misc_jiras.tbl3183_t3) tmpt where i in (select i from qp_misc_jiras.tbl3183_t2 union all select i from qp_misc_jiras.tbl3183_t3);
drop table qp_misc_jiras.tbl3183_t1;
drop table qp_misc_jiras.tbl3183_t2;
drop table qp_misc_jiras.tbl3183_t3;
create table qp_misc_jiras.tbl5028_part_test (a int, b date,c text) 
partition by range(b)
(
	partition p1 start('2007-01-01'),
        partition p2 start('2008-01-01'),
        partition p3 start('2009-01-01'),
	default partition def_part
);
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (1, '2006-1-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (1, '2007-1-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (1, '2008-1-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (1, '2009-1-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (2, '2006-2-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (2, '2007-2-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (2, '2008-2-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (2, '2009-2-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (3, '2006-3-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (3, '2007-3-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (3, '2008-3-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (3, '2009-3-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (4, '2006-4-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (4, '2007-4-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (4, '2008-4-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (4, '2009-4-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (5, '2006-5-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (5, '2007-5-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (5, '2008-5-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (5, '2009-5-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (6, '2006-6-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (6, '2007-6-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (6, '2008-6-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (6, '2009-6-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (7, '2006-7-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (7, '2007-7-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (7, '2008-7-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (7, '2009-7-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (8, '2006-8-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (8, '2007-8-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (8, '2008-8-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (8, '2009-8-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (9, '2006-9-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (9, '2007-9-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (9, '2008-9-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (9, '2009-9-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (10, '2006-10-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (10, '2007-10-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (10, '2008-10-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (10, '2009-10-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (11, '2006-11-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (11, '2007-11-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (11, '2008-11-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (11, '2009-11-01', 'Year 4');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (12, '2006-12-01', 'Year 1');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (12, '2007-12-01', 'Year 2');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (12, '2008-12-01', 'Year 3');
INSERT INTO qp_misc_jiras.tbl5028_part_test VALUES (12, '2009-12-01', 'Year 4');

select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_def_part ;
select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_p1;
select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_p2;
select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_p3;

DELETE FROM qp_misc_jiras.tbl5028_part_test where b >= '2007-01-01' and b <= '2007-03-01';

select count(*) from qp_misc_jiras.tbl5028_part_test; -- should be 45

select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_def_part ; -- should be 12
select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_p1; -- should be 9
select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_p2; -- should be 12
select count(*) from qp_misc_jiras.tbl5028_part_test_1_prt_p3; -- should be 12
drop table qp_misc_jiras.tbl5028_part_test;
--   MPP-11125: partition p1 start('0') end('25') every 8 (12)
--   	- it did not work as expected but behaved same as "every (12)"
--   	- "every 8 (12)" is now an invalid syntax
--   Modified: "every 8 (12)" to "every (12)"
--             added an extra "every 8 (12)" to test invalid syntax

create table qp_misc_jiras.tbl5151_customer(C_CUSTKEY INTEGER,
                C_NAME VARCHAR(25),
                C_ADDRESS VARCHAR(40),
               C_NATIONKEY INTEGER,
                C_PHONE CHAR(15),
                C_ACCTBAL decimal,
                C_MKTSEGMENT CHAR(10),
                C_COMMENT VARCHAR(117)
                )
partition by range (c_nationkey) 
subpartition by range (c_acctbal) subpartition template (start('-999.99') end('10000.99') every(2200)
)
(
partition p1 start('0') end('25') every (12)
);
create table qp_misc_jiras.tbl5151_customer(C_CUSTKEY INTEGER, primary key(c_custkey));
drop table qp_misc_jiras.tbl5151_customer;

--    invalid syntax
create table qp_misc_jiras.tbl5151_customer(C_CUSTKEY INTEGER) partition by range (c_nationkey) (start('0') end('25') every 8 (12));
create table qp_misc_jiras.tbl2945_test (a numeric(19), b int) distributed by (a);
insert into qp_misc_jiras.tbl2945_test values(1,2), (3,4);
-- the following should error out
alter table qp_misc_jiras.tbl2945_test alter a type bigint;
drop table qp_misc_jiras.tbl2945_test;
create table qp_misc_jiras.tbl5219_test (i int, j int);
insert into qp_misc_jiras.tbl5219_test select i, i%10 from generate_series(0, 99) i;
select case when 1=2 then rank() over(partition by j order by i) end from qp_misc_jiras.tbl5219_test;
drop table qp_misc_jiras.tbl5219_test;
select n
from ( select row_number() over (partition by x) from (values (0)) as t(x) ) as r(n)
group by n;

select n
from ( select row_number() over () from (values (0)) as t(x) ) as r(n)
group by n;
create external web table qp_misc_jiras.tbl4622_ext1 (i int, j int) execute 'echo " | 20" ' on master format 'text' (delimiter as E'|' null as ' ');
select * from qp_misc_jiras.tbl4622_ext1;
create external web table qp_misc_jiras.tbl4622_ext2 (i int, j int) execute 'echo "  | 20" ' on master format 'text' (delimiter as E'|' null as '  ');
select * from qp_misc_jiras.tbl4622_ext2;
drop external web table qp_misc_jiras.tbl4622_ext1;
drop external web table qp_misc_jiras.tbl4622_ext2;
create external web table qp_misc_jiras.tbl3286_ext_hostname (hostname text) execute 'hostname' format 'text'; 
-- start_ignore
select * from qp_misc_jiras.tbl3286_ext_hostname,gp_configuration;
-- end_ignore
drop external web table qp_misc_jiras.tbl3286_ext_hostname;
create table qp_misc_jiras.tbl5305_test(a int, b int);
insert into qp_misc_jiras.tbl5305_test select i,i+1 from generate_series(1,1000)i;
begin;
declare c scroll cursor for select * from qp_misc_jiras.tbl5305_test order by a;
fetch forward 100 from c; -- should give the first 100 rows
fetch 100 from c; -- should give the next 100 rows
fetch 1 from c; -- should give one row
fetch backward 100 from c; -- should error out. we currently don't support this
rollback;
drop table qp_misc_jiras.tbl5305_test;
create external web table qp_misc_jiras.tbl_3739_foo (a text) execute E'' ON SEGMENT 0 FORMAT 'CSV';
show gp_interconnect_setup_timeout;
set gp_interconnect_setup_timeout=3600;
show gp_interconnect_setup_timeout;
set gp_interconnect_setup_timeout=10000;
set gp_interconnect_setup_timeout=4000;
create schema tbl5352_test;
select tablename from pg_tables where schemaname='tbl5352_test' and tablename not in( select partitiontablename from pg_partitions where partitionschemaname = 'tbl5352_test' );
drop schema tbl5352_test;

CREATE TABLE qp_misc_jiras.tbl5223_sales_fact(
	   time     timestamp,
	   product  int, 
	   sales    numeric(12,2)
) distributed by (product);

copy qp_misc_jiras.tbl5223_sales_fact from stdin;
1-2-07	1	1.03
1-6-07	2	13.25
1-11-07	3	8.32
2-24-07	1	1.05
2-8-07	2	15.25
3-15-07	3	8.32
3-3-07	1	1.06
3-2-07	2	10.25
4-5-07	3	8.32
5-9-07	1	1.07
6-28-07	2	12.25
6-22-07	3	8.3
7-10-07	1	1.09
8-8-07	2	15.25
9-2-07	3	8.32
10-14-07	1	1.29
11-12-07	2	13.25
12-3-07	3	8.32
12-6-07	3	8.32
12-10-07	3	8.32
1-2-07	1	1.39
1-6-07	2	14.25
1-11-07	3	8.32
2-24-07	1	1.44
2-8-07	2	16.25
3-15-07	3	8.32
3-3-07	1	1.52
3-2-07	2	15.25
4-5-07	3	8.32
5-9-07	1	1.42
6-28-07	2	19.25
6-22-07	3	8.32
7-10-07	1	1.45
8-8-07	2	15.25
9-2-07	3	8.32
10-14-07	1	1.90
11-12-07	2	14.25
12-3-07	3	8.32
12-6-07	3	8.32
12-10-07	3	8.32
1-2-08	1	2.03
1-6-08	2	23.25
1-11-08	3	9.32
2-24-08	1	2.05
2-8-08	2	25.25
3-15-08	3	9.32
3-3-08	1	2.06
3-2-08	2	20.25
4-5-08	3	9.32
5-9-08	1	2.07
6-28-08	2	22.25
6-22-08	3	9.3
7-10-08	1	2.09
8-8-08	2	25.25
9-2-08	3	9.32
10-14-08	2	2.29
11-12-08	2	23.25
12-3-08	3	9.32
12-6-08	3	9.32
12-10-08	3	10.32
1-2-08	1	2.39
1-6-08	2	24.25
1-11-08	3	2.32
2-24-08	1	2.44
2-8-08	2	26.25
\.

SELECT
  product, year, sales, 
  100*(1.0 - sales/(sum(sales) over (w))) as "% sales_growth"
FROM
  (SELECT
      product,
      extract(year from time) as year, 
	  extract(month from time) as month,
      sum(sales) as sales
   FROM qp_misc_jiras.tbl5223_sales_fact 
   GROUP BY product, year, month
  ) product_yearly_sales
window w as (partition by product order by year*12+month
             range between 12 preceding and 1 preceding)
order by year, product, sales; -- mvd 1,2->4

drop table qp_misc_jiras.tbl5223_sales_fact;

CREATE VIEW qp_misc_jiras.tbl4255_simple_v as SELECT 1 as value;
CREATE VIEW qp_misc_jiras.tbl4255_union_v as SELECT 1 as value UNION ALL SELECT 2;

SELECT * FROM qp_misc_jiras.tbl4255_simple_v;
SELECT generate_series(1,3), * from qp_misc_jiras.tbl4255_simple_v;
SELECT * from qp_misc_jiras.tbl4255_union_v;
SELECT generate_series(1,3), * from qp_misc_jiras.tbl4255_union_v;

drop view qp_misc_jiras.tbl4255_simple_v;
drop view qp_misc_jiras.tbl4255_union_v;

create table qp_misc_jiras.tbl5246_sale
(       
        cn int not null,
        vn int not null,
        pn int not null,
        dt date not null,
        qty int not null,
        prc float not null,
        
        primary key (cn, vn, pn)
        
) distributed by (cn,vn,pn);

insert into qp_misc_jiras.tbl5246_sale values
  ( 2, 40, 100, '1401-1-1', 1100, 2400),
  ( 1, 10, 200, '1401-3-1', 1, 0),
  ( 3, 40, 200, '1401-4-1', 1, 0),
  ( 1, 20, 100, '1401-5-1', 1, 0),
  ( 1, 30, 300, '1401-5-2', 1, 0),
  ( 1, 50, 400, '1401-6-1', 1, 0),
  ( 2, 50, 400, '1401-6-1', 1, 0),
  ( 1, 30, 500, '1401-6-1', 12, 5),
  ( 3, 30, 500, '1401-6-1', 12, 5),
  ( 3, 30, 600, '1401-6-1', 12, 5),
  ( 4, 40, 700, '1401-6-1', 1, 1),
  ( 4, 40, 800, '1401-6-1', 1, 1);

explain select cn, count(*) over (order by dt range between '2 day'::interval preceding and 2 preceding) from qp_misc_jiras.tbl5246_sale;

drop table qp_misc_jiras.tbl5246_sale;

create table qp_misc_jiras.tbl4896_customer
(
        cn int not null,
        cname text not null,
        cloc text,

        primary key (cn)

) distributed by (cn);

create table qp_misc_jiras.tbl4896_sale
(
        cn int not null,
        vn int not null,
        pn int not null,
        dt date not null,
        qty int not null,
        prc float not null,
        
        primary key (cn, vn, pn)
        
) distributed by (cn,vn,pn);

insert into qp_misc_jiras.tbl4896_customer values
  ( 1, 'Macbeth', 'Inverness'),
  ( 2, 'Duncan', 'Forres'),
  ( 3, 'Lady Macbeth', 'Inverness'),
  ( 4, 'Witches, Inc', 'Lonely Heath');

insert into qp_misc_jiras.tbl4896_sale values
  ( 2, 40, 100, '1401-1-1', 1100, 2400),
  ( 1, 10, 200, '1401-3-1', 1, 0),
  ( 3, 40, 200, '1401-4-1', 1, 0),
  ( 1, 20, 100, '1401-5-1', 1, 0),
  ( 1, 30, 300, '1401-5-2', 1, 0),
  ( 1, 50, 400, '1401-6-1', 1, 0),
  ( 2, 50, 400, '1401-6-1', 1, 0),
  ( 1, 30, 500, '1401-6-1', 12, 5),
  ( 3, 30, 500, '1401-6-1', 12, 5),
  ( 3, 30, 600, '1401-6-1', 12, 5),
  ( 4, 40, 700, '1401-6-1', 1, 1),
  ( 4, 40, 800, '1401-6-1', 1, 1);

SELECT sale.cn*2 as newalias1,
CASE WHEN sale.vn < 10 THEN 1 ELSE 2 END as newalias2,
sale.cn + sale.cn as newalias3, 
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.vn/sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(VARIANCE(DISTINCT floor(sale.vn/sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(AVG(DISTINCT floor(sale.vn+sale.pn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(SUM(DISTINCT floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(DISTINCT floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(MAX(DISTINCT floor(sale.cn)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(DISTINCT floor(sale.qty)),0),'99999999.9999999'),
TO_CHAR(COALESCE(COUNT(DISTINCT floor(sale.cn)),0),'99999999.9999999')
FROM qp_misc_jiras.tbl4896_sale as sale ,qp_misc_jiras.tbl4896_customer as customer
WHERE sale.cn=customer.cn
GROUP BY CUBE((sale.dt),(newalias1,newalias2,newalias1),(sale.cn,sale.cn,sale.cn,newalias1),
(sale.qty),(sale.pn,newalias3,sale.vn),(sale.vn,sale.vn,sale.prc),(sale.cn,newalias2)),sale.cn,sale.vn;

drop table qp_misc_jiras.tbl4896_sale;
drop table qp_misc_jiras.tbl4896_customer;

create table qp_misc_jiras.tbl4703_test (i int, j int);
insert into qp_misc_jiras.tbl4703_test select i, i%10 from generate_series(0, 9999) i;
create index test_j on qp_misc_jiras.tbl4703_test(j);
set gp_select_invisible=true;
set enable_seqscan=off;
set enable_indexscan=off;
select count(*) from qp_misc_jiras.tbl4703_test where j = 5 or j = 10; --shouldn't crash
drop table qp_misc_jiras.tbl4703_test;
create table qp_misc_jiras.tbl_694_1(c1 int, b1 box);
select * from qp_misc_jiras.tbl_694_1;
create table qp_misc_jiras.tbl_694_2 (like qp_misc_jiras.tbl_694_1);

select * from qp_misc_jiras.tbl_694_2;
insert into qp_misc_jiras.tbl_694_2 values(2,'(2,2),(2,2)');
insert into qp_misc_jiras.tbl_694_2 values(2,'(2,2),(3,2)');
insert into qp_misc_jiras.tbl_694_2 values(2,'(2,2),(3,3)');
insert into qp_misc_jiras.tbl_694_1 select * from qp_misc_jiras.tbl_694_2;
select * from qp_misc_jiras.tbl_694_2;
select * from qp_misc_jiras.tbl_694_2 join qp_misc_jiras.tbl_694_1 on qp_misc_jiras.tbl_694_2.b1=qp_misc_jiras.tbl_694_1.b1;
select * from qp_misc_jiras.tbl_694_1 join qp_misc_jiras.tbl_694_2 on qp_misc_jiras.tbl_694_1.b1=qp_misc_jiras.tbl_694_2.b1;

drop table qp_misc_jiras.tbl_694_1;
drop table qp_misc_jiras.tbl_694_2;

select * from ( select 'a' as a) x join (select 'a' as b) y on a=b;
select * from ( ( select 'a' as a ) xx join (select 'a' as b) yy on a = b ) x join (select 'a' as c) y on a=c;
select x.b from ( ( select 'a' as a ) xx join (select 'a' as b) yy on a = b ) x join (select 'a' as c) y on a=c;
create table qp_misc_jiras.tbl6027_test (i int, j bigint, k int, l int, m int);
insert into qp_misc_jiras.tbl6027_test select i, i%100, i%123, i%234, i%345 from generate_series(1, 500) i;
select j, sum(k), row_number() over (partition by j order by sum(k)) from qp_misc_jiras.tbl6027_test group by j order by j limit 10; -- order 1
CREATE TABLE qp_misc_jiras.tbl6419_test(
lstg_id numeric(38,0),
icedt date,
icehr smallint,
cre_time timestamp without time zone,
lstg_desc_txt text
)
WITH (appendonly=true, compresslevel=5) distributed by (lstg_id) PARTITION BY RANGE(cre_time)
(
START ('2009-05-26 00:00:00'::timestamp without time zone) 
END ('2009-06-05 00:00:00'::timestamp without time zone) EVERY ('1 day'::interval) WITH (appendonly=true, compresslevel=5)
);

insert into qp_misc_jiras.tbl6419_test values( 123, '2009-06-01', 12, '2009-06-01 01:01:01', 'aaaaaa');
select * from qp_misc_jiras.tbl6419_test where icedt::text = (select partitionrangestart FROM pg_partitions where tablename='test1' and schemaname='public' and partitionrank=1);
select * from qp_misc_jiras.tbl6419_test where '2009-12-12'::date::text = (select 'test'::text);
drop table qp_misc_jiras.tbl6419_test;

CREATE TABLE qp_misc_jiras.m_ccr_mthy_cr_nds_t00
(
cls_yymm date,
plyno character(16),
cr_yymm character(6),
cnrdt date,
ins_itm_smccd character(10),
ins_imcd character(10),
cr_udrtk_grdcd character(10),
nrdps_ctm_dscno character(20),
dh_stfno character(7),
ce_br_orgcd character(7),
ce_bzp_orgcd character(7),
ce_stfno character(7),
carno_hngl character(50),
nrdps_adr_sd character(30),
nrdps_adr_sgng character(30),
nrdps_adr_twmd character(30),
udrtk_gu_arecd character(10)
);
CREATE TABLE qp_misc_jiras.m_ccr_cvr_nds_t99
(
cls_yymm date,
plyno character(16),
ndsno character(4),
nds_dt_seqno numeric(4),
nds_ap_dthms timestamp(0) without time zone,
nds_ap_dt date,
cr_yymm date,
cnrdt date,
ins_itm_smccd character(10),
ins_imcd character(10),
dh_stfno character(7),
ce_br_orgcd character(7),
ce_bzp_orgcd character(7),
ce_stfno character(7),
chbvl character varying(500),
chavl character varying(500),
nds_dt_itnm character varying(500),
nds_mgb character(3),
nds_lgb character(2)
)
WITH (OIDS=FALSE);

CREATE TABLE qp_misc_jiras.ins_cr_nds_dt
(
plyno character varying(16) NOT NULL, 
ndsno character(4) NOT NULL, 
nds_dt_seqno numeric(5) NOT NULL, 
atrcd character(7) NOT NULL, 
chbvl character varying(500), 
chavl character varying(500), 
nds_dt_itnm character varying(500) NOT NULL, 
nds_note character varying(500), 
scr_idc_gp numeric(5), 
oj_or_relpc_flgcd character(10), 
ppr_seqno character(10), 
sbd_seqno character(10), 
dt_seqno character varying(20), 
chbf_cd character(10), 
chaf_cd character(10), 
bj_dscno character varying(13), 
bjnm character varying(100), 
dlflg_cd character(10), 
nds_tpcd character(10), 
ndscd character(10), 
da_ch_yn character(1), 
it_out_yn character(1), 
inp_usr_id character(7) NOT NULL, 
inp_dthms timestamp(0) without time zone NOT NULL, 
mdf_usr_id character(7) NOT NULL, 
mdf_dthms timestamp(0) without time zone NOT NULL, 
load_dthms timestamp(0) without time zone 
)
WITH (OIDS=FALSE);

CREATE TABLE qp_misc_jiras.ins_cr_nds_mstr
(
plyno character varying(16) NOT NULL, 
ndsno character(4) NOT NULL, 
ikd_grpcd character(10) NOT NULL, 
ch_rvsn_flgcd character(10), 
nds_cas_flgcd character(10), 
nds_wrdg character varying(2000), 
nds_prgcd character(10), 
prort_shtm_flgcd character(10), 
prort_cal_days numeric(5), 
prort_shtrt numeric(12,6) NOT NULL, 
nds_rqdt date, 
nds_ap_dthms timestamp(0) without time zone, 
otcm_mg_apdt date, 
nds_cr_flgcd character(10), 
nds_stdt date, 
nds_st_hms character(4), 
nds_nddt date, 
rqfno character(7), 
ap_stfno character(7), 
nds_acnmt_isdt date, 
nds_sno numeric(10), 
nds_ap_br_orgcd character(7), 
retr_nds_yn character(1), 
frc_nds_yn character(1), 
inp_trm_ip character varying(30), 
bsc_ndsno character(4), 
amt_cr_yn character(1), 
nds_plno character varying(16), 
nds_cgaf_ch_seqno numeric(5), 
retr_ndsof_ndsno character(4), 
whpy_mtdcd character(10), 
nds_rq_spc character varying(500), 
nds_rq_rs character varying(500), 
nds_ccmtd character varying(1000), 
inp_usr_id character(7) NOT NULL, 
inp_dthms timestamp(0) without time zone NOT NULL, 
mdf_usr_id character(7) NOT NULL, 
mdf_dthms timestamp(0) without time zone NOT NULL, 
frc_nds_cas_flgcd character(10), 
load_dthms timestamp(0) without time zone 
)
WITH (OIDS=FALSE);



SELECT CLS_YYMM
,PLYNO
,NDSNO 
,NDS_DT_SEQNO 
,NDS_AP_DTHMS
,NDS_AP_DTHMS ::DATE AS NDS_AP_DT
,CR_YYMM 
,CNRDT 
,INS_ITM_SMCCD 
,INS_IMCD 
,DH_STFNO 
,CE_BR_ORGCD 
,CE_BZP_ORGCD 
,CE_STFNO 
,CHBVL 
,CHAVL 
,NDS_DT_ITNM 
,NDS_MGB
,NDS_LGB 
FROM 
( 
SELECT CLS_YYMM
,PLYNO
,NDSNO 
,NDS_DT_SEQNO 
,NDS_AP_DTHMS
,NDS_AP_DTHMS ::DATE AS NDS_AP_DT
,CR_YYMM 
,CNRDT 
,INS_ITM_SMCCD 
,INS_IMCD 
,DH_STFNO 
,CE_BR_ORGCD 
,CE_BZP_ORGCD 
,CE_STFNO 
,CHBVL 
,CHAVL 
,NDS_DT_ITNM 
,NDS_MGB
,NDS_LGB 
,RANK() OVER(PARTITION BY CLS_YYMM,PLYNO,NDSNO,NDS_MGB ORDER BY NDS_DT_SEQNO ASC) AS SQ
FROM
( 
SELECT S1.CLS_YYMM
,A1.PLYNO
,A1.NDSNO 
,A1.NDS_DT_SEQNO 
,A2.NDS_AP_DTHMS
,CAST(A2.NDS_AP_DTHMS AS DATE) NDS_AP_DT
,S1.CR_YYMM 
,S1.CNRDT 
,S1.INS_ITM_SMCCD 
,S1.INS_IMCD 
,S1.DH_STFNO 
,S1.CE_BR_ORGCD 
,S1.CE_BZP_ORGCD 
,S1.CE_STFNO 
,A1.CHBVL 
,A1.CHAVL 
,A1.NDS_DT_ITNM 
,CASE WHEN NDS_DT_ITNM IN ('긴급출동') THEN '201'
WHEN NDS_DT_ITNM IN ('대물2','대물배상가입금액','대물배상 가입금액') THEN '202'
WHEN NDS_DT_ITNM IN ('대인2','대인2 가입금액','대인배상가입금액') THEN '203'
WHEN NDS_DT_ITNM IN ('무보험(상해)','상해') THEN '204'
WHEN NDS_DT_ITNM IN ('자상','자상 가입금액','자손','자손가입금액','자기신체가입금액','자손확장특약가입금액') THEN '205'
WHEN NDS_DT_ITNM IN ('자차','자기차량가입금액','자기부담금액','자차 가입금액','자차 공제금액') THEN '206'
WHEN NDS_DT_ITNM IN ('기계 가입금액','기계장치가입금액') THEN '207'
ELSE '299' END NDS_MGB
,'2' NDS_LGB 
FROM qp_misc_jiras.M_CCR_MTHY_CR_NDS_T00 S1 /******공통사용예정********/
INNER JOIN qp_misc_jiras.INS_CR_NDS_DT A1
ON S1.PLYNO=A1.PLYNO
LEFT OUTER JOIN qp_misc_jiras.INS_CR_NDS_MSTR A2
ON A1.PLYNO = A2.PLYNO
AND A1.NDSNO = A2.NDSNO 
) T1
WHERE NDS_DT_ITNM LIKE ('%가입금액%')
AND (T1.PLYNO, T1.NDSNO, SUBSTR(T1.NDS_MGB,2,2)) NOT IN (
SELECT PLYNO
,NDSNO
,SUBSTR(NDS_MGB,2,2) AS NDS_MGB
FROM qp_misc_jiras.M_CCR_CVR_NDS_T99
) 
) T
WHERE T.SQ = 1 
;

drop table qp_misc_jiras.ins_cr_nds_dt, qp_misc_jiras.ins_cr_nds_mstr, qp_misc_jiras.m_ccr_cvr_nds_t99, qp_misc_jiras.m_ccr_mthy_cr_nds_t00;

set gp_select_invisible=false;

create table qp_misc_jiras.tbl6448 (x "char");
insert into qp_misc_jiras.tbl6448 values ('a');
select x from qp_misc_jiras.tbl6448;
drop table qp_misc_jiras.tbl6448;
create table qp_misc_jiras.tbl6833_anl(a int, b int, c int) distributed by (a) partition by range (b) (partition bb start (0) end (2) every (1));
analyze qp_misc_jiras.tbl6833_anl;
explain select * from qp_misc_jiras.tbl6833_anl; -- should not hit cdbRelSize();
select relname, reltuples, relpages from pg_class where relname like 'tbl6833_anl%'; -- should show relpages = 1.0

create table qp_misc_jiras.tbl6833_vac(a int, b int, c int) distributed by (a) partition by range (b) (partition bb start (0) end (2) every (1));
vacuum qp_misc_jiras.tbl6833_vac;
explain select * from qp_misc_jiras.tbl6833_vac; -- should not hit cdbRelSize();
select relname, reltuples, relpages from pg_class where relname like 'tbl6833_vac%'; -- should show relpages = 1.0

drop table qp_misc_jiras.tbl6833_anl;
drop table qp_misc_jiras.tbl6833_vac;
-- actual query

drop table if exists qp_misc_jiras_foo;
create table qp_misc_jiras_foo(x int) distributed by (x);

SELECT t.distkey, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
a.attnotnull, a.attnum, pg_catalog.col_description(a.attrelid, a.attnum)
FROM pg_catalog.pg_class c
LEFT outer JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
left outer join pg_catalog.gp_distribution_policy t on localoid = c.oid
left outer join pg_catalog.pg_attribute a on a.attrelid = c.oid and a.attnum in (select unnest(t.distkey))
WHERE c.relname ~ '^(qp_misc_jiras_foo)$';


-- previously supported query

create table qp_misc_jiras_bar (t int, d int, g int);

insert into qp_misc_jiras_bar values(1,2,3);
insert into qp_misc_jiras_bar values(4,5,6);

select a.t from qp_misc_jiras_bar a where d in(select d from qp_misc_jiras_bar b where a.g=b.g) order by a.t;
-- Various AO/CO util functions

-- ctas from one storage type to another
create table qp_misc_jiras.tbl7126_ao (a int, b text, c date) with (appendonly=true);
insert into qp_misc_jiras.tbl7126_ao select i, 'abcd'||i, '1981-10-02' from generate_series(1,100000)i;
create table qp_misc_jiras.tbl7126_ao_zlib3 with (appendonly=true, compresstype = zlib, compresslevel=3 ) as select * from qp_misc_jiras.tbl7126_ao;
create table qp_misc_jiras.tbl7126_co with (appendonly=true,orientation=column) as select * from qp_misc_jiras.tbl7126_ao_zlib3;
create table qp_misc_jiras.tbl7126_co_zlib3  with (appendonly=true,orientation=column, compresstype=zlib, compresslevel=3) as select * from qp_misc_jiras.tbl7126_co;

-- selects
select count(*) from qp_misc_jiras.tbl7126_ao;
select count(*) from qp_misc_jiras.tbl7126_ao_zlib3;
select count(*) from qp_misc_jiras.tbl7126_co;
select count(*) from qp_misc_jiras.tbl7126_co_zlib3;

-- util functions

-- start_ignore

select get_ao_compression_ratio('qp_misc_jiras.tbl7126_ao');
select get_ao_compression_ratio('qp_misc_jiras.tbl7126_ao_zlib3');
select get_ao_compression_ratio('qp_misc_jiras.tbl7126_co');
select get_ao_compression_ratio('qp_misc_jiras.tbl7126_co_zlib3');


select get_ao_distribution('qp_misc_jiras.tbl7126_ao');
select get_ao_distribution('qp_misc_jiras.tbl7126_ao_zlib3');
select get_ao_distribution('qp_misc_jiras.tbl7126_co');
select get_ao_distribution('qp_misc_jiras.tbl7126_co_zlib3');

-- end_ignore

select gp_update_ao_master_stats('qp_misc_jiras.tbl7126_ao');
select gp_update_ao_master_stats('qp_misc_jiras.tbl7126_ao_zlib3');
select gp_update_ao_master_stats('qp_misc_jiras.tbl7126_co');
select gp_update_ao_master_stats('qp_misc_jiras.tbl7126_co_zlib3');



-- drop the objects created
drop table qp_misc_jiras.tbl7126_ao;
drop table qp_misc_jiras.tbl7126_ao_zlib3;
drop table qp_misc_jiras.tbl7126_co;
drop table qp_misc_jiras.tbl7126_co_zlib3;

create table pre_visitor_ca_event_ao(
visitor_timestamp timestamp without time zone,
visitor_key varchar,
visitor_domain varchar,
visitor_firstvisited timestamp without time zone,
visitor_lastvisited timestamp without time zone,
visitor_useragent timestamp without time zone,
visitor_referer timestamp without time zone,
visitor_camifymemberid numeric,
visitor_language varchar,
visitor_labels varchar,
visitor_city varchar,
visitor_country varchar,
visitor_countrycode varchar,
visitor_ipaddress varchar,
visitor_latitude real,
visitor_longitude real,
visitor_organization varchar,
visitor_browser varchar,
visitor_browserversion varchar,
visitor_os varchar,
visitor_osversion varchar,
visitor_age numeric,
visitor_alias varchar,
visitor_avatar varchar,
visitor_profileurl varchar,
visitor_gender varchar,
contentaccess_accesseddate timestamp without time zone,
contentaccess_contentkey varchar,
contentaccess_domain varchar,
contentaccess_exiteddate timestamp without time zone,
contentaccess_status varchar,
contentaccess_url varchar,
contentaccess_contenttype varchar,
event_date timestamp without time zone,
event_name varchar,
event_duration numeric,
contentaccesslabels_name varchar,
contentaccesslabels_value varchar,
contentaccesslabels_date timestamp without time zone,
event_description varchar,
visitor_state varchar,
contentaccess_applicationid varchar) with (appendonly=true); 

copy pre_visitor_ca_event_ao from STDIN with delimiter '~' null '';
~63746267502688530790~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 13:48:18~-814737572~admin.brightcove.com~2009-09-25 08:55:35~~http://admin.brightcove.com/viewer/federated/sony_05.swf?bn=577&pubId=59121~Video~~~~~~~~~fsc
2009-09-25 01:55:35~63715756233187257420~admin.brightcove.com~2009-09-21 13:31:46~2009-09-21 13:31:49~~~0~unknown~~East Aurora~United States~US~199.89.199.82~42.768~-78.5801~Mattel~~~~~~~~~~~~~~~~~~~~~~~~NY~
2009-09-25 01:55:35~81048700113193828880~admin.brightcove.com~2009-09-21 18:40:10~2009-09-21 18:40:15~~~0~unknown~~Berkeley~United States~US~24.130.240.245~37.8781~-122.271~Comcast Cable~~~~~~~~~~~~~~~~~~~~~~~~CA~
~63690433578412488390~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 13:50:07~-1618417882~admin.brightcove.com~2009-09-25 08:55:35~~http://admin.brightcove.com/viewer/federated/e_001.swf?bn=219&pubId=136368194~Video~~~~~~~~~fsc
~62826206434849800180~~~~~~~~~~~~~~~~~~~~~~~~~~-814737572~~~~~~2009-09-21 13:42:08~seek~0~~~~~~
~84558474275891664420~~~~~~~~~~~~~~~~~~~~~~~~~~1528666849~~~~~~2009-09-21 18:24:28~unpause~0~~~~~~
2009-09-25 01:55:35~62907625998851997230~admin.brightcove.com~2009-09-21 13:44:32~2009-09-21 13:48:45~~~0~unknown~~Spring Valley~United States~US~209.66.200.20~32.716~-116.97~San Diego County Office of Education~~~~~~~~~~~~~~~~~~~~~~~~CA~
~65170533035987666600~~~~~~~~~~~~~~~~~~~~~~~~~~-517444718~~~~~~2009-09-21 18:14:33~unpause~0~~~~~~
~83143281530960791700~~~~~~~~~~~~~~~~~~~~~~~~~~-517444718~~~~~~2009-09-21 18:54:24~seek~0~~~~~~
2009-09-25 01:55:45~60107359119154350810~admin.brightcove.com~2009-09-21 13:29:05~2009-09-21 13:30:25~~~0~unknown~~Beijing~China~CN~58.209.96.50~39.9289~116.388~CHINANET jiangsu province network~~~~~~~~~~~~~~~~~~~~~~~~22~
2009-09-25 01:55:45~58708536294795805290~admin.brightcove.com~2009-09-21 13:55:00~2009-09-21 13:55:26~~~0~unknown~~Shenzhen~China~CN~121.37.12.232~22.5333~114.133~China Motion Network Communication~~~~~~~~~~~~~~~~~~~~~~~~30~
~70825484458163850770~~~~~~~~~~~~~~~~~~~~~~~~~~1379350152~~~~~~2009-09-21 13:17:24~stop~0~~~~~~
~71591645244668946410~~~~~~~~~~~~~~~~~~~~~~~~~~-818021851~~~~~~2009-09-21 00:18:14~pause~0~~~~~~
~62203940189161012290~~~~~~~~~~~~~~~~~~~~~~~~~~1442874516~~~~~~2009-09-21 00:38:42~seek~0~~~~~~
~62269140292331912800~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 13:01:22~-1371414842~admin.brightcove.com~2009-09-25 08:55:45~~http://admin.brightcove.com/viewer/us1.21.02.03/federatedVideoUI/BrightcovePlayer.swf?isUI=true&isVid=true&purl=http://www.time.com/time/video/player/0,32068,14218770001_0,00.html&@videoPlayer=14218770001&autoStart=&bgcolor=%23000000&flashID=myExperience&height=396&playerID=29323562001&publisherID=293884104&width=629~Video~~~~~~~~~fsc
~64270272754251385030~~~~~~~~~~~~~~~~~~~~~~~~~~817101695~~~~~~2009-09-21 00:39:27~play~0~~~~~~
2009-09-25 01:55:45~61396129686948642150~admin.brightcove.com~2009-09-21 00:22:15~2009-09-21 00:26:49~~~0~unknown~~Brooklyn~United States~US~24.185.95.217~40.6702~-73.8944~Optimum Online (Cablevision Systems)~~~~~~~~~~~~~~~~~~~~~~~~NY~
~70178323593675692640~~~~~~~~~~~~~~~~~~~~~~~~~~-628491556~~~~~~2009-09-21 13:05:26~stop~0~~~~~~
~72905398911246894820~~~~~~~~~~~~~~~~~~~~~~~~~~1191578597~~~~~~2009-09-21 17:59:42~unpause~0~~~~~~
~70223239937170704020~~~~~~~~~~~~~~~~~~~~~~~~~~1623020058~~~~~~2009-09-21 14:48:21~stop~0~~~~~~
~637046513263066530101~~~~~~~~~~~~~~~~~~~~~~~~~~-2141059678~~~~~~2009-09-21 13:39:28~pause~0~~~~~~
~744738197783356862101~~~~~~~~~~~~~~~~~~~~~~~~~~-1494935159~~~~~~2009-09-21 13:12:17~stop~0~~~~~~
~745326634854569224501~~~~~~~~~~~~~~~~~~~~~~~~~~-814737572~~~~~~2009-09-21 13:24:21~pause~0~~~~~~
~743803819917020120701~~~~~~~~~~~~~~~~~~~~~~~~~~1890603928~~~~~~2009-09-21 18:53:22~stop~0~~~~~~
~802352828699123464101~~~~~~~~~~~~~~~~~~~~~~~~~~2020750008~~~~~~2009-09-21 18:42:04~play~0~~~~~~
~586659425014882440501~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 13:43:51~-1101392348~admin.brightcove.com~2009-09-25 08:55:35~~http://admin.brightcove.com/viewer/federated/f_013.swf?bn=355&pubId=429049095~Video~~~~~~~~~fsc
~608560160733025158901~~~~~~~~~~~~~~~~~~~~~~~~~~-2141059678~~~~~~2009-09-21 13:43:11~pause~0~~~~~~
~587000605177480842701~~~~~~~~~~~~~~~~~~~~~~~~~~1527279998~~~~~~2009-09-21 13:18:50~play~0~~~~~~
~50055534096150222020~~~~~~~~~~~~~~~~~~~~~~~~~~1538014332~~~~~~2009-09-21 18:15:57~play~0~~~~~~
~75123772794682909380~~~~~~~~~~~~~~~~~~~~~~~~~~1433301054~~~~~~2009-09-21 18:59:15~unpause~0~~~~~~
~61458174620428158290~~~~~~~~~~~~~~~~~~~~~~~~~~-2141059678~~~~~~2009-09-21 13:35:21~stop~0~~~~~~
~80122931714647926640~~~~~~~~~~~~~~~~~~~~~~~~~~-1807911989~~~~~~2009-09-21 13:29:07~play~0~~~~~~
~80305978414455368900~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 18:19:16~488308902~admin.brightcove.com~2009-09-25 08:55:35~~http://admin.brightcove.com/viewer/us1.21.02.03/federatedVideo/BrightcovePlayer.swf?isVid=true&purl=http://www.thestreet.com/_yahoo/video/10601184/rim-alternative.html?cm_ven=YAHOOV&cm_cat=FREE&cm_ite=NA&s=1&@videoPlayer=ref:10601184&autoStart=false&bgcolor=%23FFFFFF&externalAds=true&flashID=myExperience&height=412&playerId=1459183594&playerTag=TheStreet.com+Video+Player&playlistTargeting=playlistID&publisherId=1079018738&width=486~Video~~~~~~~~~fsc
2009-09-25 01:55:35~54179638535613620280~admin.brightcove.com~2009-09-21 13:19:22~2009-09-21 13:19:30~~~0~unknown~~Brisbane~Australia~AU~58.170.42.210~-27.5~153.017~Telstra Internet~~~~~~~~~~~~~~~~~~~~~~~~04~
~54961929118762409670~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 13:03:29~1608879007~admin.brightcove.com~2009-09-25 08:55:35~~http://admin.brightcove.com/viewer/federated/f_012.swf?bn=355&pubId=59121~Video~~~~~~~~~fsc
~76570972593000499900~~~~~~~~~~~~~~~~~~~~~~~~~~-289845470~~~~~~2009-09-21 13:57:25~stop~0~~~~~~
2009-09-25 01:55:35~73074623622818289310~admin.brightcove.com~2009-09-21 18:09:14~2009-09-21 18:11:58~~~0~unknown~~Bellevue~United States~US~131.107.0.113~47.6027~-122.156~Microsoft Corp~~~~~~~~~~~~~~~~~~~~~~~~WA~
~79585479433677909640~~~~~~~~~~~~~~~~~~~~~~~~~~-813740138~~~~~~2009-09-21 09:13:46~play~0~~~~~~
~60838950193892071170~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 09:42:39~32508387~admin.brightcove.com~2009-09-25 08:56:25~~http://admin.brightcove.com/viewer/us1.21.02.03/federatedVideoUI/BrightcovePlayer.swf?isUI=true&isVid=true&purl=http://console.brightcove.com/services/preview/bcpid35374403001?bc_token=MKb07nmRDAozml6zZh1pBGWsHxjeEOdcBwll39z8k2T%252FGVlAnLgfkfYfzEKgFHrI&autoStart=&bgcolor=%23FFFFFF&cacheAMFURL=http://services.brightcove.com/services/messagebroker/amf&flashID=myExperience&height=516&playerID=35374403001&publisherID=31193465001&width=300~Video~~~~~~~~~fsc
~59425527999071818900~~~~~~~~~~~~~~~~~~~~~~~~~~-192831593~~~~~~2009-09-21 17:01:39~stop~0~~~~~~
2009-09-25 01:56:25~54229776614462843670~admin.brightcove.com~2009-09-21 09:18:54~2009-09-21 09:19:39~~~0~unknown~~Athens~Greece~GR~62.103.170.36~37.9833~23.7332~OTEnet S.A.~~~~~~~~~~~~~~~~~~~~~~~~35~
~59299413110728502820~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 17:00:57~1029202533~admin.brightcove.com~2009-09-25 08:56:25~~http://admin.brightcove.com/viewer/us1.21.02.03/federatedVideoUI/BrightcovePlayer.swf?isVid=1&isUI=1&flashID=null&cdnURL=http://admin.brightcove.com&servicesURL=http://services.brightcove.com/services&playerWidth=400&playerHeight=348&publisherID=1417332081&autoStart=true&playerID=1403442681&suppressNoContentMessage=true~Video~~~~~~~~~fsc
2009-09-25 01:56:25~80840385779847857090~admin.brightcove.com~2009-09-21 09:03:07~2009-09-21 09:04:23~~~0~unknown~~Bucharest~Romania~RO~82.76.121.69~44.4333~26.1~Romania Data Systems~~~~~~~~~~~~~~~~~~~~~~~~10~
2009-09-25 01:56:25~64436012755510447270~admin.brightcove.com~2009-09-21 17:01:42~2009-09-21 17:02:13~~~0~unknown~~Fullerton~United States~US~75.83.100.158~33.8817~-117.924~Road Runner~~~~~~~~~~~~~~~~~~~~~~~~CA~
~64349085194392919530~~~~~~~~~~~~~~~~~~~~~~~~~~-1193368066~~~~~~2009-09-21 17:21:33~seek~0~~~~~~
~62964097441307341640~~~~~~~~~~~~~~~~~~~~~~~~~~-703480914~~~~~~2009-09-21 17:09:36~stop~0~~~~~~
~61545540869445028530~~~~~~~~~~~~~~~~~~~~~~~~~2009-09-21 17:10:42~816540283~admin.brightcove.com~2009-09-25 08:56:35~~http://admin.brightcove.com/viewer/us1.21.02.03/federatedVideoUI/BrightcovePlayer.swf?&width=613&height=387&flashID=lifetimePlayer&bgcolor=%23FFFFFF&playerID=9093010001&videoID=40864186001&additionalAdTargetingParams=undefined&isVid=true&linkBaseURL=http://www.mylifetime.com/on-tv/shows/project-runway/full-episodes/video/full-episode-5-fashion-headliners&autoStart=true&wmode=transparent&isUI=true~Video~~~~~~~~~fsc
~63622760826862436910~~~~~~~~~~~~~~~~~~~~~~~~~~-382774050~~~~~~2009-09-21 17:37:07~play~0~~~~~~
~56456276011434289130~~~~~~~~~~~~~~~~~~~~~~~~~~2071031722~~~~~~2009-09-21 17:21:05~pause~0~~~~~~
2009-09-25 01:56:35~72348551069183808890~admin.brightcove.com~2009-09-21 17:46:21~2009-09-21 17:47:21~~~0~unknown~~Houston~United States~US~69.15.167.132~29.7755~-95.4152~Wilson Cribbs & Goren~~~~~~~~~~~~~~~~~~~~~~~~TX~
\.

create table pre_visitor_ca_event_cao with (appendonly=true,orientation=column) as select * from pre_visitor_ca_event_ao;

select count(*) from pre_visitor_ca_event_ao;
select count(*) from pre_visitor_ca_event_cao;

drop table pre_visitor_ca_event_ao;
drop table pre_visitor_ca_event_cao;
create table qp_misc_jiras.tbl6701_test(i int[]);
insert into qp_misc_jiras.tbl6701_test values('{1,2,3}');
select i from qp_misc_jiras.tbl6701_test;
select i from qp_misc_jiras.tbl6701_test group by i;
drop table qp_misc_jiras.tbl6701_test;
select name,category from pg_settings where name like 'gp_enable_predicate_propagation';
create table qp_misc_jiras.tbl7161_co(a int, b varchar);
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 0) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 15) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 11) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 16) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 3) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 7) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 5) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 9) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 1) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 17) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 9) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 13) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 12) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 6) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 11) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 17) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 3) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 6) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 16) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 18) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 10) i;
select count(*) from qp_misc_jiras.tbl7161_co;
insert into qp_misc_jiras.tbl7161_co (a, b) select i, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i from generate_series(0, 4) i;
select count(*) from qp_misc_jiras.tbl7161_co;
drop table qp_misc_jiras.tbl7161_co;

select i, j into qp_misc_jiras.tbl6535_table from generate_series(1, 200) i, generate_series(1, 201) j;
insert into qp_misc_jiras.tbl6535_table select * from qp_misc_jiras.tbl6535_table ;
insert into qp_misc_jiras.tbl6535_table select * from qp_misc_jiras.tbl6535_table ;
insert into qp_misc_jiras.tbl6535_table select * from qp_misc_jiras.tbl6535_table ;
insert into qp_misc_jiras.tbl6535_table select * from qp_misc_jiras.tbl6535_table ;
insert into qp_misc_jiras.tbl6535_table select * from qp_misc_jiras.tbl6535_table ;
set gp_enable_agg_distinct_pruning  = off;
select count(distinct i), count(distinct j) from qp_misc_jiras.tbl6535_table;
set gp_enable_agg_distinct = off;
select count(distinct i), count(distinct j) from qp_misc_jiras.tbl6535_table;
drop table qp_misc_jiras.tbl6535_table;

create table statement_timeout_test(a int, b int);
insert into statement_timeout_test select i,i+1 from generate_series(1,10000)i;
prepare prestmt as select * from statement_timeout_test s1, statement_timeout_test s2, statement_timeout_test s3, statement_timeout_test s4, statement_timeout_test s5;
set statement_timeout = 1000; -- 1 sec
execute prestmt; -- should get cancelled automatically
drop table statement_timeout_test;
reset statement_timeout;
set gp_autostats_mode=none;

-- Test for an old bug with ANALYZE (analyze code has been rewritten since).
create table qp_misc_jiras.tbl_6934(x inet);
insert into qp_misc_jiras.tbl_6934 select (i%200 || '.' || i%11 || '.' || i%11 || '.' || i%100)::inet from generate_series(1,40000) i;
analyze qp_misc_jiras.tbl_6934; 
drop table qp_misc_jiras.tbl_6934;

create table qp_misc_jiras.tbl7286_test (i int, d date);
insert into qp_misc_jiras.tbl7286_test select i%10, '2009/01/01'::date + (i || ' days')::interval from generate_series(0, 99999) i;

set gp_enable_agg_distinct=off;
set gp_enable_agg_distinct_pruning=off;
set statement_mem='1000kB';
set optimizer_force_three_stage_scalar_dqa=off;

select count(distinct d) from qp_misc_jiras.tbl7286_test;
drop table qp_misc_jiras.tbl7286_test;
create table qp_misc_jiras.tbl7381_test (i int, t text, d date, ti time with time zone);
alter table qp_misc_jiras.tbl7381_test alter column t set storage external;

insert into qp_misc_jiras.tbl7381_test select i, '0123456789' || i, current_date, current_time from generate_series(0, 999) i;
insert into qp_misc_jiras.tbl7381_test select i, t||t||t||t||t||t||t||t||t||t, d, ti from qp_misc_jiras.tbl7381_test;
insert into qp_misc_jiras.tbl7381_test select i, t||t||t||t||t||t||t||t||t||t, d, ti from qp_misc_jiras.tbl7381_test;
insert into qp_misc_jiras.tbl7381_test select i, t||t||t||t||t||t||t||t||t||t, d, ti from qp_misc_jiras.tbl7381_test;

select max(length(t)) from qp_misc_jiras.tbl7381_test;

create table qp_misc_jiras.tbl7381_test1 (i int, t text, d date, ti time with time zone);
alter table qp_misc_jiras.tbl7381_test1 alter column t set storage extended;

insert into qp_misc_jiras.tbl7381_test1 select i,t,current_date, current_time from qp_misc_jiras.tbl7381_test;
select count(*) from qp_misc_jiras.tbl7381_test1;
drop table qp_misc_jiras.tbl7381_test;
drop table qp_misc_jiras.tbl7381_test1;
set gp_autostats_mode=none;
create table qp_misc_jiras.tbl7404_t1(x text);
create table qp_misc_jiras.tbl7404_t2(x text);

insert into qp_misc_jiras.tbl7404_t1 select ('foo'|| i::text) from generate_series(1,2) i;
insert into qp_misc_jiras.tbl7404_t2 select x from qp_misc_jiras.tbl7404_t1;

--Setting to modify system catalog
set allow_system_table_mods=true;

--
-- Case 1: make t1 and t2 appear really big
--

update pg_class set reltuples=100000, relpages=1000 where relname='qp_misc_jiras.tbl7404_t1';
update pg_class set reltuples=100000, relpages=1000 where relname='qp_misc_jiras.tbl7404_t2';

--explain select count(*) from qp_misc_jiras.tbl7404_t1 where substr(x,0,2) in (select substr(x,0,2) from qp_misc_jiras.tbl7404_t2);

select count(*) from qp_misc_jiras.tbl7404_t1 where substr(x,0,2) in (select substr(x,0,2) from qp_misc_jiras.tbl7404_t2);

--
-- Case 2: make t1, t2 appear small
--

update pg_class set reltuples=1, relpages=1 where relname='qp_misc_jiras.tbl7404_t1';
update pg_class set reltuples=1, relpages=1 where relname='qp_misc_jiras.tbl7404_t2';

--explain select * from qp_misc_jiras.tbl7404_t1 where substr(x,0,2) in (select substr(x,0,2) from qp_misc_jiras.tbl7404_t2);

select count(*) from qp_misc_jiras.tbl7404_t1 where substr(x,0,2) in (select substr(x,0,2) from qp_misc_jiras.tbl7404_t2);
select to_char(10, '999E99');
create table qp_misc_jiras.tbl7616_test (a int, b text) with (appendonly=true, orientation=column) distributed by (a);
insert into qp_misc_jiras.tbl7616_test select generate_series(1,1000), generate_series(1,1000);
select count(a.*) from qp_misc_jiras.tbl7616_test a inner join qp_misc_jiras.tbl7616_test b using (a);
select count(a.b) from qp_misc_jiras.tbl7616_test a inner join qp_misc_jiras.tbl7616_test b using (a);
drop table qp_misc_jiras.tbl7616_test;

create table qp_misc_jiras.tbl6874 (a int, b text) distributed by (a);
\d+ qp_misc_jiras.tbl6874
insert into qp_misc_jiras.tbl6874 values (generate_series(1,10),'test_1');
create index tbl6874_a on qp_misc_jiras.tbl6874 using bitmap(a);
\d+ qp_misc_jiras.tbl6874
drop index qp_misc_jiras.tbl6874_a;
\d+ qp_misc_jiras.tbl6874
drop table qp_misc_jiras.tbl6874;
CREATE TABLE qp_misc_jiras.tbl7740_rank (id int, gender char(1), count char(1) )
            DISTRIBUTED BY (id)
            PARTITION BY LIST (gender,count)
            ( PARTITION girls VALUES (('F','1')),
              PARTITION boys VALUES (('M','1')),
              DEFAULT PARTITION other );

insert into qp_misc_jiras.tbl7740_rank values(1,'F','1');
insert into qp_misc_jiras.tbl7740_rank values(1,'F','0');
insert into qp_misc_jiras.tbl7740_rank values(1,'M','1');
insert into qp_misc_jiras.tbl7740_rank values(1,'M','0');
-- start_ignore 
alter table qp_misc_jiras.tbl7740_rank split default partition at (values(('F', '0'))) into (partition mother, partition other);
-- end_ignore
select * from qp_misc_jiras.tbl7740_rank_1_prt_girls;
select * from qp_misc_jiras.tbl7740_rank_1_prt_boys;
select * from qp_misc_jiras.tbl7740_rank_1_prt_mother;
select * from qp_misc_jiras.tbl7740_rank_1_prt_other;
drop table qp_misc_jiras.tbl7740_rank;
-- This is the reproduction material for bug 7027.
-- Most of this is taken from an existing test, so this might be 
-- redundant, but it's easy to add it here and then we don't have to 
-- worry about the original test being modified in a way that no longer 
-- tests this scenario.

-- Create a column-oriented table.
CREATE TABLE qp_misc_jiras.one_of_every_data_type (
    id BIGINT,
    lseg_col LSEG,
    serial_col SERIAL
    ) 
 DISTRIBUTED BY (id)
 ;

CREATE FUNCTION TO_LSEG(TEXT) RETURNS LSEG AS
  $$
    SELECT lseg_in(textout($1))
  $$ LANGUAGE SQL;

INSERT INTO qp_misc_jiras.one_of_every_data_type ( lseg_col)
 VALUES (
  TO_LSEG('((' || 1 || ', ' || 1 || '), (' || 1 + 1 || ', ' || 1 + 1 || '))')
  ) ;

INSERT INTO qp_misc_jiras.one_of_every_data_type ( lseg_col)
 VALUES (
  TO_LSEG('((' || 1 || ', ' || 1 || '), (' || 1 + 1 || ', ' || 1 + 1 || '))')
  ) ;


-- Retrieve data from the table.
SELECT * FROM qp_misc_jiras.one_of_every_data_type ORDER BY id,serial_col;

-- Clean up.
DROP TABLE IF EXISTS qp_misc_jiras.one_of_every_data_type; 

create table qp_misc_jiras.tbl7553_test (i int, j int);
insert into qp_misc_jiras.tbl7553_test values(1,2);
explain select i as a, i as b from qp_misc_jiras.tbl7553_test group by grouping sets( (a, b), (a));

select i as a, i as b from qp_misc_jiras.tbl7553_test group by grouping sets( (a, b), (a)); 

explain select j as a, j as b from qp_misc_jiras.tbl7553_test group by grouping sets( (a, b), (a)); 

select j as a, j as b from qp_misc_jiras.tbl7553_test group by grouping sets( (a, b), (a)); 

drop table qp_misc_jiras.tbl7553_test;

-- Check that a table created with CTAS "inherits" the distribution key from
-- the source table.
create table qp_misc_jiras.tbl7268_foo (a varchar(15), b varchar(15)) distributed by (b);

\d qp_misc_jiras.tbl7268_foo;

create table qp_misc_jiras.tbl7268_bar as select * from qp_misc_jiras.tbl7268_foo;

\d qp_misc_jiras.tbl7268_bar; 

drop table if exists qp_misc_jiras.tbl7268_foo;
drop table if exists qp_misc_jiras.tbl7268_bar;

create table qp_misc_jiras.tbl7268_foo (a int, b int) distributed by (b);

\d qp_misc_jiras.tbl7268_foo;

create table qp_misc_jiras.tbl7268_bar as select * from qp_misc_jiras.tbl7268_foo;

\d qp_misc_jiras.tbl7268_bar; 

drop table if exists qp_misc_jiras.tbl7268_foo;
drop table if exists qp_misc_jiras.tbl7268_bar;

create table qp_misc_jiras.tbl7268_foo (a varchar(15), b int) distributed by (b);

\d qp_misc_jiras.tbl7268_foo;

create table qp_misc_jiras.tbl7268_bar as select * from qp_misc_jiras.tbl7268_foo;

\d qp_misc_jiras.tbl7268_bar; 

drop table if exists qp_misc_jiras.tbl7268_foo;
drop table if exists qp_misc_jiras.tbl7268_bar;

create table qp_misc_jiras.tbl7268_foo (a int, b varchar(15)) distributed by (b);

\d qp_misc_jiras.tbl7268_foo;

create table qp_misc_jiras.tbl7268_bar as select * from qp_misc_jiras.tbl7268_foo;

\d qp_misc_jiras.tbl7268_bar; 

drop table if exists qp_misc_jiras.tbl7268_foo;
drop table if exists qp_misc_jiras.tbl7268_bar;

create table qp_misc_jiras.tbl6775_bar(x int) distributed randomly;        
create table qp_misc_jiras.tbl6775_foo(like qp_misc_jiras.tbl6775_bar) distributed randomly;

alter table qp_misc_jiras.tbl6775_foo add column y int;
alter table qp_misc_jiras.tbl6775_foo drop column y;

insert into qp_misc_jiras.tbl6775_foo(x) select  t1.x from qp_misc_jiras.tbl6775_bar t1 join qp_misc_jiras.tbl6775_bar t2 on t1.x=t2.x;

insert into qp_misc_jiras.tbl6775_foo(x) select  t1.x from qp_misc_jiras.tbl6775_bar t1;
insert into qp_misc_jiras.tbl6775_foo(x) select  t1.x from qp_misc_jiras.tbl6775_bar t1 group by t1.x;

drop table if exists qp_misc_jiras.tbl6775_foo;
drop table if exists qp_misc_jiras.tbl6775_bar;
--
-- Test Set 1
--
set optimizer_segments=3;
create table qp_misc_jiras.sample as select generate_series(1,1000);

create table qp_misc_jiras.fim1 (a int);
insert into qp_misc_jiras.fim1 values(100);
insert into qp_misc_jiras.fim1 values(200);
insert into qp_misc_jiras.fim1 values(300);

create table qp_misc_jiras.pg_foo(x int);
insert into qp_misc_jiras.pg_foo values(111);
insert into qp_misc_jiras.pg_foo values(222);
insert into qp_misc_jiras.pg_foo values(333);


create or replace function func1(int) returns int as $$
declare
	t1 int;
begin
	execute 'select count(*) from pg_class' into t1;
	return t1;
end;
$$
language plpgsql;

drop table if exists qp_misc_jiras.bar;
create table qp_misc_jiras.bar(a int);

create or replace function func2(int) returns int as $$
declare
	t1 int;
begin
	execute 'select count(*) from qp_misc_jiras.pg_foo' into t1;
	return t1;
end;
$$
language plpgsql;

select gp_segment_id, a, func2(a) from qp_misc_jiras.fim1; -- we should disallow this


create or replace function func3(int) returns int as $$
declare
	t1 int;
begin
	execute 'select count(*) from (select count(distinct relname) from pg_class) a' into t1;
	return t1;
end;
$$
language plpgsql;

select gp_segment_id, a, func3(a) from qp_misc_jiras.fim1; -- we allow this

--
-- Test Set 2
--

drop table if exists qp_misc_jiras.fim1;

create table qp_misc_jiras.fim1 (a int, chldrn_id varchar(50)) ;
insert into qp_misc_jiras.fim1 values (1,'1~9');
insert into qp_misc_jiras.fim1 values (2,'2~9');

CREATE OR REPLACE FUNCTION public.array_intersect1(anyarray, anyarray) RETURNS anyarray as $$
SELECT ARRAY(SELECT x FROM unnest($1) x INTERSECT SELECT y from unnest($2) y)
$$ language sql;

CREATE OR REPLACE FUNCTION public.array_intersect2(anyarray, anyarray) RETURNS anyarray as $$
SELECT ARRAY(SELECT $1[i]
    FROM generate_series( array_lower($1, 1), array_upper($1, 1) ) i
      WHERE ARRAY[$1[i]] && $2)
$$ language sql;

CREATE OR REPLACE FUNCTION public.array_intersect3(anyarray, anyarray) RETURNS anyarray as $$
SELECT ARRAY(SELECT x FROM unnest($1) x INTERSECT SELECT y from unnest($2) y)
$$ language sql immutable;

CREATE OR REPLACE FUNCTION public.array_intersect4(anyarray, anyarray) RETURNS anyarray as $$
SELECT ARRAY(SELECT $1[i]
    FROM generate_series( array_lower($1, 1), array_upper($1, 1) ) i
      WHERE ARRAY[$1[i]] && $2)
$$ language sql immutable;


select array_intersect1(array[1,9]::int4[], array[-1,5,4,3,2,1,0]::int4[]) from qp_misc_jiras.fim1; -- we allow it even though the function is volatile.
select array_intersect2(array[1,9]::int4[], array[-1,5,4,3,2,1,0]::int4[]) from qp_misc_jiras.fim1; -- we allow it even though the function is volatile.
select array_intersect3(array[1,9]::int4[], array[-1,5,4,3,2,1,0]::int4[]) from qp_misc_jiras.fim1; -- should work.
select array_intersect4(array[1,9]::int4[], array[-1,5,4,3,2,1,0]::int4[]) from qp_misc_jiras.fim1; -- should work.

select array_intersect3(i::int4[], array[-1,5,4,3,2,1,0]::int4[]) from (select string_to_array(chldrn_id,'~') as i from qp_misc_jiras.fim1) b  ; -- should produce {1},{2}

select array_intersect4(i::int4[], array[-1,5,4,3,2,1,0]::int4[]) from (select string_to_array(chldrn_id,'~') as i from qp_misc_jiras.fim1) b  ; -- should produce {1},{2}

drop function func1(int);
drop function func2(int);
drop function func3(int);

drop table qp_misc_jiras.sample;
drop table qp_misc_jiras.fim1;
drop table qp_misc_jiras.pg_foo;
drop table qp_misc_jiras.bar;

-- Create 2 tables from which we can inherit.  
CREATE TABLE qp_misc_jiras.tableA (ssn INT, lastName VARCHAR, junk INT) DISTRIBUTED BY (ssn);
CREATE TABLE qp_misc_jiras.tableB (id INT, lastName VARCHAR, moreJunk INT) DISTRIBUTED BY (id);
-- Create a table that inherits from the previous two.
CREATE TABLE qp_misc_jiras.tableC (uid INT) INHERITS (qp_misc_jiras.tableA, qp_misc_jiras.tableB) DISTRIBUTED BY (uid);

-- Expose the bug.
SELECT * FROM qp_misc_jiras.tableC;

-- start_ignore
DROP TABLE qp_misc_jiras.tableC;
DROP TABLE qp_misc_jiras.tableB;
DROP TABLE qp_misc_jiras.tableA;
-- end_ignore


CREATE TABLE qp_misc_jiras.tableA (ssn INT, lastName VARCHAR, junk INT) DISTRIBUTED BY (ssn);
CREATE TABLE qp_misc_jiras.tableB (id INT, lastName VARCHAR, moreJunk INT) DISTRIBUTED BY (id);
INSERT INTO qp_misc_jiras.tablea VALUES (1, 'foo',2);
INSERT INTO qp_misc_jiras.tableb VALUES (2, 'bar',4);
CREATE TABLE qp_misc_jiras.tableC (uid1 INT, uid2 INT, uid3 INT) INHERITS (qp_misc_jiras.tableA, qp_misc_jiras.tableB) DISTRIBUTED BY (uid1, uid2, uid3);
INSERT INTO qp_misc_jiras.tableC VALUES (3, 'foobar', 5, 6, 7, 8, 9, 10);

-- Should display records from tableA and tableC.
SELECT * FROM qp_misc_jiras.tableA ORDER BY ssn;
-- Should display records from tableB and tableC.
SELECT * FROM qp_misc_jiras.tableB ORDER BY id;
-- Should display only records from tableC.
SELECT * FROM qp_misc_jiras.tableC;


CREATE TABLE qp_misc_jiras.tableD (a INT, b INT, c INT) DISTRIBUTED BY (a);
INSERT INTO qp_misc_jiras.tableD VALUES (11, 12, 13);
CREATE TABLE qp_misc_jiras.tableE (x INT) INHERITS (qp_misc_jiras.tableA, qp_misc_jiras.tableD) DISTRIBUTED BY (x);
INSERT INTO qp_misc_jiras.tableE VALUES (1,'foo', -2, 11, 12, -13, 14);
-- Should display records from tableD and tableE.
SELECT * FROM qp_misc_jiras.tableD ORDER BY a, c;
-- Should display records from only table E.
SELECT * FROM qp_misc_jiras.tableE;
-- Should display records from tableA, tableC, and tableE.
SELECT * FROM qp_misc_jiras.tableA ORDER BY ssn, junk;


CREATE TABLE qp_misc_jiras.tableAA (ssn INT, lastName VARCHAR, junk INT) DISTRIBUTED randomly;
CREATE TABLE qp_misc_jiras.tableBB (id INT, lastName VARCHAR, moreJunk INT) DISTRIBUTED randomly;
INSERT INTO qp_misc_jiras.tableAA VALUES (1, 'foo',2);
INSERT INTO qp_misc_jiras.tableBB VALUES (2, 'bar',4);
CREATE TABLE qp_misc_jiras.tableCC (uid1 INT, uid2 INT, uid3 INT) INHERITS (qp_misc_jiras.tableAA, qp_misc_jiras.tableBB) DISTRIBUTED randomly;
INSERT INTO qp_misc_jiras.tableCC VALUES (3, 'foobar', 5, 6, 7, 8, 9, 10);

-- Should display records from tableAA and tableCC.
SELECT * FROM qp_misc_jiras.tableAA ORDER BY ssn, junk;
-- Should display records from tableBB and tableCC.
SELECT * FROM qp_misc_jiras.tableBB ORDER BY id, moreJunk;
-- Should display records from only tableCC.
SELECT * FROM qp_misc_jiras.tableCC;




-- start_ignore
set statement_timeout=0;
set gp_interconnect_setup_timeout=7200;
-- end_ignore


CREATE TABLE qp_misc_jiras.inet_ip_pairs (saddr inet, daddr inet, instance_count int8);
SELECT * FROM pg_indexes WHERE tablename='inet_ip_pairs';
INSERT INTO qp_misc_jiras.inet_ip_pairs (saddr) SELECT (i%201 || '.' || i%11 || '.' || i%11 || '.' || i%100)::inet FROM generate_series(1,1000000) i;


--
-- MPP-6870: CREATE INDEX caused Out-Of-Memory problem
--
CREATE INDEX  inet_ip_pairs_idx1 ON  qp_misc_jiras.inet_ip_pairs (saddr);
SELECT * FROM pg_indexes WHERE tablename='inet_ip_pairs';


--
-- Data should be evenly distributed to multiple segments?
--
SELECT gp_segment_id,count(*) FROM qp_misc_jiras.inet_ip_pairs GROUP BY 1;


-- start_ignore
DROP TABLE IF EXISTS qp_misc_jiras.inet_ip_pairs CASCADE;
-- end_ignore




set gp_autostats_mode=none;

create table qp_misc_jiras.tbl_7498_t1(x int, y text) distributed by (x);
insert into qp_misc_jiras.tbl_7498_t1 select x, 'foo' from generate_series(1,70000) x;
create table qp_misc_jiras.tbl_7498_t2 as select * from qp_misc_jiras.tbl_7498_t1;
create table qp_misc_jiras.tbl_7498_t3 as select * from qp_misc_jiras.tbl_7498_t1;
drop table qp_misc_jiras.tbl_7498_t2;

drop table if exists qp_misc_jiras.tbl_7498_t1, qp_misc_jiras.tbl_7498_t2, qp_misc_jiras.tbl_7498_t3 cascade;


create table qp_misc_jiras.tbl_7498_t1(x int, y text) distributed by (x);
insert into qp_misc_jiras.tbl_7498_t1 select x, 'foo' from generate_series(1,70000) x;
create table qp_misc_jiras.tbl_7498_t2 as select * from qp_misc_jiras.tbl_7498_t1;
create table qp_misc_jiras.tbl_7498_t3 as select * from qp_misc_jiras.tbl_7498_t1;


-- clean up all the test tables
-- start_ignore
drop table if exists qp_misc_jiras.tbl_7498_t1, qp_misc_jiras.tbl_7498_t2, qp_misc_jiras.tbl_7498_t3 cascade;
-- end_ignore

-- currtid2() is not supported in GPDB.
select currtid2('tbl7886'::text, '(0,1)'::tid);

create table qp_misc_jiras.tbl8017(x int, y int) distributed by (x);
create index tbl8017_x on qp_misc_jiras.tbl8017(x);
insert into qp_misc_jiras.tbl8017 select generate_series(1,100), generate_series(1,100);
analyze qp_misc_jiras.tbl8017;
select relname, reltuples, relpages from pg_class where relname in ('tbl8017', 'tbl8017_x');
drop table qp_misc_jiras.tbl8017;


create table qp_misc_jiras.tbl7957_foo(x int, y int, z int) distributed by (x);

insert into qp_misc_jiras.tbl7957_foo values(1,2,3);
insert into qp_misc_jiras.tbl7957_foo values(1,2,3);
insert into qp_misc_jiras.tbl7957_foo values(1,2,3);

select count(*) as c from qp_misc_jiras.tbl7957_foo group by cube(x) order by c;

select count(*) as c from qp_misc_jiras.tbl7957_foo group by cube(y) order by c;

select count(*) as c from qp_misc_jiras.tbl7957_foo group by cube(z) order by c;

select sum(z) as c from qp_misc_jiras.tbl7957_foo group by cube(x) order by c;

select sum(z) as c from qp_misc_jiras.tbl7957_foo group by cube(y) order by c;

select sum(z) as c from qp_misc_jiras.tbl7957_foo group by cube(z) order by c;

drop table qp_misc_jiras.tbl7957_foo;

--
-- Test for MPP-6421, an old bug in GPDB 3.2.3.0.
--
-- Description of the bug from the original report:
--
-- Executing immutable functions that contain selects from a table as a
-- non-superuser causes a SIGSEGV and a system core dump.  Executing the same
-- function as a superuser does NOT cause the problem.  Functions that are not
-- immutable do NOT cause the problem.  This problem seems to have been
-- introduced in 3.2.3.0, it is NOT reproducible in versions earlier than
-- that.  It is still reproducible in 3.3.0.1.  It is reproducible on both
-- Solaris and Mac platforms.
--
CREATE SCHEMA mustan;
CREATE TABLE mustan.test(
	id int,
	d date
);
INSERT INTO mustan.test( id, d ) VALUES( 1, '20080101' ), (2, '20080102');

CREATE USER regress_mustan NOLOGIN;
GRANT USAGE ON SCHEMA mustan TO regress_mustan;
GRANT SELECT ON mustan.test to regress_mustan;

CREATE OR REPLACE FUNCTION mustan.f1() RETURNS DATE AS $$
BEGIN
	return '20080101';
END;
$$LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mustan.f2() RETURNS DATE AS $$
BEGIN
	return '20080101';
END;
$$LANGUAGE plpgsql immutable;

CREATE OR REPLACE FUNCTION mustan.f3() RETURNS DATE AS $$
DECLARE
	out_dt date;
BEGIN
	SELECT '20080101' INTO out_dt;
	return '20080101';
END;
$$LANGUAGE plpgsql immutable;

CREATE OR REPLACE FUNCTION mustan.f4() RETURNS DATE AS $$
DECLARE
	out_dt date;
BEGIN
	SELECT '20080101' INTO out_dt FROM mustan.test WHERE id=2;
	return out_dt;
END;
$$LANGUAGE plpgsql immutable;

CREATE OR REPLACE FUNCTION mustan.f5() RETURNS DATE AS $$
DECLARE
	out_dt date;
BEGIN
	SELECT d INTO out_dt FROM mustan.test WHERE id=2;
	return out_dt;
END;
$$LANGUAGE plpgsql immutable;

CREATE OR REPLACE FUNCTION mustan.f6( n int ) RETURNS DATE AS $$
DECLARE
	out_dt date;
BEGIN
	SELECT d INTO out_dt FROM mustan.test WHERE id=n;
	return out_dt;
END;
$$LANGUAGE plpgsql immutable;

CREATE OR REPLACE FUNCTION mustan.f7( in_d date ) RETURNS DATE AS $$
DECLARE
	out_dt date;
BEGIN
	SELECT d INTO out_dt FROM mustan.test WHERE d=in_d;
	return out_dt;
END;
$$LANGUAGE plpgsql immutable;

CREATE OR REPLACE FUNCTION mustan.f7( in_d text ) RETURNS DATE AS $$
DECLARE
	out_dt date;
BEGIN
	-- SELECT d INTO out_dt FROM mustan.test WHERE d=cast( in_d as date );
	SELECT d INTO out_dt FROM mustan.test WHERE d=in_d::date;
	return out_dt;
END;
$$LANGUAGE plpgsql immutable;

SET SESSION AUTHORIZATION regress_mustan;

select * from mustan.f1();
select * from mustan.f2();
select * from mustan.f3();
select * from mustan.f4();
select * from mustan.f5();
select * from mustan.f6( 2 );
select * from mustan.f7( '20080102'::date );
select * from mustan.f7( '20080102' );-- 

-- Clean up
RESET SESSION AUTHORIZATION;
drop schema mustan cascade;
drop user regress_mustan;


-- start_ignore
create table qp_misc_jiras.tbl5994_test (i int, j int);
-- end_ignore

insert into qp_misc_jiras.tbl5994_test select i, i%1000 from generate_series(1, 100000) i;


reset enable_indexscan; 
reset enable_nestloop; 
reset enable_seqscan; 
reset gp_enable_agg_distinct; 
reset gp_enable_agg_distinct_pruning;

-- both queries should use hashagg
explain select count(distinct j) from (select t1.* from qp_misc_jiras.tbl5994_test t1, qp_misc_jiras.tbl5994_test t2 where t1.j = t2.j) tmp group by j;
explain select count(distinct j) from (select t1.* from qp_misc_jiras.tbl5994_test t1, qp_misc_jiras.tbl5994_test t2 where t1.i = t2.i) tmp group by j;

-- Try same two queries, with group agg.
set enable_groupagg=on;
set enable_hashagg=off;
explain select count(distinct j) from (select t1.* from qp_misc_jiras.tbl5994_test t1, qp_misc_jiras.tbl5994_test t2 where t1.j = t2.j) tmp group by j;
explain select count(distinct j) from (select t1.* from qp_misc_jiras.tbl5994_test t1, qp_misc_jiras.tbl5994_test t2 where t1.i = t2.i) tmp group by j;

reset enable_groupagg;
reset enable_hashagg;
drop table qp_misc_jiras.tbl5994_test;

CREATE TABLE qp_misc_jiras.tbl_8205 (
text_col text,
bigint_col bigint,
char_vary_col character varying(30),
numeric_col numeric)
DISTRIBUTED RANDOMLY;

set enable_seqscan = off;
set enable_bitmapscan = off;
set enable_indexscan = on;

show enable_indexscan;

explain analyze select reltablespace  from pg_class where oid = (select reltoastrelid from pg_class where relname='tbl_8205'); -- force_explain

select reltablespace from pg_class where oid = (select reltoastrelid from pg_class where relname='tbl_8205');

drop table qp_misc_jiras.tbl_8205;
reset enable_seqscan;
reset enable_bitmapscan;
reset enable_indexscan;

create table qp_misc_jiras.tbl8258 (a int, b double precision)
PARTITION BY RANGE(b)
(START (1::double precision) END (100::double precision)
EVERY ((20)::double precision),
PARTITION p1 START (100::double precision) END (150::double precision));

select pg_get_partition_def('qp_misc_jiras.tbl8258'::regclass, true);

drop table qp_misc_jiras.tbl8258;

reset statement_timeout;
set statement_mem = '512MB';

create table qp_misc_jiras.utable (
    tstart timestamp,
    tfinish timestamp,
    utxt text,
    unum numeric
    )
    distributed by (tstart);

create view qp_misc_jiras.uview(period, ctxt, wsum) as
select
    period, 
    utxt,
    sum(usum) over (partition by period)
from
	(   select 
		    to_char( tstart, 'YYYY-MM-DD HH24:00'), 
		    utxt, 
		    sum(unum)
		from
		(
			select 
			    tstart, 
			    utxt,
			    case -- this case seems critical
			        when tfinish >tstart
			        then unum 
			        else 0 
			        end
			from qp_misc_jiras.utable
		) x(tstart, utxt, unum)
		
		group by 1, 2
	) y(period, utxt, usum);


insert into qp_misc_jiras.utable values
    (timestamp '2009-05-01 01:01:10', timestamp '2009-05-01 02:01:10', 'a', 1.0),
    (timestamp '2009-05-01 02:01:10', timestamp '2009-05-01 01:01:10', 'a', 1.0),
    (timestamp '2009-05-01 02:01:10', timestamp '2009-05-01 03:01:10', 'a', 1.0),
    (timestamp '2009-05-01 03:01:10', timestamp '2009-05-01 03:01:10', 'b', 1.0),
    (timestamp '2009-05-01 01:01:09', timestamp '2009-05-01 02:01:10', 'b', 1.0),
    (timestamp '2009-05-03 01:01:10', timestamp '2009-05-01 02:01:10', 'b', 1.0);


-- This works.

reset statement_mem;

select 
    period::date,
--    avg( case ctxt when 'a' then wsum end) as "a"
    avg( case ctxt when 'b' then wsum end) as "b"
from
    qp_misc_jiras.uview 
group by 1;


-- This used to fail on an assert on a segment (palloc)

select 
    period::date, -- the cast is significant 
    avg( case ctxt when 'a' then wsum end) as "a", 
    avg( case ctxt when 'b' then wsum end) as "b"
from
    qp_misc_jiras.uview 
group by 1;


-- start_ignore
drop view qp_misc_jiras.uview;
drop table qp_misc_jiras.utable;
-- end_ignore



CREATE TABLE qp_misc_jiras.foo_6325 (
  foo_6325_attr text
)
DISTRIBUTED BY (foo_6325_attr);

CREATE TABLE qp_misc_jiras.bar_6325 (
  bar_6325_attr text
)
DISTRIBUTED RANDOMLY;

set enable_nestloop=on;

-- force_explain

explain
select  t1.bar_6325_attr::int, t1.foo_6325_attr 
from (
	select c.bar_6325_attr::int, a.foo_6325_attr
	from qp_misc_jiras.foo_6325 a, qp_misc_jiras.bar_6325 c
	group by 1,2
     ) as t1 
group by 1, 2;


-- start_ignore
drop table qp_misc_jiras.foo_6325;
drop table qp_misc_jiras.bar_6325;
-- end_ignore

-- Test for an old bug in extending a heap relation beyond RELSEG_SIZE (1 GB).
-- The insert failed with an error:
--   could not open segment 1 of relation ...: No such file or directory
create table qp_misc_jiras.heap_tbl8621 (a int) with (fillfactor=10) distributed by (a);
insert into qp_misc_jiras.heap_tbl8621 select 1 from generate_series(1, 3100000);

-- Verify that the table really was larger than 1 GB. Otherwise we wouldn't
-- test the segment boundary.
select pg_relation_size('qp_misc_jiras.heap_tbl8621') > 1100000000 as over_1gb;

DROP TABLE qp_misc_jiras.heap_tbl8621;


CREATE TABLE qp_misc_jiras.tbl8860_1 (
     id INTEGER NOT NULL,
     key TEXT NOT NULL
) distributed by (id);

CREATE TABLE qp_misc_jiras.tbl8860_2 (
     id INTEGER NOT NULL,
     key CHARACTER VARYING(50) NOT NULL
) distributed by (id);
INSERT INTO qp_misc_jiras.tbl8860_1 SELECT 1, key FROM qp_misc_jiras.tbl8860_2;
-- start_ignore
drop table if exists qp_misc_jiras.tbl8860_1;
drop table if exists qp_misc_jiras.tbl8860_2;
-- end_ignore

-- MPP-9739: Unicode not supported - chr() ascii() functions do not return unicode codepoints for multibyte characters
Create Table qp_misc_jiras.tbl9739_unicode_test
(
id int Not Null,
str Varchar(10) Not Null
)
Distributed By (id);

Insert Into qp_misc_jiras.tbl9739_unicode_test (id, str) Values (1, Chr(1000) || Chr(2001) || Chr(3010));
Insert Into qp_misc_jiras.tbl9739_unicode_test (id, str) Values (2, Chr(105) || Chr(110) || Chr(35));

Select id,
Ascii (Substr(str, 1, 1)) As first_char_code,
Ascii (Substr(str, 2, 1)) As second_char_code,
Ascii (Substr(str, 3, 1)) As third_char_code,
Ascii (Substr(str, 4, 1)) As fourth_char_code
From qp_misc_jiras.tbl9739_unicode_test
Order
By id;

drop table qp_misc_jiras.tbl9739_unicode_test;


--
-- Create table qp_misc_jiras.tbl9613
--

create table qp_misc_jiras.tbl9613(x int, y numeric) distributed by (x);

insert into qp_misc_jiras.tbl9613 values(1, 1);
insert into qp_misc_jiras.tbl9613 values(1, 2);
insert into qp_misc_jiras.tbl9613 values(1, 3);
insert into qp_misc_jiras.tbl9613 values(1, 4);
insert into qp_misc_jiras.tbl9613 values(1, 5);
insert into qp_misc_jiras.tbl9613 values(1, 6);
insert into qp_misc_jiras.tbl9613 values(1, 7);
insert into qp_misc_jiras.tbl9613 values(1, 8);


--
-- First set of functions. These do not access external relations.
--

CREATE OR REPLACE FUNCTION _final_median(numeric[])
   RETURNS numeric AS
$$
   SELECT AVG(val)
   FROM (
     SELECT val
     FROM unnest($1) val
     ORDER BY 1
     LIMIT  2 - MOD(array_upper($1, 1), 2)
     OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
   ) sub;
$$
LANGUAGE 'sql' IMMUTABLE;

-- start_ignore
DROP AGGREGATE "median"(numeric);
-- end_ignore
 
CREATE AGGREGATE "median"(numeric) (
  SFUNC=array_append,
  STYPE=numeric[],
  FINALFUNC=_final_median,
  INITCOND='{}'
);

--
-- Second set of functions. _final_median2 accesses relation in a subquery. This should be disallowed.
--

CREATE OR REPLACE FUNCTION _final_median2(numeric[])
   RETURNS numeric AS
$$
   SELECT AVG(val.x)
   FROM (
   	select count(*) as x from qp_misc_jiras.tbl9613
   	) val;
$$
LANGUAGE 'sql' IMMUTABLE;

-- start_ignore
DROP AGGREGATE median2(numeric);
-- end_ignore
 
CREATE AGGREGATE median2(numeric) (
  SFUNC=array_append,
  STYPE=numeric[],
  FINALFUNC=_final_median2,
  INITCOND='{}'
);

--
-- Queries.
--

explain select x, "median"(y) from qp_misc_jiras.tbl9613 group by x; -- this should be allowed

select x, "median"(y) from qp_misc_jiras.tbl9613 group by x; -- this should be allowed

explain select x, median2(y) from qp_misc_jiras.tbl9613 group by x; -- this should be disallowed

select x, median2(y) from qp_misc_jiras.tbl9613 group by x; -- this should be disallowed


-- start_ignore
drop table qp_misc_jiras.tbl9613;
DROP AGGREGATE "median"(numeric);
DROP AGGREGATE median2(numeric);
-- end_ignore

create table qp_misc_jiras.tbl7285_axg (id int, time timestamp) partition by range (time) (start ('2009-01-01 00:00:00'::timestamp without time zone) end ('2009-01-03 00:00:00'::timestamp without time zone) every ('1 day'::interval));
alter table qp_misc_jiras.tbl7285_axg add partition p20090103 start ('2009-01-03 00:00:00'::timestamp without time zone) end ('2009-01-04 00:00:00'::timestamp without time zone);
select count(*) from qp_misc_jiras.tbl7285_axg;
drop table qp_misc_jiras.tbl7285_axg;
show autovacuum;
set autovacuum=on;

create table qp_misc_jiras.tbl9706ao ( a int ) with (appendonly=true, compresslevel=5) distributed by (a);
create table qp_misc_jiras.tbl9706aoc ( a int ) with (appendonly=true, compresslevel=5, orientation=column) distributed by (a);

insert into qp_misc_jiras.tbl9706ao select i from generate_series(1, 100) i;
insert into qp_misc_jiras.tbl9706aoc select i from generate_series(1, 100) i;

select * from pg_catalog.get_ao_distribution('qp_misc_jiras.tbl9706ao'::regclass) order by 1 limit 2;
select * from pg_catalog.get_ao_distribution('qp_misc_jiras.tbl9706aoc'::regclass) order by 1 limit 2;
create table qp_misc_jiras.bmap2 (a varchar, b varchar);
insert into qp_misc_jiras.bmap2 values ('1', NULL);
create index bmap2_index on qp_misc_jiras.bmap2 using bitmap (a, b);
create table qp_misc_jiras.badbitmapindex (x int, y int, z int);
insert into qp_misc_jiras.badbitmapindex values (0,0,0);
insert into qp_misc_jiras.badbitmapindex values (0,0,NULL);
create index badbitmapindex1 on qp_misc_jiras.badbitmapindex using bitmap(y,z);
analyze qp_misc_jiras.badbitmapindex;
--explain select * from badbitmapindex where y = 0 and z = 0;
insert into qp_misc_jiras.badbitmapindex select generate_series(1,100*500);
analyze qp_misc_jiras.badbitmapindex;
--explain select * from badbitmapindex where y = 0 and z = 0;
select * from qp_misc_jiras.badbitmapindex where y = 0 and z = 0;
drop index qp_misc_jiras.badbitmapindex1;
drop index qp_misc_jiras.bmap2_index;
drop table qp_misc_jiras.badbitmapindex;
drop table qp_misc_jiras.bmap2;

-- Test for ancient bug that led to a crash in EXPLAIN (MPP-9957)
CREATE TABLE qp_misc_jiras.ir_voice_sms_and_data (
    imsi_number character varying(35),
    ir_call_country_name character varying(35),
    partner_operator_id character varying(15),
    tap_event_identifier character varying(35),
    ir_call_originating_number character varying(30),
    ir_call_terminating_number character varying(30),
    ir_call_type_group_code character varying(10),
    ir_call_supplementary_svc_code character(3),
    ir_call_charged_item_code character(1)
) distributed randomly;

set gp_motion_cost_per_row =0.1; -- to get a two-stage agg
explain select
case when ir_call_type_group_code in ('H', 'VH', 'PCB') then 'Thailland'
else 'Unidentify' end || 'a'
from qp_misc_jiras.ir_voice_sms_and_data
group by 
case when ir_call_supplementary_svc_code in ('S20','S21')
then 'MO' else 'foo' end
, 
case when ir_call_type_group_code in ('H', 'VH', 'PCB') then 'Thailland'
else 'Unidentify' end
;
DROP TABLE qp_misc_jiras.ir_voice_sms_and_data;
reset gp_motion_cost_per_row;

create table qp_misc_jiras.r
    (a int, b int, c int)
    with (appendonly = true)
    distributed by (a);

create index rb
    on qp_misc_jiras.r(b);

insert into qp_misc_jiras.r
    values
        (1,2,3),
        (4,5,6),
        (7,8,9);

--alter table r
--    set with (reorganize = true)
--    distributed by (c);

alter table qp_misc_jiras.r
    set distributed by (c);
drop table if exists qp_misc_jiras.r cascade;

-- Triggers are applied AFTER the row has been routed per the distribution
-- policy. However, a trigger can change the value of the distribution column.
-- This means, we have an unexpected row in a segment, which can lead to wrong
-- results. Including gp_segment_id in below queries allows us to ensure that
-- no redistribution has occurred.
CREATE TABLE qp_misc_jiras.test_trig(id int, aaa text) DISTRIBUTED BY (id);
INSERT INTO qp_misc_jiras.test_trig VALUES (1, 'before creating trigger');
SELECT gp_segment_id, * FROM qp_misc_jiras.test_trig;

CREATE OR REPLACE FUNCTION fn_trig() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
   NEW.aaa = NEW.aaa || '1';
   NEW.id = NEW.id + 1;
   RAISE NOTICE '%', NEW.id;
RETURN NEW;
END
$$;

CREATE TRIGGER test_trig_1
BEFORE INSERT OR UPDATE ON qp_misc_jiras.test_trig
FOR EACH ROW
    EXECUTE PROCEDURE fn_trig();

INSERT INTO qp_misc_jiras.test_trig VALUES (0, 'after creating trigger');

INSERT INTO qp_misc_jiras.test_trig VALUES (1, 'aaa');
SELECT gp_segment_id, * FROM qp_misc_jiras.test_trig;

UPDATE qp_misc_jiras.test_trig SET id = id;
SELECT gp_segment_id, * FROM qp_misc_jiras.test_trig;

DROP TABLE qp_misc_jiras.test_trig;
DROP FUNCTION fn_trig();

set gp_select_invisible=false;

create table qp_misc_jiras.tbl1_tbl_11257 (
 id            bigint,
partition_key timestamp,
distribution_key bigint not null,
constraint pk_tbl1_tbl_11257 primary key (distribution_key, partition_key, id)
)
distributed by (distribution_key)
partition by range (partition_key)
(
default partition default_partition,
partition d_2010_09_28 start (date '2010-09-28') end (date '2010-09-29')
);
select relname, distkey as distribution_attributes from gp_distribution_policy p, pg_class c where p.localoid = c.oid and relname like 'tbl1_tbl_11257%';
-- start_ignore
alter table qp_misc_jiras.tbl1_tbl_11257 split default partition
start (date '2010-09-27' )
end (date '2010-09-28')
into (partition d_2010_09_27, partition default_partition);
-- end_ignore
select relname, distkey as distribution_attributes from gp_distribution_policy p, pg_class c where p.localoid = c.oid and relname like 'tbl1_tbl_11257%';



create table qp_misc_jiras.tbl2_tbl_11257 (
a int,
b int,
c int,
constraint pk_tbl2_tbl_11257 primary key (a,b,c)
)
distributed by (a)
partition by range (b)
(
 default partition default_partition,
partition p1 start (1) end (2)
 );
insert into qp_misc_jiras.tbl2_tbl_11257 values(1,2,3);
select * from qp_misc_jiras.tbl2_tbl_11257;
delete from qp_misc_jiras.tbl2_tbl_11257 where a=1 and b=2 and c=3;
select * from qp_misc_jiras.tbl2_tbl_11257;
insert into qp_misc_jiras.tbl2_tbl_11257 values(1,2,3); 
select relname, distkey as distribution_attributes from gp_distribution_policy p, pg_class c where p.localoid = c.oid and relname like 'tbl2_tbl_11257%';

-- start_ignore
alter table qp_misc_jiras.tbl2_tbl_11257 split default partition
start (3)
end (4)
into (partition p2, partition default_partition);
-- end_ignore
select relname, distkey as distribution_attributes from gp_distribution_policy p, pg_class c where p.localoid = c.oid and relname like 'tbl2_tbl_11257%';

delete from qp_misc_jiras.tbl2_tbl_11257 where a=1 and b=2 and c=3;
select * from qp_misc_jiras.tbl2_tbl_11257;

drop table qp_misc_jiras.tbl1_tbl_11257;
drop table qp_misc_jiras.tbl2_tbl_11257;
set enable_seqscan=off;
--
-- Heap table. TidScan should be used.
--
create table qp_misc_jiras.test_heap (i int, j int);
insert into qp_misc_jiras.test_heap values (0, 0);
explain select  * from qp_misc_jiras.test_heap where ctid='(0,1)' and gp_segment_id >= 0;
select  * from qp_misc_jiras.test_heap where ctid='(0,1)' and gp_segment_id >= 0;

--
-- AO table. TidScan should not be used.
--
create table qp_misc_jiras.test_ao (i int, j int) with (appendonly=true);
insert into qp_misc_jiras.test_ao values (0, 0);
explain select  * from qp_misc_jiras.test_ao where ctid='(33554432,2)' and gp_segment_id >= 0;
select  * from qp_misc_jiras.test_ao where ctid='(33554432,2)' and gp_segment_id >= 0;

--
-- CO table. TidScan should not be used.
--
create table qp_misc_jiras.test_co (i int, j int) with (appendonly=true, orientation=column);
insert into qp_misc_jiras.test_co values (0, 0);
explain select  * from qp_misc_jiras.test_co where ctid='(33554432,2)' and gp_segment_id >= 0;
select  * from qp_misc_jiras.test_co where ctid='(33554432,2)' and gp_segment_id >= 0;

-- This is to verify MPP-10856: test gp_enable_explain_allstat
set gp_enable_explain_allstat=on;
insert into qp_misc_jiras.test_heap select i, i from generate_series(0, 99999) i;
explain analyze select count(*) from qp_misc_jiras.test_heap;

-- This is to verify MPP-8946
-- ramans2 : Modifying queries to add filter on schema name to remove diffs in multi-node cdbfast runs
create schema schema1;
create schema schema2;
create schema schema3;
create table schema1.foo1 (i int) partition by range(i)  (start(1) end(2) every(1));
create table schema2.foo2 (i int) partition by range(i)  (start(1) end(2) every(1));
create table schema2.foo3 (i int) partition by range(i)  (start(1) end(2) every(1));
create table schema3.foo4 (i int) partition by range(i)  (start(1) end(2) every(1));
set statement_mem='1900MB';
select * from pg_partitions where schemaname in ('schema1', 'schema2', 'schema3');

--
-- Case 1: query from MPP-7073.
-- expected 0 rows
--
select * from pg_partitions p 
   where not exists (select 1 from pg_partitions p2 where p2.schemaname = p.schemaname) and p.schemaname in ('schema1', 'schema2', 'schema3'); 

--
-- Case 2: modified form of Case 1 which returns non-zero results
-- expected 2 rows: (foo1 and foo4)
--
select * from pg_partitions p 
   where not exists (select 1 from pg_partitions p2 where p2.schemaname = p.schemaname and p2.tablename != p.tablename ) and p.schemaname in ('schema1', 'schema2', 'schema3'); 

--
-- Case 3: query from MPP-8946.
--
select schemaname, tablename, partitionschemaname,
       partitiontablename, partitionlevel 
  from pg_partitions p1 
  where partitionlevel > 0 
    and partitionlevel < (select max(partitionlevel)
                          from pg_partitions p2 where p2.tablename=p1.tablename)  and schemaname in ('schema1', 'schema2', 'schema3'); 

drop schema schema1 cascade;
drop schema schema2 cascade;
drop schema schema3 cascade;

-- This is the to verify MPP-9167:
--	Gather Motion shows wrong row estimate compared to 3.3
-- 	from Lyublena: 
	/*
 	I believe the correct number of total rows output by explain should be on  a per segment basis.

	For example, a sequential scan on 2 segments with 500 rows each should
	show 500. For Motion nodes, the number is based on the number of segments
	that send data in, and you might see a difference in the output of explain
	analyze that shows the number of rows sent out.
	
	For example in the last plan below, for the redistribute motion node you
	see "rows=500" and "Rows out:  Avg 500.0 rows x 2 workers". For the gather
	motion you get "rows=500" and "Rows out:  1000 rows at destination". That
	output should be fine.
	*/

create table qp_misc_jiras.r(a int, b int);
insert into qp_misc_jiras.r select generate_series(1,1000), generate_series(1,1000);
create table qp_misc_jiras.s(a int, b int);
insert into qp_misc_jiras.s select generate_series(1,1000), generate_series(1,1000);
analyze qp_misc_jiras.r;
analyze qp_misc_jiras.s;
explain select * from qp_misc_jiras.r;
explain analyze select * from qp_misc_jiras.r,qp_misc_jiras.s where r.a=s.b;
drop table qp_misc_jiras.r;
drop table qp_misc_jiras.s;

--to_date had issues when there were padding(space) in front of the string

select to_date ('20010101','YYYYMMDD');

select to_date (' 20010101','YYYYMMDD');

select to_date ('    20010101','YYYYMMDD');

select to_date(to_char( 20010101, '99999999'), 'YYYYMMDD');

select to_date('20000-1130', 'YYYY-MMDD');

select to_date('  20000-1130', 'YYYY-MMDD');

select to_date(' 2011-01-20','YYYY-MM-DD HH24:MI:SS');

SELECT to_date('20110521', 'YYYYMMDD'), to_date(' 20110521', 'YYYYMMDD'), to_date(' 20110521', ' YYYYMMDD');

select to_timestamp('2011-01-20','YYYY-MM-DD HH24:MI:SS');

select to_timestamp(' 2011-01-20','YYYY-MM-DD HH24:MI:SS');

select to_timestamp('    2011-01-20','YYYY-MM-DD HH24:MI:SS');

set TIMEZONE TO 'America/Los_Angeles';

select to_timestamp('15:12:02.020.001230', 'HH:MI:SS.MS.US');

--The above warning is due to change in postgres after 3.2
-- To avoid the warning use HH24 format instead of HH

select to_timestamp('15:12:02.020.001230', 'HH24:MI:SS.MS.US');

reset TIMEZONE;

create table qp_misc_jiras.tbl13409_test (i int, j int);

--Hangs when given an invalid value for reorganize
alter table qp_misc_jiras.tbl13409_test set with (reorganize=foo) distributed by (j);

--Invalid integer value. def->arg
alter table qp_misc_jiras.tbl13409_test set with (reorganize=123) distributed by (j);
alter table qp_misc_jiras.tbl13409_test set with (reorganize="true");
alter table qp_misc_jiras.tbl13409_test set with (reorganize="TRUE");
alter table qp_misc_jiras.tbl13409_test set with (reorganize="FALSE");
alter table qp_misc_jiras.tbl13409_test set with (reorganize="false");

--Valid strings
alter table qp_misc_jiras.tbl13409_test set with (reorganize=true);
alter table qp_misc_jiras.tbl13409_test set with (reorganize=TRUE);
alter table qp_misc_jiras.tbl13409_test set with (reorganize=FALSE);
alter table qp_misc_jiras.tbl13409_test set with (reorganize=false);
alter table qp_misc_jiras.tbl13409_test set with (reorganize='false');
alter table qp_misc_jiras.tbl13409_test set with (reorganize='FALSE');
alter table qp_misc_jiras.tbl13409_test set with (reorganize='TRUE');
alter table qp_misc_jiras.tbl13409_test set with (reorganize='true');

create table qp_misc_jiras.tbl13879_1 (a int) distributed by (a);
insert into qp_misc_jiras.tbl13879_1 select generate_series(1,10);
select * from qp_misc_jiras.tbl13879_1;
select a, max(a) over (order by a range between current row and 2 following) as max from qp_misc_jiras.tbl13879_1;
create table qp_misc_jiras.tbl13879_2 (a numeric) distributed by (a);
create table qp_misc_jiras.tbl13879_2 (a numeric) distributed by (a);
insert into qp_misc_jiras.tbl13879_2 select generate_series(1,10);
select * from qp_misc_jiras.tbl13879_2;
select a, max(a) over (order by a range between current row and 2 following) as max from qp_misc_jiras.tbl13879_2;
drop table qp_misc_jiras.tbl13879_1;
drop table qp_misc_jiras.tbl13879_2;

create table qp_misc_jiras.esc176_1 (id integer, seq integer, val double precision, clickdate timestamp without time zone) distributed by (id);
insert into qp_misc_jiras.esc176_1 values (1,1,0.2,CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 values (1,2,0.1,CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 values (1,3,0.5,CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 values (1,4,0.7,CURRENT_TIMESTAMP);
select id, seq, sum (val) over (partition by id order by clickdate range between interval '0 seconds' following and interval '1000 seconds' following) from qp_misc_jiras.esc176_1;
select id, seq, sum (val) over (partition by id order by clickdate range between interval '0 seconds' preceding and interval '1000 seconds' following) from qp_misc_jiras.esc176_1;
select id, seq, sum(val) over (partition by id order by seq::numeric range between 0 following and 10 following), val from qp_misc_jiras.esc176_1;
select id, seq, sum(val) over (partition by id order by seq::numeric range between 10 preceding and 0 preceding), val from qp_misc_jiras.esc176_1;

insert into qp_misc_jiras.esc176_1 values (1,9,0.3,CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 values (1,10,0.4,CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 values (1,11,0.6, CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 select * from qp_misc_jiras.esc176_1;
insert into qp_misc_jiras.esc176_1 select * from qp_misc_jiras.esc176_1;
insert into qp_misc_jiras.esc176_1 select * from qp_misc_jiras.esc176_1;
insert into qp_misc_jiras.esc176_1 values (1,9,0.3, CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 values (1,10,0.4,CURRENT_TIMESTAMP);
insert into qp_misc_jiras.esc176_1 values (1,11,0.6,CURRENT_TIMESTAMP);
select id, seq, sum (val) over (partition by id order by clickdate range between interval '0 seconds' following and interval '1000 seconds' following) from qp_misc_jiras.esc176_1;
select id, seq, sum (val) over (partition by id order by clickdate range between interval '0 seconds' preceding and interval '1000 seconds' following) from qp_misc_jiras.esc176_1;
select id, seq, sum(val) over (partition by id order by seq::numeric range between 0 following and 10 following), val from qp_misc_jiras.esc176_1;
select id, seq, sum(val) over (partition by id order by seq::numeric range between 10 preceding and 0 preceding), val from qp_misc_jiras.esc176_1;
drop table qp_misc_jiras.esc176_1;

create table qp_misc_jiras.tbl13491_h(a int,str varchar)distributed by (a);
alter table qp_misc_jiras.tbl13491_h alter column str set storage external;
insert into qp_misc_jiras.tbl13491_h values (1, lpad('a', 100000, 'b'));
create table qp_misc_jiras.tbl13491_aocol with (appendonly=true, orientation=column) as select * from qp_misc_jiras.tbl13491_h distributed by (a);
truncate table qp_misc_jiras.tbl13491_h;
\t off
select str = lpad('a', 100000, 'b') from qp_misc_jiras.tbl13491_aocol;
drop table qp_misc_jiras.tbl13491_h;
select str = lpad('a', 100000, 'b') from qp_misc_jiras.tbl13491_aocol;
drop table qp_misc_jiras.tbl13491_aocol;

--
-- Test that rules with functions can be serialized correctly
-- This is tested here because the rules tests are not enabled
--
create table  qp_misc_jiras.rules (a integer);
create rule "_RETURN" as on select to qp_misc_jiras.rules do instead
  select * from generate_series(1,5) x(a);

--
-- Test gp_enable_relsize_collection's effect on ORCA plan generation
--
create table tbl_z(x int) distributed by (x);
set optimizer_metadata_caching to off;
insert into tbl_z select i from generate_series(1,100) i;

-- plan with no relsize collection
explain select 1 as t1 where 1 <= ALL (select x from tbl_z);
set gp_enable_relsize_collection = on;
-- plan with relsize collection
explain select 1 as t1 where 1 <= ALL (select x from tbl_z);

drop table if exists tbl_z;
reset optimizer_metadata_caching;
reset gp_enable_relsize_collection;

-- orca should estimate more than 1 row
-- even for real small frequency

create table epsilon_test (b smallint);
set allow_system_table_mods=on;

UPDATE pg_class
SET
        relpages = 4901842::int,
        reltuples = 495454000.0::real
WHERE relname = 'epsilon_test';

INSERT INTO pg_statistic VALUES (
        'epsilon_test'::regclass,
        1::smallint,
	False::boolean,
        0.0::real,
        2::integer,
        10.0::real,
        1::smallint,
        2::smallint,
        0::smallint,
        0::smallint,
	0::smallint,
        94::oid,
        95::oid,
        0::oid,
        0::oid,
	0::oid,
        E'{0.259358,0.15047,0.124118,0.117294,0.117195,0.11612,0.115285}'::real[],
        NULL::real[],
        NULL::real[],
        NULL::real[],
	NULL::real[],
        E'{25,7,143,6,107,10,21}'::int2[],
        E'{0,30}'::int2[],
        NULL::int2[],
        NULL::int2[],
	NULL::anyarray);

explain select b from epsilon_test where b in (11,30) limit 30;

-- start_ignore
drop schema qp_misc_jiras cascade;
-- end_ignore
