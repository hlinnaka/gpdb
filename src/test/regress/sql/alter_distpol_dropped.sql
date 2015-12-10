-- MPP-5928
-- We want to test a whole bunch of different type configurations and whether
-- ATSDB can handle them as dropped types
-- Here's the script to generate all this:
/*
#!/bin/bash
align="int2 int4 char double"
length="variable 1 3 4 11 17 19 23 32 196"
pbv="true false"
storage="true false"

# First create all the datatypes and test tables to test with. We do these in one large
# transaction, to keep transaction overhead to minimum.
echo "BEGIN;"
echo "CREATE SCHEMA adp_dropped;"
echo "set search_path='adp_dropped';"
for a in $align
do
	for l in $length
	do
		for p in $pbv
		do
			for s in $storage
			do
				if [ $p == "true" ] && [ $l != "variable" ] && [ $l -gt 8 ];
				then
					continue
				fi
				if [ $p == "true" ] && [ $l == "variable" ];
				then
					continue
				fi

				typesuffix="${a}_${l}_${p}_${s}"

				cat <<EOF
create type break_${typesuffix};
create function breakin_${typesuffix} (cstring) returns break_${typesuffix} as 'textin' language internal;
create function breakout_${typesuffix} (break_${typesuffix}) returns cstring as 'textout' language internal;

create type break_${typesuffix} (input = breakin_${typesuffix}, output = breakout_${typesuffix}, internallength = $l, passedbyvalue = $p, alignment = $a);
create table alter_distpol_g_${typesuffix} (i int, j break_${typesuffix}, k text) with (appendonly = $s) distributed by (i);
insert into alter_distpol_g_${typesuffix} (i, k) select i, i from generate_series(1, 10) i;
EOF
			done
		done
	done
done
echo "COMMIT;"

for a in $align
do
	for l in $length
	do
		for p in $pbv
		do
			for s in $storage
			do
				if [ $p == "true" ] && [ $l != "variable" ] && [ $l -gt 8 ];
				then
					continue
				fi
				if [ $p == "true" ] && [ $l == "variable" ];
				then
					continue
				fi

				typesuffix="${a}_${l}_${p}_${s}"

				cat <<EOF
alter table alter_distpol_g_${typesuffix} drop column j;
select * from alter_distpol_g_${typesuffix} order by 1;
alter table alter_distpol_g_${typesuffix} set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_${typesuffix} order by 1;
drop type break_${typesuffix} cascade;
alter table alter_distpol_g_${typesuffix} set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_${typesuffix} order by 1;
EOF
			done
		done
	done
done
*/

-- Generated part begins here
BEGIN;
CREATE SCHEMA adp_dropped;
set search_path='adp_dropped';
create type break_int2_variable_false_true;
create function breakin_int2_variable_false_true (cstring) returns break_int2_variable_false_true as 'textin' language internal;
create function breakout_int2_variable_false_true (break_int2_variable_false_true) returns cstring as 'textout' language internal;

create type break_int2_variable_false_true (input = breakin_int2_variable_false_true, output = breakout_int2_variable_false_true, internallength = variable, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_variable_false_true (i int, j break_int2_variable_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_variable_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_variable_false_false;
create function breakin_int2_variable_false_false (cstring) returns break_int2_variable_false_false as 'textin' language internal;
create function breakout_int2_variable_false_false (break_int2_variable_false_false) returns cstring as 'textout' language internal;

create type break_int2_variable_false_false (input = breakin_int2_variable_false_false, output = breakout_int2_variable_false_false, internallength = variable, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_variable_false_false (i int, j break_int2_variable_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_variable_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_1_true_true;
create function breakin_int2_1_true_true (cstring) returns break_int2_1_true_true as 'textin' language internal;
create function breakout_int2_1_true_true (break_int2_1_true_true) returns cstring as 'textout' language internal;

create type break_int2_1_true_true (input = breakin_int2_1_true_true, output = breakout_int2_1_true_true, internallength = 1, passedbyvalue = true, alignment = int2);
create table alter_distpol_g_int2_1_true_true (i int, j break_int2_1_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_1_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_1_true_false;
create function breakin_int2_1_true_false (cstring) returns break_int2_1_true_false as 'textin' language internal;
create function breakout_int2_1_true_false (break_int2_1_true_false) returns cstring as 'textout' language internal;

create type break_int2_1_true_false (input = breakin_int2_1_true_false, output = breakout_int2_1_true_false, internallength = 1, passedbyvalue = true, alignment = int2);
create table alter_distpol_g_int2_1_true_false (i int, j break_int2_1_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_1_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_1_false_true;
create function breakin_int2_1_false_true (cstring) returns break_int2_1_false_true as 'textin' language internal;
create function breakout_int2_1_false_true (break_int2_1_false_true) returns cstring as 'textout' language internal;

create type break_int2_1_false_true (input = breakin_int2_1_false_true, output = breakout_int2_1_false_true, internallength = 1, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_1_false_true (i int, j break_int2_1_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_1_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_1_false_false;
create function breakin_int2_1_false_false (cstring) returns break_int2_1_false_false as 'textin' language internal;
create function breakout_int2_1_false_false (break_int2_1_false_false) returns cstring as 'textout' language internal;

create type break_int2_1_false_false (input = breakin_int2_1_false_false, output = breakout_int2_1_false_false, internallength = 1, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_1_false_false (i int, j break_int2_1_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_1_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_3_true_true;
create function breakin_int2_3_true_true (cstring) returns break_int2_3_true_true as 'textin' language internal;
create function breakout_int2_3_true_true (break_int2_3_true_true) returns cstring as 'textout' language internal;

create type break_int2_3_true_true (input = breakin_int2_3_true_true, output = breakout_int2_3_true_true, internallength = 3, passedbyvalue = true, alignment = int2);
create table alter_distpol_g_int2_3_true_true (i int, j break_int2_3_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_3_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_3_true_false;
create function breakin_int2_3_true_false (cstring) returns break_int2_3_true_false as 'textin' language internal;
create function breakout_int2_3_true_false (break_int2_3_true_false) returns cstring as 'textout' language internal;

create type break_int2_3_true_false (input = breakin_int2_3_true_false, output = breakout_int2_3_true_false, internallength = 3, passedbyvalue = true, alignment = int2);
create table alter_distpol_g_int2_3_true_false (i int, j break_int2_3_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_3_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_3_false_true;
create function breakin_int2_3_false_true (cstring) returns break_int2_3_false_true as 'textin' language internal;
create function breakout_int2_3_false_true (break_int2_3_false_true) returns cstring as 'textout' language internal;

create type break_int2_3_false_true (input = breakin_int2_3_false_true, output = breakout_int2_3_false_true, internallength = 3, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_3_false_true (i int, j break_int2_3_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_3_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_3_false_false;
create function breakin_int2_3_false_false (cstring) returns break_int2_3_false_false as 'textin' language internal;
create function breakout_int2_3_false_false (break_int2_3_false_false) returns cstring as 'textout' language internal;

create type break_int2_3_false_false (input = breakin_int2_3_false_false, output = breakout_int2_3_false_false, internallength = 3, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_3_false_false (i int, j break_int2_3_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_3_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_4_true_true;
create function breakin_int2_4_true_true (cstring) returns break_int2_4_true_true as 'textin' language internal;
create function breakout_int2_4_true_true (break_int2_4_true_true) returns cstring as 'textout' language internal;

create type break_int2_4_true_true (input = breakin_int2_4_true_true, output = breakout_int2_4_true_true, internallength = 4, passedbyvalue = true, alignment = int2);
create table alter_distpol_g_int2_4_true_true (i int, j break_int2_4_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_4_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_4_true_false;
create function breakin_int2_4_true_false (cstring) returns break_int2_4_true_false as 'textin' language internal;
create function breakout_int2_4_true_false (break_int2_4_true_false) returns cstring as 'textout' language internal;

create type break_int2_4_true_false (input = breakin_int2_4_true_false, output = breakout_int2_4_true_false, internallength = 4, passedbyvalue = true, alignment = int2);
create table alter_distpol_g_int2_4_true_false (i int, j break_int2_4_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_4_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_4_false_true;
create function breakin_int2_4_false_true (cstring) returns break_int2_4_false_true as 'textin' language internal;
create function breakout_int2_4_false_true (break_int2_4_false_true) returns cstring as 'textout' language internal;

create type break_int2_4_false_true (input = breakin_int2_4_false_true, output = breakout_int2_4_false_true, internallength = 4, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_4_false_true (i int, j break_int2_4_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_4_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_4_false_false;
create function breakin_int2_4_false_false (cstring) returns break_int2_4_false_false as 'textin' language internal;
create function breakout_int2_4_false_false (break_int2_4_false_false) returns cstring as 'textout' language internal;

create type break_int2_4_false_false (input = breakin_int2_4_false_false, output = breakout_int2_4_false_false, internallength = 4, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_4_false_false (i int, j break_int2_4_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_4_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_11_false_true;
create function breakin_int2_11_false_true (cstring) returns break_int2_11_false_true as 'textin' language internal;
create function breakout_int2_11_false_true (break_int2_11_false_true) returns cstring as 'textout' language internal;

create type break_int2_11_false_true (input = breakin_int2_11_false_true, output = breakout_int2_11_false_true, internallength = 11, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_11_false_true (i int, j break_int2_11_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_11_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_11_false_false;
create function breakin_int2_11_false_false (cstring) returns break_int2_11_false_false as 'textin' language internal;
create function breakout_int2_11_false_false (break_int2_11_false_false) returns cstring as 'textout' language internal;

create type break_int2_11_false_false (input = breakin_int2_11_false_false, output = breakout_int2_11_false_false, internallength = 11, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_11_false_false (i int, j break_int2_11_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_11_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_17_false_true;
create function breakin_int2_17_false_true (cstring) returns break_int2_17_false_true as 'textin' language internal;
create function breakout_int2_17_false_true (break_int2_17_false_true) returns cstring as 'textout' language internal;

create type break_int2_17_false_true (input = breakin_int2_17_false_true, output = breakout_int2_17_false_true, internallength = 17, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_17_false_true (i int, j break_int2_17_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_17_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_17_false_false;
create function breakin_int2_17_false_false (cstring) returns break_int2_17_false_false as 'textin' language internal;
create function breakout_int2_17_false_false (break_int2_17_false_false) returns cstring as 'textout' language internal;

create type break_int2_17_false_false (input = breakin_int2_17_false_false, output = breakout_int2_17_false_false, internallength = 17, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_17_false_false (i int, j break_int2_17_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_17_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_19_false_true;
create function breakin_int2_19_false_true (cstring) returns break_int2_19_false_true as 'textin' language internal;
create function breakout_int2_19_false_true (break_int2_19_false_true) returns cstring as 'textout' language internal;

create type break_int2_19_false_true (input = breakin_int2_19_false_true, output = breakout_int2_19_false_true, internallength = 19, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_19_false_true (i int, j break_int2_19_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_19_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_19_false_false;
create function breakin_int2_19_false_false (cstring) returns break_int2_19_false_false as 'textin' language internal;
create function breakout_int2_19_false_false (break_int2_19_false_false) returns cstring as 'textout' language internal;

create type break_int2_19_false_false (input = breakin_int2_19_false_false, output = breakout_int2_19_false_false, internallength = 19, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_19_false_false (i int, j break_int2_19_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_19_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_23_false_true;
create function breakin_int2_23_false_true (cstring) returns break_int2_23_false_true as 'textin' language internal;
create function breakout_int2_23_false_true (break_int2_23_false_true) returns cstring as 'textout' language internal;

create type break_int2_23_false_true (input = breakin_int2_23_false_true, output = breakout_int2_23_false_true, internallength = 23, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_23_false_true (i int, j break_int2_23_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_23_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_23_false_false;
create function breakin_int2_23_false_false (cstring) returns break_int2_23_false_false as 'textin' language internal;
create function breakout_int2_23_false_false (break_int2_23_false_false) returns cstring as 'textout' language internal;

create type break_int2_23_false_false (input = breakin_int2_23_false_false, output = breakout_int2_23_false_false, internallength = 23, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_23_false_false (i int, j break_int2_23_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_23_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_32_false_true;
create function breakin_int2_32_false_true (cstring) returns break_int2_32_false_true as 'textin' language internal;
create function breakout_int2_32_false_true (break_int2_32_false_true) returns cstring as 'textout' language internal;

create type break_int2_32_false_true (input = breakin_int2_32_false_true, output = breakout_int2_32_false_true, internallength = 32, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_32_false_true (i int, j break_int2_32_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_32_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_32_false_false;
create function breakin_int2_32_false_false (cstring) returns break_int2_32_false_false as 'textin' language internal;
create function breakout_int2_32_false_false (break_int2_32_false_false) returns cstring as 'textout' language internal;

create type break_int2_32_false_false (input = breakin_int2_32_false_false, output = breakout_int2_32_false_false, internallength = 32, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_32_false_false (i int, j break_int2_32_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_32_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_196_false_true;
create function breakin_int2_196_false_true (cstring) returns break_int2_196_false_true as 'textin' language internal;
create function breakout_int2_196_false_true (break_int2_196_false_true) returns cstring as 'textout' language internal;

create type break_int2_196_false_true (input = breakin_int2_196_false_true, output = breakout_int2_196_false_true, internallength = 196, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_196_false_true (i int, j break_int2_196_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int2_196_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int2_196_false_false;
create function breakin_int2_196_false_false (cstring) returns break_int2_196_false_false as 'textin' language internal;
create function breakout_int2_196_false_false (break_int2_196_false_false) returns cstring as 'textout' language internal;

create type break_int2_196_false_false (input = breakin_int2_196_false_false, output = breakout_int2_196_false_false, internallength = 196, passedbyvalue = false, alignment = int2);
create table alter_distpol_g_int2_196_false_false (i int, j break_int2_196_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int2_196_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_variable_false_true;
create function breakin_int4_variable_false_true (cstring) returns break_int4_variable_false_true as 'textin' language internal;
create function breakout_int4_variable_false_true (break_int4_variable_false_true) returns cstring as 'textout' language internal;

create type break_int4_variable_false_true (input = breakin_int4_variable_false_true, output = breakout_int4_variable_false_true, internallength = variable, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_variable_false_true (i int, j break_int4_variable_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_variable_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_variable_false_false;
create function breakin_int4_variable_false_false (cstring) returns break_int4_variable_false_false as 'textin' language internal;
create function breakout_int4_variable_false_false (break_int4_variable_false_false) returns cstring as 'textout' language internal;

create type break_int4_variable_false_false (input = breakin_int4_variable_false_false, output = breakout_int4_variable_false_false, internallength = variable, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_variable_false_false (i int, j break_int4_variable_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_variable_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_1_true_true;
create function breakin_int4_1_true_true (cstring) returns break_int4_1_true_true as 'textin' language internal;
create function breakout_int4_1_true_true (break_int4_1_true_true) returns cstring as 'textout' language internal;

create type break_int4_1_true_true (input = breakin_int4_1_true_true, output = breakout_int4_1_true_true, internallength = 1, passedbyvalue = true, alignment = int4);
create table alter_distpol_g_int4_1_true_true (i int, j break_int4_1_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_1_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_1_true_false;
create function breakin_int4_1_true_false (cstring) returns break_int4_1_true_false as 'textin' language internal;
create function breakout_int4_1_true_false (break_int4_1_true_false) returns cstring as 'textout' language internal;

create type break_int4_1_true_false (input = breakin_int4_1_true_false, output = breakout_int4_1_true_false, internallength = 1, passedbyvalue = true, alignment = int4);
create table alter_distpol_g_int4_1_true_false (i int, j break_int4_1_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_1_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_1_false_true;
create function breakin_int4_1_false_true (cstring) returns break_int4_1_false_true as 'textin' language internal;
create function breakout_int4_1_false_true (break_int4_1_false_true) returns cstring as 'textout' language internal;

create type break_int4_1_false_true (input = breakin_int4_1_false_true, output = breakout_int4_1_false_true, internallength = 1, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_1_false_true (i int, j break_int4_1_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_1_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_1_false_false;
create function breakin_int4_1_false_false (cstring) returns break_int4_1_false_false as 'textin' language internal;
create function breakout_int4_1_false_false (break_int4_1_false_false) returns cstring as 'textout' language internal;

create type break_int4_1_false_false (input = breakin_int4_1_false_false, output = breakout_int4_1_false_false, internallength = 1, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_1_false_false (i int, j break_int4_1_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_1_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_3_true_true;
create function breakin_int4_3_true_true (cstring) returns break_int4_3_true_true as 'textin' language internal;
create function breakout_int4_3_true_true (break_int4_3_true_true) returns cstring as 'textout' language internal;

create type break_int4_3_true_true (input = breakin_int4_3_true_true, output = breakout_int4_3_true_true, internallength = 3, passedbyvalue = true, alignment = int4);
create table alter_distpol_g_int4_3_true_true (i int, j break_int4_3_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_3_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_3_true_false;
create function breakin_int4_3_true_false (cstring) returns break_int4_3_true_false as 'textin' language internal;
create function breakout_int4_3_true_false (break_int4_3_true_false) returns cstring as 'textout' language internal;

create type break_int4_3_true_false (input = breakin_int4_3_true_false, output = breakout_int4_3_true_false, internallength = 3, passedbyvalue = true, alignment = int4);
create table alter_distpol_g_int4_3_true_false (i int, j break_int4_3_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_3_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_3_false_true;
create function breakin_int4_3_false_true (cstring) returns break_int4_3_false_true as 'textin' language internal;
create function breakout_int4_3_false_true (break_int4_3_false_true) returns cstring as 'textout' language internal;

create type break_int4_3_false_true (input = breakin_int4_3_false_true, output = breakout_int4_3_false_true, internallength = 3, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_3_false_true (i int, j break_int4_3_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_3_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_3_false_false;
create function breakin_int4_3_false_false (cstring) returns break_int4_3_false_false as 'textin' language internal;
create function breakout_int4_3_false_false (break_int4_3_false_false) returns cstring as 'textout' language internal;

create type break_int4_3_false_false (input = breakin_int4_3_false_false, output = breakout_int4_3_false_false, internallength = 3, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_3_false_false (i int, j break_int4_3_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_3_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_4_true_true;
create function breakin_int4_4_true_true (cstring) returns break_int4_4_true_true as 'textin' language internal;
create function breakout_int4_4_true_true (break_int4_4_true_true) returns cstring as 'textout' language internal;

create type break_int4_4_true_true (input = breakin_int4_4_true_true, output = breakout_int4_4_true_true, internallength = 4, passedbyvalue = true, alignment = int4);
create table alter_distpol_g_int4_4_true_true (i int, j break_int4_4_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_4_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_4_true_false;
create function breakin_int4_4_true_false (cstring) returns break_int4_4_true_false as 'textin' language internal;
create function breakout_int4_4_true_false (break_int4_4_true_false) returns cstring as 'textout' language internal;

create type break_int4_4_true_false (input = breakin_int4_4_true_false, output = breakout_int4_4_true_false, internallength = 4, passedbyvalue = true, alignment = int4);
create table alter_distpol_g_int4_4_true_false (i int, j break_int4_4_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_4_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_4_false_true;
create function breakin_int4_4_false_true (cstring) returns break_int4_4_false_true as 'textin' language internal;
create function breakout_int4_4_false_true (break_int4_4_false_true) returns cstring as 'textout' language internal;

create type break_int4_4_false_true (input = breakin_int4_4_false_true, output = breakout_int4_4_false_true, internallength = 4, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_4_false_true (i int, j break_int4_4_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_4_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_4_false_false;
create function breakin_int4_4_false_false (cstring) returns break_int4_4_false_false as 'textin' language internal;
create function breakout_int4_4_false_false (break_int4_4_false_false) returns cstring as 'textout' language internal;

create type break_int4_4_false_false (input = breakin_int4_4_false_false, output = breakout_int4_4_false_false, internallength = 4, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_4_false_false (i int, j break_int4_4_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_4_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_11_false_true;
create function breakin_int4_11_false_true (cstring) returns break_int4_11_false_true as 'textin' language internal;
create function breakout_int4_11_false_true (break_int4_11_false_true) returns cstring as 'textout' language internal;

create type break_int4_11_false_true (input = breakin_int4_11_false_true, output = breakout_int4_11_false_true, internallength = 11, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_11_false_true (i int, j break_int4_11_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_11_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_11_false_false;
create function breakin_int4_11_false_false (cstring) returns break_int4_11_false_false as 'textin' language internal;
create function breakout_int4_11_false_false (break_int4_11_false_false) returns cstring as 'textout' language internal;

create type break_int4_11_false_false (input = breakin_int4_11_false_false, output = breakout_int4_11_false_false, internallength = 11, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_11_false_false (i int, j break_int4_11_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_11_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_17_false_true;
create function breakin_int4_17_false_true (cstring) returns break_int4_17_false_true as 'textin' language internal;
create function breakout_int4_17_false_true (break_int4_17_false_true) returns cstring as 'textout' language internal;

create type break_int4_17_false_true (input = breakin_int4_17_false_true, output = breakout_int4_17_false_true, internallength = 17, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_17_false_true (i int, j break_int4_17_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_17_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_17_false_false;
create function breakin_int4_17_false_false (cstring) returns break_int4_17_false_false as 'textin' language internal;
create function breakout_int4_17_false_false (break_int4_17_false_false) returns cstring as 'textout' language internal;

create type break_int4_17_false_false (input = breakin_int4_17_false_false, output = breakout_int4_17_false_false, internallength = 17, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_17_false_false (i int, j break_int4_17_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_17_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_19_false_true;
create function breakin_int4_19_false_true (cstring) returns break_int4_19_false_true as 'textin' language internal;
create function breakout_int4_19_false_true (break_int4_19_false_true) returns cstring as 'textout' language internal;

create type break_int4_19_false_true (input = breakin_int4_19_false_true, output = breakout_int4_19_false_true, internallength = 19, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_19_false_true (i int, j break_int4_19_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_19_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_19_false_false;
create function breakin_int4_19_false_false (cstring) returns break_int4_19_false_false as 'textin' language internal;
create function breakout_int4_19_false_false (break_int4_19_false_false) returns cstring as 'textout' language internal;

create type break_int4_19_false_false (input = breakin_int4_19_false_false, output = breakout_int4_19_false_false, internallength = 19, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_19_false_false (i int, j break_int4_19_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_19_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_23_false_true;
create function breakin_int4_23_false_true (cstring) returns break_int4_23_false_true as 'textin' language internal;
create function breakout_int4_23_false_true (break_int4_23_false_true) returns cstring as 'textout' language internal;

create type break_int4_23_false_true (input = breakin_int4_23_false_true, output = breakout_int4_23_false_true, internallength = 23, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_23_false_true (i int, j break_int4_23_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_23_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_23_false_false;
create function breakin_int4_23_false_false (cstring) returns break_int4_23_false_false as 'textin' language internal;
create function breakout_int4_23_false_false (break_int4_23_false_false) returns cstring as 'textout' language internal;

create type break_int4_23_false_false (input = breakin_int4_23_false_false, output = breakout_int4_23_false_false, internallength = 23, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_23_false_false (i int, j break_int4_23_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_23_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_32_false_true;
create function breakin_int4_32_false_true (cstring) returns break_int4_32_false_true as 'textin' language internal;
create function breakout_int4_32_false_true (break_int4_32_false_true) returns cstring as 'textout' language internal;

create type break_int4_32_false_true (input = breakin_int4_32_false_true, output = breakout_int4_32_false_true, internallength = 32, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_32_false_true (i int, j break_int4_32_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_32_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_32_false_false;
create function breakin_int4_32_false_false (cstring) returns break_int4_32_false_false as 'textin' language internal;
create function breakout_int4_32_false_false (break_int4_32_false_false) returns cstring as 'textout' language internal;

create type break_int4_32_false_false (input = breakin_int4_32_false_false, output = breakout_int4_32_false_false, internallength = 32, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_32_false_false (i int, j break_int4_32_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_32_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_196_false_true;
create function breakin_int4_196_false_true (cstring) returns break_int4_196_false_true as 'textin' language internal;
create function breakout_int4_196_false_true (break_int4_196_false_true) returns cstring as 'textout' language internal;

create type break_int4_196_false_true (input = breakin_int4_196_false_true, output = breakout_int4_196_false_true, internallength = 196, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_196_false_true (i int, j break_int4_196_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_int4_196_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_int4_196_false_false;
create function breakin_int4_196_false_false (cstring) returns break_int4_196_false_false as 'textin' language internal;
create function breakout_int4_196_false_false (break_int4_196_false_false) returns cstring as 'textout' language internal;

create type break_int4_196_false_false (input = breakin_int4_196_false_false, output = breakout_int4_196_false_false, internallength = 196, passedbyvalue = false, alignment = int4);
create table alter_distpol_g_int4_196_false_false (i int, j break_int4_196_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_int4_196_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_variable_false_true;
create function breakin_char_variable_false_true (cstring) returns break_char_variable_false_true as 'textin' language internal;
create function breakout_char_variable_false_true (break_char_variable_false_true) returns cstring as 'textout' language internal;

create type break_char_variable_false_true (input = breakin_char_variable_false_true, output = breakout_char_variable_false_true, internallength = variable, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_variable_false_true (i int, j break_char_variable_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_variable_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_variable_false_false;
create function breakin_char_variable_false_false (cstring) returns break_char_variable_false_false as 'textin' language internal;
create function breakout_char_variable_false_false (break_char_variable_false_false) returns cstring as 'textout' language internal;

create type break_char_variable_false_false (input = breakin_char_variable_false_false, output = breakout_char_variable_false_false, internallength = variable, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_variable_false_false (i int, j break_char_variable_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_variable_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_1_true_true;
create function breakin_char_1_true_true (cstring) returns break_char_1_true_true as 'textin' language internal;
create function breakout_char_1_true_true (break_char_1_true_true) returns cstring as 'textout' language internal;

create type break_char_1_true_true (input = breakin_char_1_true_true, output = breakout_char_1_true_true, internallength = 1, passedbyvalue = true, alignment = char);
create table alter_distpol_g_char_1_true_true (i int, j break_char_1_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_1_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_1_true_false;
create function breakin_char_1_true_false (cstring) returns break_char_1_true_false as 'textin' language internal;
create function breakout_char_1_true_false (break_char_1_true_false) returns cstring as 'textout' language internal;

create type break_char_1_true_false (input = breakin_char_1_true_false, output = breakout_char_1_true_false, internallength = 1, passedbyvalue = true, alignment = char);
create table alter_distpol_g_char_1_true_false (i int, j break_char_1_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_1_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_1_false_true;
create function breakin_char_1_false_true (cstring) returns break_char_1_false_true as 'textin' language internal;
create function breakout_char_1_false_true (break_char_1_false_true) returns cstring as 'textout' language internal;

create type break_char_1_false_true (input = breakin_char_1_false_true, output = breakout_char_1_false_true, internallength = 1, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_1_false_true (i int, j break_char_1_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_1_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_1_false_false;
create function breakin_char_1_false_false (cstring) returns break_char_1_false_false as 'textin' language internal;
create function breakout_char_1_false_false (break_char_1_false_false) returns cstring as 'textout' language internal;

create type break_char_1_false_false (input = breakin_char_1_false_false, output = breakout_char_1_false_false, internallength = 1, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_1_false_false (i int, j break_char_1_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_1_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_3_true_true;
create function breakin_char_3_true_true (cstring) returns break_char_3_true_true as 'textin' language internal;
create function breakout_char_3_true_true (break_char_3_true_true) returns cstring as 'textout' language internal;

create type break_char_3_true_true (input = breakin_char_3_true_true, output = breakout_char_3_true_true, internallength = 3, passedbyvalue = true, alignment = char);
create table alter_distpol_g_char_3_true_true (i int, j break_char_3_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_3_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_3_true_false;
create function breakin_char_3_true_false (cstring) returns break_char_3_true_false as 'textin' language internal;
create function breakout_char_3_true_false (break_char_3_true_false) returns cstring as 'textout' language internal;

create type break_char_3_true_false (input = breakin_char_3_true_false, output = breakout_char_3_true_false, internallength = 3, passedbyvalue = true, alignment = char);
create table alter_distpol_g_char_3_true_false (i int, j break_char_3_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_3_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_3_false_true;
create function breakin_char_3_false_true (cstring) returns break_char_3_false_true as 'textin' language internal;
create function breakout_char_3_false_true (break_char_3_false_true) returns cstring as 'textout' language internal;

create type break_char_3_false_true (input = breakin_char_3_false_true, output = breakout_char_3_false_true, internallength = 3, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_3_false_true (i int, j break_char_3_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_3_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_3_false_false;
create function breakin_char_3_false_false (cstring) returns break_char_3_false_false as 'textin' language internal;
create function breakout_char_3_false_false (break_char_3_false_false) returns cstring as 'textout' language internal;

create type break_char_3_false_false (input = breakin_char_3_false_false, output = breakout_char_3_false_false, internallength = 3, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_3_false_false (i int, j break_char_3_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_3_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_4_true_true;
create function breakin_char_4_true_true (cstring) returns break_char_4_true_true as 'textin' language internal;
create function breakout_char_4_true_true (break_char_4_true_true) returns cstring as 'textout' language internal;

create type break_char_4_true_true (input = breakin_char_4_true_true, output = breakout_char_4_true_true, internallength = 4, passedbyvalue = true, alignment = char);
create table alter_distpol_g_char_4_true_true (i int, j break_char_4_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_4_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_4_true_false;
create function breakin_char_4_true_false (cstring) returns break_char_4_true_false as 'textin' language internal;
create function breakout_char_4_true_false (break_char_4_true_false) returns cstring as 'textout' language internal;

create type break_char_4_true_false (input = breakin_char_4_true_false, output = breakout_char_4_true_false, internallength = 4, passedbyvalue = true, alignment = char);
create table alter_distpol_g_char_4_true_false (i int, j break_char_4_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_4_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_4_false_true;
create function breakin_char_4_false_true (cstring) returns break_char_4_false_true as 'textin' language internal;
create function breakout_char_4_false_true (break_char_4_false_true) returns cstring as 'textout' language internal;

create type break_char_4_false_true (input = breakin_char_4_false_true, output = breakout_char_4_false_true, internallength = 4, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_4_false_true (i int, j break_char_4_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_4_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_4_false_false;
create function breakin_char_4_false_false (cstring) returns break_char_4_false_false as 'textin' language internal;
create function breakout_char_4_false_false (break_char_4_false_false) returns cstring as 'textout' language internal;

create type break_char_4_false_false (input = breakin_char_4_false_false, output = breakout_char_4_false_false, internallength = 4, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_4_false_false (i int, j break_char_4_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_4_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_11_false_true;
create function breakin_char_11_false_true (cstring) returns break_char_11_false_true as 'textin' language internal;
create function breakout_char_11_false_true (break_char_11_false_true) returns cstring as 'textout' language internal;

create type break_char_11_false_true (input = breakin_char_11_false_true, output = breakout_char_11_false_true, internallength = 11, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_11_false_true (i int, j break_char_11_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_11_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_11_false_false;
create function breakin_char_11_false_false (cstring) returns break_char_11_false_false as 'textin' language internal;
create function breakout_char_11_false_false (break_char_11_false_false) returns cstring as 'textout' language internal;

create type break_char_11_false_false (input = breakin_char_11_false_false, output = breakout_char_11_false_false, internallength = 11, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_11_false_false (i int, j break_char_11_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_11_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_17_false_true;
create function breakin_char_17_false_true (cstring) returns break_char_17_false_true as 'textin' language internal;
create function breakout_char_17_false_true (break_char_17_false_true) returns cstring as 'textout' language internal;

create type break_char_17_false_true (input = breakin_char_17_false_true, output = breakout_char_17_false_true, internallength = 17, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_17_false_true (i int, j break_char_17_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_17_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_17_false_false;
create function breakin_char_17_false_false (cstring) returns break_char_17_false_false as 'textin' language internal;
create function breakout_char_17_false_false (break_char_17_false_false) returns cstring as 'textout' language internal;

create type break_char_17_false_false (input = breakin_char_17_false_false, output = breakout_char_17_false_false, internallength = 17, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_17_false_false (i int, j break_char_17_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_17_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_19_false_true;
create function breakin_char_19_false_true (cstring) returns break_char_19_false_true as 'textin' language internal;
create function breakout_char_19_false_true (break_char_19_false_true) returns cstring as 'textout' language internal;

create type break_char_19_false_true (input = breakin_char_19_false_true, output = breakout_char_19_false_true, internallength = 19, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_19_false_true (i int, j break_char_19_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_19_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_19_false_false;
create function breakin_char_19_false_false (cstring) returns break_char_19_false_false as 'textin' language internal;
create function breakout_char_19_false_false (break_char_19_false_false) returns cstring as 'textout' language internal;

create type break_char_19_false_false (input = breakin_char_19_false_false, output = breakout_char_19_false_false, internallength = 19, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_19_false_false (i int, j break_char_19_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_19_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_23_false_true;
create function breakin_char_23_false_true (cstring) returns break_char_23_false_true as 'textin' language internal;
create function breakout_char_23_false_true (break_char_23_false_true) returns cstring as 'textout' language internal;

create type break_char_23_false_true (input = breakin_char_23_false_true, output = breakout_char_23_false_true, internallength = 23, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_23_false_true (i int, j break_char_23_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_23_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_23_false_false;
create function breakin_char_23_false_false (cstring) returns break_char_23_false_false as 'textin' language internal;
create function breakout_char_23_false_false (break_char_23_false_false) returns cstring as 'textout' language internal;

create type break_char_23_false_false (input = breakin_char_23_false_false, output = breakout_char_23_false_false, internallength = 23, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_23_false_false (i int, j break_char_23_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_23_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_32_false_true;
create function breakin_char_32_false_true (cstring) returns break_char_32_false_true as 'textin' language internal;
create function breakout_char_32_false_true (break_char_32_false_true) returns cstring as 'textout' language internal;

create type break_char_32_false_true (input = breakin_char_32_false_true, output = breakout_char_32_false_true, internallength = 32, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_32_false_true (i int, j break_char_32_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_32_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_32_false_false;
create function breakin_char_32_false_false (cstring) returns break_char_32_false_false as 'textin' language internal;
create function breakout_char_32_false_false (break_char_32_false_false) returns cstring as 'textout' language internal;

create type break_char_32_false_false (input = breakin_char_32_false_false, output = breakout_char_32_false_false, internallength = 32, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_32_false_false (i int, j break_char_32_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_32_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_char_196_false_true;
create function breakin_char_196_false_true (cstring) returns break_char_196_false_true as 'textin' language internal;
create function breakout_char_196_false_true (break_char_196_false_true) returns cstring as 'textout' language internal;

create type break_char_196_false_true (input = breakin_char_196_false_true, output = breakout_char_196_false_true, internallength = 196, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_196_false_true (i int, j break_char_196_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_char_196_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_char_196_false_false;
create function breakin_char_196_false_false (cstring) returns break_char_196_false_false as 'textin' language internal;
create function breakout_char_196_false_false (break_char_196_false_false) returns cstring as 'textout' language internal;

create type break_char_196_false_false (input = breakin_char_196_false_false, output = breakout_char_196_false_false, internallength = 196, passedbyvalue = false, alignment = char);
create table alter_distpol_g_char_196_false_false (i int, j break_char_196_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_char_196_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_variable_false_true;
create function breakin_double_variable_false_true (cstring) returns break_double_variable_false_true as 'textin' language internal;
create function breakout_double_variable_false_true (break_double_variable_false_true) returns cstring as 'textout' language internal;

create type break_double_variable_false_true (input = breakin_double_variable_false_true, output = breakout_double_variable_false_true, internallength = variable, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_variable_false_true (i int, j break_double_variable_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_variable_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_variable_false_false;
create function breakin_double_variable_false_false (cstring) returns break_double_variable_false_false as 'textin' language internal;
create function breakout_double_variable_false_false (break_double_variable_false_false) returns cstring as 'textout' language internal;

create type break_double_variable_false_false (input = breakin_double_variable_false_false, output = breakout_double_variable_false_false, internallength = variable, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_variable_false_false (i int, j break_double_variable_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_variable_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_1_true_true;
create function breakin_double_1_true_true (cstring) returns break_double_1_true_true as 'textin' language internal;
create function breakout_double_1_true_true (break_double_1_true_true) returns cstring as 'textout' language internal;

create type break_double_1_true_true (input = breakin_double_1_true_true, output = breakout_double_1_true_true, internallength = 1, passedbyvalue = true, alignment = double);
create table alter_distpol_g_double_1_true_true (i int, j break_double_1_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_1_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_1_true_false;
create function breakin_double_1_true_false (cstring) returns break_double_1_true_false as 'textin' language internal;
create function breakout_double_1_true_false (break_double_1_true_false) returns cstring as 'textout' language internal;

create type break_double_1_true_false (input = breakin_double_1_true_false, output = breakout_double_1_true_false, internallength = 1, passedbyvalue = true, alignment = double);
create table alter_distpol_g_double_1_true_false (i int, j break_double_1_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_1_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_1_false_true;
create function breakin_double_1_false_true (cstring) returns break_double_1_false_true as 'textin' language internal;
create function breakout_double_1_false_true (break_double_1_false_true) returns cstring as 'textout' language internal;

create type break_double_1_false_true (input = breakin_double_1_false_true, output = breakout_double_1_false_true, internallength = 1, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_1_false_true (i int, j break_double_1_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_1_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_1_false_false;
create function breakin_double_1_false_false (cstring) returns break_double_1_false_false as 'textin' language internal;
create function breakout_double_1_false_false (break_double_1_false_false) returns cstring as 'textout' language internal;

create type break_double_1_false_false (input = breakin_double_1_false_false, output = breakout_double_1_false_false, internallength = 1, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_1_false_false (i int, j break_double_1_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_1_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_3_true_true;
create function breakin_double_3_true_true (cstring) returns break_double_3_true_true as 'textin' language internal;
create function breakout_double_3_true_true (break_double_3_true_true) returns cstring as 'textout' language internal;

create type break_double_3_true_true (input = breakin_double_3_true_true, output = breakout_double_3_true_true, internallength = 3, passedbyvalue = true, alignment = double);
create table alter_distpol_g_double_3_true_true (i int, j break_double_3_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_3_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_3_true_false;
create function breakin_double_3_true_false (cstring) returns break_double_3_true_false as 'textin' language internal;
create function breakout_double_3_true_false (break_double_3_true_false) returns cstring as 'textout' language internal;

create type break_double_3_true_false (input = breakin_double_3_true_false, output = breakout_double_3_true_false, internallength = 3, passedbyvalue = true, alignment = double);
create table alter_distpol_g_double_3_true_false (i int, j break_double_3_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_3_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_3_false_true;
create function breakin_double_3_false_true (cstring) returns break_double_3_false_true as 'textin' language internal;
create function breakout_double_3_false_true (break_double_3_false_true) returns cstring as 'textout' language internal;

create type break_double_3_false_true (input = breakin_double_3_false_true, output = breakout_double_3_false_true, internallength = 3, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_3_false_true (i int, j break_double_3_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_3_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_3_false_false;
create function breakin_double_3_false_false (cstring) returns break_double_3_false_false as 'textin' language internal;
create function breakout_double_3_false_false (break_double_3_false_false) returns cstring as 'textout' language internal;

create type break_double_3_false_false (input = breakin_double_3_false_false, output = breakout_double_3_false_false, internallength = 3, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_3_false_false (i int, j break_double_3_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_3_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_4_true_true;
create function breakin_double_4_true_true (cstring) returns break_double_4_true_true as 'textin' language internal;
create function breakout_double_4_true_true (break_double_4_true_true) returns cstring as 'textout' language internal;

create type break_double_4_true_true (input = breakin_double_4_true_true, output = breakout_double_4_true_true, internallength = 4, passedbyvalue = true, alignment = double);
create table alter_distpol_g_double_4_true_true (i int, j break_double_4_true_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_4_true_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_4_true_false;
create function breakin_double_4_true_false (cstring) returns break_double_4_true_false as 'textin' language internal;
create function breakout_double_4_true_false (break_double_4_true_false) returns cstring as 'textout' language internal;

create type break_double_4_true_false (input = breakin_double_4_true_false, output = breakout_double_4_true_false, internallength = 4, passedbyvalue = true, alignment = double);
create table alter_distpol_g_double_4_true_false (i int, j break_double_4_true_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_4_true_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_4_false_true;
create function breakin_double_4_false_true (cstring) returns break_double_4_false_true as 'textin' language internal;
create function breakout_double_4_false_true (break_double_4_false_true) returns cstring as 'textout' language internal;

create type break_double_4_false_true (input = breakin_double_4_false_true, output = breakout_double_4_false_true, internallength = 4, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_4_false_true (i int, j break_double_4_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_4_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_4_false_false;
create function breakin_double_4_false_false (cstring) returns break_double_4_false_false as 'textin' language internal;
create function breakout_double_4_false_false (break_double_4_false_false) returns cstring as 'textout' language internal;

create type break_double_4_false_false (input = breakin_double_4_false_false, output = breakout_double_4_false_false, internallength = 4, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_4_false_false (i int, j break_double_4_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_4_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_11_false_true;
create function breakin_double_11_false_true (cstring) returns break_double_11_false_true as 'textin' language internal;
create function breakout_double_11_false_true (break_double_11_false_true) returns cstring as 'textout' language internal;

create type break_double_11_false_true (input = breakin_double_11_false_true, output = breakout_double_11_false_true, internallength = 11, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_11_false_true (i int, j break_double_11_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_11_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_11_false_false;
create function breakin_double_11_false_false (cstring) returns break_double_11_false_false as 'textin' language internal;
create function breakout_double_11_false_false (break_double_11_false_false) returns cstring as 'textout' language internal;

create type break_double_11_false_false (input = breakin_double_11_false_false, output = breakout_double_11_false_false, internallength = 11, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_11_false_false (i int, j break_double_11_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_11_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_17_false_true;
create function breakin_double_17_false_true (cstring) returns break_double_17_false_true as 'textin' language internal;
create function breakout_double_17_false_true (break_double_17_false_true) returns cstring as 'textout' language internal;

create type break_double_17_false_true (input = breakin_double_17_false_true, output = breakout_double_17_false_true, internallength = 17, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_17_false_true (i int, j break_double_17_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_17_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_17_false_false;
create function breakin_double_17_false_false (cstring) returns break_double_17_false_false as 'textin' language internal;
create function breakout_double_17_false_false (break_double_17_false_false) returns cstring as 'textout' language internal;

create type break_double_17_false_false (input = breakin_double_17_false_false, output = breakout_double_17_false_false, internallength = 17, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_17_false_false (i int, j break_double_17_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_17_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_19_false_true;
create function breakin_double_19_false_true (cstring) returns break_double_19_false_true as 'textin' language internal;
create function breakout_double_19_false_true (break_double_19_false_true) returns cstring as 'textout' language internal;

create type break_double_19_false_true (input = breakin_double_19_false_true, output = breakout_double_19_false_true, internallength = 19, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_19_false_true (i int, j break_double_19_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_19_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_19_false_false;
create function breakin_double_19_false_false (cstring) returns break_double_19_false_false as 'textin' language internal;
create function breakout_double_19_false_false (break_double_19_false_false) returns cstring as 'textout' language internal;

create type break_double_19_false_false (input = breakin_double_19_false_false, output = breakout_double_19_false_false, internallength = 19, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_19_false_false (i int, j break_double_19_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_19_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_23_false_true;
create function breakin_double_23_false_true (cstring) returns break_double_23_false_true as 'textin' language internal;
create function breakout_double_23_false_true (break_double_23_false_true) returns cstring as 'textout' language internal;

create type break_double_23_false_true (input = breakin_double_23_false_true, output = breakout_double_23_false_true, internallength = 23, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_23_false_true (i int, j break_double_23_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_23_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_23_false_false;
create function breakin_double_23_false_false (cstring) returns break_double_23_false_false as 'textin' language internal;
create function breakout_double_23_false_false (break_double_23_false_false) returns cstring as 'textout' language internal;

create type break_double_23_false_false (input = breakin_double_23_false_false, output = breakout_double_23_false_false, internallength = 23, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_23_false_false (i int, j break_double_23_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_23_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_32_false_true;
create function breakin_double_32_false_true (cstring) returns break_double_32_false_true as 'textin' language internal;
create function breakout_double_32_false_true (break_double_32_false_true) returns cstring as 'textout' language internal;

create type break_double_32_false_true (input = breakin_double_32_false_true, output = breakout_double_32_false_true, internallength = 32, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_32_false_true (i int, j break_double_32_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_32_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_32_false_false;
create function breakin_double_32_false_false (cstring) returns break_double_32_false_false as 'textin' language internal;
create function breakout_double_32_false_false (break_double_32_false_false) returns cstring as 'textout' language internal;

create type break_double_32_false_false (input = breakin_double_32_false_false, output = breakout_double_32_false_false, internallength = 32, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_32_false_false (i int, j break_double_32_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_32_false_false (i, k) select i, i from generate_series(1, 10) i;
create type break_double_196_false_true;
create function breakin_double_196_false_true (cstring) returns break_double_196_false_true as 'textin' language internal;
create function breakout_double_196_false_true (break_double_196_false_true) returns cstring as 'textout' language internal;

create type break_double_196_false_true (input = breakin_double_196_false_true, output = breakout_double_196_false_true, internallength = 196, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_196_false_true (i int, j break_double_196_false_true, k text) with (appendonly = true) distributed by (i);
insert into alter_distpol_g_double_196_false_true (i, k) select i, i from generate_series(1, 10) i;
create type break_double_196_false_false;
create function breakin_double_196_false_false (cstring) returns break_double_196_false_false as 'textin' language internal;
create function breakout_double_196_false_false (break_double_196_false_false) returns cstring as 'textout' language internal;

create type break_double_196_false_false (input = breakin_double_196_false_false, output = breakout_double_196_false_false, internallength = 196, passedbyvalue = false, alignment = double);
create table alter_distpol_g_double_196_false_false (i int, j break_double_196_false_false, k text) with (appendonly = false) distributed by (i);
insert into alter_distpol_g_double_196_false_false (i, k) select i, i from generate_series(1, 10) i;
COMMIT;
alter table alter_distpol_g_int2_variable_false_true drop column j;
select * from alter_distpol_g_int2_variable_false_true order by 1;
alter table alter_distpol_g_int2_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_variable_false_true order by 1;
drop type break_int2_variable_false_true cascade;
alter table alter_distpol_g_int2_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_variable_false_true order by 1;
alter table alter_distpol_g_int2_variable_false_false drop column j;
select * from alter_distpol_g_int2_variable_false_false order by 1;
alter table alter_distpol_g_int2_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_variable_false_false order by 1;
drop type break_int2_variable_false_false cascade;
alter table alter_distpol_g_int2_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_variable_false_false order by 1;
alter table alter_distpol_g_int2_1_true_true drop column j;
select * from alter_distpol_g_int2_1_true_true order by 1;
alter table alter_distpol_g_int2_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_true_true order by 1;
drop type break_int2_1_true_true cascade;
alter table alter_distpol_g_int2_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_true_true order by 1;
alter table alter_distpol_g_int2_1_true_false drop column j;
select * from alter_distpol_g_int2_1_true_false order by 1;
alter table alter_distpol_g_int2_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_true_false order by 1;
drop type break_int2_1_true_false cascade;
alter table alter_distpol_g_int2_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_true_false order by 1;
alter table alter_distpol_g_int2_1_false_true drop column j;
select * from alter_distpol_g_int2_1_false_true order by 1;
alter table alter_distpol_g_int2_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_false_true order by 1;
drop type break_int2_1_false_true cascade;
alter table alter_distpol_g_int2_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_false_true order by 1;
alter table alter_distpol_g_int2_1_false_false drop column j;
select * from alter_distpol_g_int2_1_false_false order by 1;
alter table alter_distpol_g_int2_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_false_false order by 1;
drop type break_int2_1_false_false cascade;
alter table alter_distpol_g_int2_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_1_false_false order by 1;
alter table alter_distpol_g_int2_3_true_true drop column j;
select * from alter_distpol_g_int2_3_true_true order by 1;
alter table alter_distpol_g_int2_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_true_true order by 1;
drop type break_int2_3_true_true cascade;
alter table alter_distpol_g_int2_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_true_true order by 1;
alter table alter_distpol_g_int2_3_true_false drop column j;
select * from alter_distpol_g_int2_3_true_false order by 1;
alter table alter_distpol_g_int2_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_true_false order by 1;
drop type break_int2_3_true_false cascade;
alter table alter_distpol_g_int2_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_true_false order by 1;
alter table alter_distpol_g_int2_3_false_true drop column j;
select * from alter_distpol_g_int2_3_false_true order by 1;
alter table alter_distpol_g_int2_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_false_true order by 1;
drop type break_int2_3_false_true cascade;
alter table alter_distpol_g_int2_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_false_true order by 1;
alter table alter_distpol_g_int2_3_false_false drop column j;
select * from alter_distpol_g_int2_3_false_false order by 1;
alter table alter_distpol_g_int2_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_false_false order by 1;
drop type break_int2_3_false_false cascade;
alter table alter_distpol_g_int2_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_3_false_false order by 1;
alter table alter_distpol_g_int2_4_true_true drop column j;
select * from alter_distpol_g_int2_4_true_true order by 1;
alter table alter_distpol_g_int2_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_true_true order by 1;
drop type break_int2_4_true_true cascade;
alter table alter_distpol_g_int2_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_true_true order by 1;
alter table alter_distpol_g_int2_4_true_false drop column j;
select * from alter_distpol_g_int2_4_true_false order by 1;
alter table alter_distpol_g_int2_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_true_false order by 1;
drop type break_int2_4_true_false cascade;
alter table alter_distpol_g_int2_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_true_false order by 1;
alter table alter_distpol_g_int2_4_false_true drop column j;
select * from alter_distpol_g_int2_4_false_true order by 1;
alter table alter_distpol_g_int2_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_false_true order by 1;
drop type break_int2_4_false_true cascade;
alter table alter_distpol_g_int2_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_false_true order by 1;
alter table alter_distpol_g_int2_4_false_false drop column j;
select * from alter_distpol_g_int2_4_false_false order by 1;
alter table alter_distpol_g_int2_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_false_false order by 1;
drop type break_int2_4_false_false cascade;
alter table alter_distpol_g_int2_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_4_false_false order by 1;
alter table alter_distpol_g_int2_11_false_true drop column j;
select * from alter_distpol_g_int2_11_false_true order by 1;
alter table alter_distpol_g_int2_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_11_false_true order by 1;
drop type break_int2_11_false_true cascade;
alter table alter_distpol_g_int2_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_11_false_true order by 1;
alter table alter_distpol_g_int2_11_false_false drop column j;
select * from alter_distpol_g_int2_11_false_false order by 1;
alter table alter_distpol_g_int2_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_11_false_false order by 1;
drop type break_int2_11_false_false cascade;
alter table alter_distpol_g_int2_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_11_false_false order by 1;
alter table alter_distpol_g_int2_17_false_true drop column j;
select * from alter_distpol_g_int2_17_false_true order by 1;
alter table alter_distpol_g_int2_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_17_false_true order by 1;
drop type break_int2_17_false_true cascade;
alter table alter_distpol_g_int2_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_17_false_true order by 1;
alter table alter_distpol_g_int2_17_false_false drop column j;
select * from alter_distpol_g_int2_17_false_false order by 1;
alter table alter_distpol_g_int2_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_17_false_false order by 1;
drop type break_int2_17_false_false cascade;
alter table alter_distpol_g_int2_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_17_false_false order by 1;
alter table alter_distpol_g_int2_19_false_true drop column j;
select * from alter_distpol_g_int2_19_false_true order by 1;
alter table alter_distpol_g_int2_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_19_false_true order by 1;
drop type break_int2_19_false_true cascade;
alter table alter_distpol_g_int2_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_19_false_true order by 1;
alter table alter_distpol_g_int2_19_false_false drop column j;
select * from alter_distpol_g_int2_19_false_false order by 1;
alter table alter_distpol_g_int2_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_19_false_false order by 1;
drop type break_int2_19_false_false cascade;
alter table alter_distpol_g_int2_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_19_false_false order by 1;
alter table alter_distpol_g_int2_23_false_true drop column j;
select * from alter_distpol_g_int2_23_false_true order by 1;
alter table alter_distpol_g_int2_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_23_false_true order by 1;
drop type break_int2_23_false_true cascade;
alter table alter_distpol_g_int2_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_23_false_true order by 1;
alter table alter_distpol_g_int2_23_false_false drop column j;
select * from alter_distpol_g_int2_23_false_false order by 1;
alter table alter_distpol_g_int2_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_23_false_false order by 1;
drop type break_int2_23_false_false cascade;
alter table alter_distpol_g_int2_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_23_false_false order by 1;
alter table alter_distpol_g_int2_32_false_true drop column j;
select * from alter_distpol_g_int2_32_false_true order by 1;
alter table alter_distpol_g_int2_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_32_false_true order by 1;
drop type break_int2_32_false_true cascade;
alter table alter_distpol_g_int2_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_32_false_true order by 1;
alter table alter_distpol_g_int2_32_false_false drop column j;
select * from alter_distpol_g_int2_32_false_false order by 1;
alter table alter_distpol_g_int2_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_32_false_false order by 1;
drop type break_int2_32_false_false cascade;
alter table alter_distpol_g_int2_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_32_false_false order by 1;
alter table alter_distpol_g_int2_196_false_true drop column j;
select * from alter_distpol_g_int2_196_false_true order by 1;
alter table alter_distpol_g_int2_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_196_false_true order by 1;
drop type break_int2_196_false_true cascade;
alter table alter_distpol_g_int2_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_196_false_true order by 1;
alter table alter_distpol_g_int2_196_false_false drop column j;
select * from alter_distpol_g_int2_196_false_false order by 1;
alter table alter_distpol_g_int2_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_196_false_false order by 1;
drop type break_int2_196_false_false cascade;
alter table alter_distpol_g_int2_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int2_196_false_false order by 1;
alter table alter_distpol_g_int4_variable_false_true drop column j;
select * from alter_distpol_g_int4_variable_false_true order by 1;
alter table alter_distpol_g_int4_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_variable_false_true order by 1;
drop type break_int4_variable_false_true cascade;
alter table alter_distpol_g_int4_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_variable_false_true order by 1;
alter table alter_distpol_g_int4_variable_false_false drop column j;
select * from alter_distpol_g_int4_variable_false_false order by 1;
alter table alter_distpol_g_int4_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_variable_false_false order by 1;
drop type break_int4_variable_false_false cascade;
alter table alter_distpol_g_int4_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_variable_false_false order by 1;
alter table alter_distpol_g_int4_1_true_true drop column j;
select * from alter_distpol_g_int4_1_true_true order by 1;
alter table alter_distpol_g_int4_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_true_true order by 1;
drop type break_int4_1_true_true cascade;
alter table alter_distpol_g_int4_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_true_true order by 1;
alter table alter_distpol_g_int4_1_true_false drop column j;
select * from alter_distpol_g_int4_1_true_false order by 1;
alter table alter_distpol_g_int4_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_true_false order by 1;
drop type break_int4_1_true_false cascade;
alter table alter_distpol_g_int4_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_true_false order by 1;
alter table alter_distpol_g_int4_1_false_true drop column j;
select * from alter_distpol_g_int4_1_false_true order by 1;
alter table alter_distpol_g_int4_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_false_true order by 1;
drop type break_int4_1_false_true cascade;
alter table alter_distpol_g_int4_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_false_true order by 1;
alter table alter_distpol_g_int4_1_false_false drop column j;
select * from alter_distpol_g_int4_1_false_false order by 1;
alter table alter_distpol_g_int4_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_false_false order by 1;
drop type break_int4_1_false_false cascade;
alter table alter_distpol_g_int4_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_1_false_false order by 1;
alter table alter_distpol_g_int4_3_true_true drop column j;
select * from alter_distpol_g_int4_3_true_true order by 1;
alter table alter_distpol_g_int4_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_true_true order by 1;
drop type break_int4_3_true_true cascade;
alter table alter_distpol_g_int4_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_true_true order by 1;
alter table alter_distpol_g_int4_3_true_false drop column j;
select * from alter_distpol_g_int4_3_true_false order by 1;
alter table alter_distpol_g_int4_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_true_false order by 1;
drop type break_int4_3_true_false cascade;
alter table alter_distpol_g_int4_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_true_false order by 1;
alter table alter_distpol_g_int4_3_false_true drop column j;
select * from alter_distpol_g_int4_3_false_true order by 1;
alter table alter_distpol_g_int4_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_false_true order by 1;
drop type break_int4_3_false_true cascade;
alter table alter_distpol_g_int4_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_false_true order by 1;
alter table alter_distpol_g_int4_3_false_false drop column j;
select * from alter_distpol_g_int4_3_false_false order by 1;
alter table alter_distpol_g_int4_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_false_false order by 1;
drop type break_int4_3_false_false cascade;
alter table alter_distpol_g_int4_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_3_false_false order by 1;
alter table alter_distpol_g_int4_4_true_true drop column j;
select * from alter_distpol_g_int4_4_true_true order by 1;
alter table alter_distpol_g_int4_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_true_true order by 1;
drop type break_int4_4_true_true cascade;
alter table alter_distpol_g_int4_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_true_true order by 1;
alter table alter_distpol_g_int4_4_true_false drop column j;
select * from alter_distpol_g_int4_4_true_false order by 1;
alter table alter_distpol_g_int4_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_true_false order by 1;
drop type break_int4_4_true_false cascade;
alter table alter_distpol_g_int4_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_true_false order by 1;
alter table alter_distpol_g_int4_4_false_true drop column j;
select * from alter_distpol_g_int4_4_false_true order by 1;
alter table alter_distpol_g_int4_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_false_true order by 1;
drop type break_int4_4_false_true cascade;
alter table alter_distpol_g_int4_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_false_true order by 1;
alter table alter_distpol_g_int4_4_false_false drop column j;
select * from alter_distpol_g_int4_4_false_false order by 1;
alter table alter_distpol_g_int4_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_false_false order by 1;
drop type break_int4_4_false_false cascade;
alter table alter_distpol_g_int4_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_4_false_false order by 1;
alter table alter_distpol_g_int4_11_false_true drop column j;
select * from alter_distpol_g_int4_11_false_true order by 1;
alter table alter_distpol_g_int4_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_11_false_true order by 1;
drop type break_int4_11_false_true cascade;
alter table alter_distpol_g_int4_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_11_false_true order by 1;
alter table alter_distpol_g_int4_11_false_false drop column j;
select * from alter_distpol_g_int4_11_false_false order by 1;
alter table alter_distpol_g_int4_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_11_false_false order by 1;
drop type break_int4_11_false_false cascade;
alter table alter_distpol_g_int4_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_11_false_false order by 1;
alter table alter_distpol_g_int4_17_false_true drop column j;
select * from alter_distpol_g_int4_17_false_true order by 1;
alter table alter_distpol_g_int4_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_17_false_true order by 1;
drop type break_int4_17_false_true cascade;
alter table alter_distpol_g_int4_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_17_false_true order by 1;
alter table alter_distpol_g_int4_17_false_false drop column j;
select * from alter_distpol_g_int4_17_false_false order by 1;
alter table alter_distpol_g_int4_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_17_false_false order by 1;
drop type break_int4_17_false_false cascade;
alter table alter_distpol_g_int4_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_17_false_false order by 1;
alter table alter_distpol_g_int4_19_false_true drop column j;
select * from alter_distpol_g_int4_19_false_true order by 1;
alter table alter_distpol_g_int4_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_19_false_true order by 1;
drop type break_int4_19_false_true cascade;
alter table alter_distpol_g_int4_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_19_false_true order by 1;
alter table alter_distpol_g_int4_19_false_false drop column j;
select * from alter_distpol_g_int4_19_false_false order by 1;
alter table alter_distpol_g_int4_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_19_false_false order by 1;
drop type break_int4_19_false_false cascade;
alter table alter_distpol_g_int4_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_19_false_false order by 1;
alter table alter_distpol_g_int4_23_false_true drop column j;
select * from alter_distpol_g_int4_23_false_true order by 1;
alter table alter_distpol_g_int4_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_23_false_true order by 1;
drop type break_int4_23_false_true cascade;
alter table alter_distpol_g_int4_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_23_false_true order by 1;
alter table alter_distpol_g_int4_23_false_false drop column j;
select * from alter_distpol_g_int4_23_false_false order by 1;
alter table alter_distpol_g_int4_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_23_false_false order by 1;
drop type break_int4_23_false_false cascade;
alter table alter_distpol_g_int4_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_23_false_false order by 1;
alter table alter_distpol_g_int4_32_false_true drop column j;
select * from alter_distpol_g_int4_32_false_true order by 1;
alter table alter_distpol_g_int4_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_32_false_true order by 1;
drop type break_int4_32_false_true cascade;
alter table alter_distpol_g_int4_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_32_false_true order by 1;
alter table alter_distpol_g_int4_32_false_false drop column j;
select * from alter_distpol_g_int4_32_false_false order by 1;
alter table alter_distpol_g_int4_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_32_false_false order by 1;
drop type break_int4_32_false_false cascade;
alter table alter_distpol_g_int4_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_32_false_false order by 1;
alter table alter_distpol_g_int4_196_false_true drop column j;
select * from alter_distpol_g_int4_196_false_true order by 1;
alter table alter_distpol_g_int4_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_196_false_true order by 1;
drop type break_int4_196_false_true cascade;
alter table alter_distpol_g_int4_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_196_false_true order by 1;
alter table alter_distpol_g_int4_196_false_false drop column j;
select * from alter_distpol_g_int4_196_false_false order by 1;
alter table alter_distpol_g_int4_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_196_false_false order by 1;
drop type break_int4_196_false_false cascade;
alter table alter_distpol_g_int4_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_int4_196_false_false order by 1;
alter table alter_distpol_g_char_variable_false_true drop column j;
select * from alter_distpol_g_char_variable_false_true order by 1;
alter table alter_distpol_g_char_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_variable_false_true order by 1;
drop type break_char_variable_false_true cascade;
alter table alter_distpol_g_char_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_variable_false_true order by 1;
alter table alter_distpol_g_char_variable_false_false drop column j;
select * from alter_distpol_g_char_variable_false_false order by 1;
alter table alter_distpol_g_char_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_variable_false_false order by 1;
drop type break_char_variable_false_false cascade;
alter table alter_distpol_g_char_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_variable_false_false order by 1;
alter table alter_distpol_g_char_1_true_true drop column j;
select * from alter_distpol_g_char_1_true_true order by 1;
alter table alter_distpol_g_char_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_true_true order by 1;
drop type break_char_1_true_true cascade;
alter table alter_distpol_g_char_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_true_true order by 1;
alter table alter_distpol_g_char_1_true_false drop column j;
select * from alter_distpol_g_char_1_true_false order by 1;
alter table alter_distpol_g_char_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_true_false order by 1;
drop type break_char_1_true_false cascade;
alter table alter_distpol_g_char_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_true_false order by 1;
alter table alter_distpol_g_char_1_false_true drop column j;
select * from alter_distpol_g_char_1_false_true order by 1;
alter table alter_distpol_g_char_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_false_true order by 1;
drop type break_char_1_false_true cascade;
alter table alter_distpol_g_char_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_false_true order by 1;
alter table alter_distpol_g_char_1_false_false drop column j;
select * from alter_distpol_g_char_1_false_false order by 1;
alter table alter_distpol_g_char_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_false_false order by 1;
drop type break_char_1_false_false cascade;
alter table alter_distpol_g_char_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_1_false_false order by 1;
alter table alter_distpol_g_char_3_true_true drop column j;
select * from alter_distpol_g_char_3_true_true order by 1;
alter table alter_distpol_g_char_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_true_true order by 1;
drop type break_char_3_true_true cascade;
alter table alter_distpol_g_char_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_true_true order by 1;
alter table alter_distpol_g_char_3_true_false drop column j;
select * from alter_distpol_g_char_3_true_false order by 1;
alter table alter_distpol_g_char_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_true_false order by 1;
drop type break_char_3_true_false cascade;
alter table alter_distpol_g_char_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_true_false order by 1;
alter table alter_distpol_g_char_3_false_true drop column j;
select * from alter_distpol_g_char_3_false_true order by 1;
alter table alter_distpol_g_char_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_false_true order by 1;
drop type break_char_3_false_true cascade;
alter table alter_distpol_g_char_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_false_true order by 1;
alter table alter_distpol_g_char_3_false_false drop column j;
select * from alter_distpol_g_char_3_false_false order by 1;
alter table alter_distpol_g_char_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_false_false order by 1;
drop type break_char_3_false_false cascade;
alter table alter_distpol_g_char_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_3_false_false order by 1;
alter table alter_distpol_g_char_4_true_true drop column j;
select * from alter_distpol_g_char_4_true_true order by 1;
alter table alter_distpol_g_char_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_true_true order by 1;
drop type break_char_4_true_true cascade;
alter table alter_distpol_g_char_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_true_true order by 1;
alter table alter_distpol_g_char_4_true_false drop column j;
select * from alter_distpol_g_char_4_true_false order by 1;
alter table alter_distpol_g_char_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_true_false order by 1;
drop type break_char_4_true_false cascade;
alter table alter_distpol_g_char_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_true_false order by 1;
alter table alter_distpol_g_char_4_false_true drop column j;
select * from alter_distpol_g_char_4_false_true order by 1;
alter table alter_distpol_g_char_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_false_true order by 1;
drop type break_char_4_false_true cascade;
alter table alter_distpol_g_char_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_false_true order by 1;
alter table alter_distpol_g_char_4_false_false drop column j;
select * from alter_distpol_g_char_4_false_false order by 1;
alter table alter_distpol_g_char_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_false_false order by 1;
drop type break_char_4_false_false cascade;
alter table alter_distpol_g_char_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_4_false_false order by 1;
alter table alter_distpol_g_char_11_false_true drop column j;
select * from alter_distpol_g_char_11_false_true order by 1;
alter table alter_distpol_g_char_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_11_false_true order by 1;
drop type break_char_11_false_true cascade;
alter table alter_distpol_g_char_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_11_false_true order by 1;
alter table alter_distpol_g_char_11_false_false drop column j;
select * from alter_distpol_g_char_11_false_false order by 1;
alter table alter_distpol_g_char_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_11_false_false order by 1;
drop type break_char_11_false_false cascade;
alter table alter_distpol_g_char_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_11_false_false order by 1;
alter table alter_distpol_g_char_17_false_true drop column j;
select * from alter_distpol_g_char_17_false_true order by 1;
alter table alter_distpol_g_char_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_17_false_true order by 1;
drop type break_char_17_false_true cascade;
alter table alter_distpol_g_char_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_17_false_true order by 1;
alter table alter_distpol_g_char_17_false_false drop column j;
select * from alter_distpol_g_char_17_false_false order by 1;
alter table alter_distpol_g_char_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_17_false_false order by 1;
drop type break_char_17_false_false cascade;
alter table alter_distpol_g_char_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_17_false_false order by 1;
alter table alter_distpol_g_char_19_false_true drop column j;
select * from alter_distpol_g_char_19_false_true order by 1;
alter table alter_distpol_g_char_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_19_false_true order by 1;
drop type break_char_19_false_true cascade;
alter table alter_distpol_g_char_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_19_false_true order by 1;
alter table alter_distpol_g_char_19_false_false drop column j;
select * from alter_distpol_g_char_19_false_false order by 1;
alter table alter_distpol_g_char_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_19_false_false order by 1;
drop type break_char_19_false_false cascade;
alter table alter_distpol_g_char_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_19_false_false order by 1;
alter table alter_distpol_g_char_23_false_true drop column j;
select * from alter_distpol_g_char_23_false_true order by 1;
alter table alter_distpol_g_char_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_23_false_true order by 1;
drop type break_char_23_false_true cascade;
alter table alter_distpol_g_char_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_23_false_true order by 1;
alter table alter_distpol_g_char_23_false_false drop column j;
select * from alter_distpol_g_char_23_false_false order by 1;
alter table alter_distpol_g_char_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_23_false_false order by 1;
drop type break_char_23_false_false cascade;
alter table alter_distpol_g_char_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_23_false_false order by 1;
alter table alter_distpol_g_char_32_false_true drop column j;
select * from alter_distpol_g_char_32_false_true order by 1;
alter table alter_distpol_g_char_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_32_false_true order by 1;
drop type break_char_32_false_true cascade;
alter table alter_distpol_g_char_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_32_false_true order by 1;
alter table alter_distpol_g_char_32_false_false drop column j;
select * from alter_distpol_g_char_32_false_false order by 1;
alter table alter_distpol_g_char_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_32_false_false order by 1;
drop type break_char_32_false_false cascade;
alter table alter_distpol_g_char_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_32_false_false order by 1;
alter table alter_distpol_g_char_196_false_true drop column j;
select * from alter_distpol_g_char_196_false_true order by 1;
alter table alter_distpol_g_char_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_196_false_true order by 1;
drop type break_char_196_false_true cascade;
alter table alter_distpol_g_char_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_196_false_true order by 1;
alter table alter_distpol_g_char_196_false_false drop column j;
select * from alter_distpol_g_char_196_false_false order by 1;
alter table alter_distpol_g_char_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_196_false_false order by 1;
drop type break_char_196_false_false cascade;
alter table alter_distpol_g_char_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_char_196_false_false order by 1;
alter table alter_distpol_g_double_variable_false_true drop column j;
select * from alter_distpol_g_double_variable_false_true order by 1;
alter table alter_distpol_g_double_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_variable_false_true order by 1;
drop type break_double_variable_false_true cascade;
alter table alter_distpol_g_double_variable_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_variable_false_true order by 1;
alter table alter_distpol_g_double_variable_false_false drop column j;
select * from alter_distpol_g_double_variable_false_false order by 1;
alter table alter_distpol_g_double_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_variable_false_false order by 1;
drop type break_double_variable_false_false cascade;
alter table alter_distpol_g_double_variable_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_variable_false_false order by 1;
alter table alter_distpol_g_double_1_true_true drop column j;
select * from alter_distpol_g_double_1_true_true order by 1;
alter table alter_distpol_g_double_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_true_true order by 1;
drop type break_double_1_true_true cascade;
alter table alter_distpol_g_double_1_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_true_true order by 1;
alter table alter_distpol_g_double_1_true_false drop column j;
select * from alter_distpol_g_double_1_true_false order by 1;
alter table alter_distpol_g_double_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_true_false order by 1;
drop type break_double_1_true_false cascade;
alter table alter_distpol_g_double_1_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_true_false order by 1;
alter table alter_distpol_g_double_1_false_true drop column j;
select * from alter_distpol_g_double_1_false_true order by 1;
alter table alter_distpol_g_double_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_false_true order by 1;
drop type break_double_1_false_true cascade;
alter table alter_distpol_g_double_1_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_false_true order by 1;
alter table alter_distpol_g_double_1_false_false drop column j;
select * from alter_distpol_g_double_1_false_false order by 1;
alter table alter_distpol_g_double_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_false_false order by 1;
drop type break_double_1_false_false cascade;
alter table alter_distpol_g_double_1_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_1_false_false order by 1;
alter table alter_distpol_g_double_3_true_true drop column j;
select * from alter_distpol_g_double_3_true_true order by 1;
alter table alter_distpol_g_double_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_true_true order by 1;
drop type break_double_3_true_true cascade;
alter table alter_distpol_g_double_3_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_true_true order by 1;
alter table alter_distpol_g_double_3_true_false drop column j;
select * from alter_distpol_g_double_3_true_false order by 1;
alter table alter_distpol_g_double_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_true_false order by 1;
drop type break_double_3_true_false cascade;
alter table alter_distpol_g_double_3_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_true_false order by 1;
alter table alter_distpol_g_double_3_false_true drop column j;
select * from alter_distpol_g_double_3_false_true order by 1;
alter table alter_distpol_g_double_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_false_true order by 1;
drop type break_double_3_false_true cascade;
alter table alter_distpol_g_double_3_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_false_true order by 1;
alter table alter_distpol_g_double_3_false_false drop column j;
select * from alter_distpol_g_double_3_false_false order by 1;
alter table alter_distpol_g_double_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_false_false order by 1;
drop type break_double_3_false_false cascade;
alter table alter_distpol_g_double_3_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_3_false_false order by 1;
alter table alter_distpol_g_double_4_true_true drop column j;
select * from alter_distpol_g_double_4_true_true order by 1;
alter table alter_distpol_g_double_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_true_true order by 1;
drop type break_double_4_true_true cascade;
alter table alter_distpol_g_double_4_true_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_true_true order by 1;
alter table alter_distpol_g_double_4_true_false drop column j;
select * from alter_distpol_g_double_4_true_false order by 1;
alter table alter_distpol_g_double_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_true_false order by 1;
drop type break_double_4_true_false cascade;
alter table alter_distpol_g_double_4_true_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_true_false order by 1;
alter table alter_distpol_g_double_4_false_true drop column j;
select * from alter_distpol_g_double_4_false_true order by 1;
alter table alter_distpol_g_double_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_false_true order by 1;
drop type break_double_4_false_true cascade;
alter table alter_distpol_g_double_4_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_false_true order by 1;
alter table alter_distpol_g_double_4_false_false drop column j;
select * from alter_distpol_g_double_4_false_false order by 1;
alter table alter_distpol_g_double_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_false_false order by 1;
drop type break_double_4_false_false cascade;
alter table alter_distpol_g_double_4_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_4_false_false order by 1;
alter table alter_distpol_g_double_11_false_true drop column j;
select * from alter_distpol_g_double_11_false_true order by 1;
alter table alter_distpol_g_double_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_11_false_true order by 1;
drop type break_double_11_false_true cascade;
alter table alter_distpol_g_double_11_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_11_false_true order by 1;
alter table alter_distpol_g_double_11_false_false drop column j;
select * from alter_distpol_g_double_11_false_false order by 1;
alter table alter_distpol_g_double_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_11_false_false order by 1;
drop type break_double_11_false_false cascade;
alter table alter_distpol_g_double_11_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_11_false_false order by 1;
alter table alter_distpol_g_double_17_false_true drop column j;
select * from alter_distpol_g_double_17_false_true order by 1;
alter table alter_distpol_g_double_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_17_false_true order by 1;
drop type break_double_17_false_true cascade;
alter table alter_distpol_g_double_17_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_17_false_true order by 1;
alter table alter_distpol_g_double_17_false_false drop column j;
select * from alter_distpol_g_double_17_false_false order by 1;
alter table alter_distpol_g_double_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_17_false_false order by 1;
drop type break_double_17_false_false cascade;
alter table alter_distpol_g_double_17_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_17_false_false order by 1;
alter table alter_distpol_g_double_19_false_true drop column j;
select * from alter_distpol_g_double_19_false_true order by 1;
alter table alter_distpol_g_double_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_19_false_true order by 1;
drop type break_double_19_false_true cascade;
alter table alter_distpol_g_double_19_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_19_false_true order by 1;
alter table alter_distpol_g_double_19_false_false drop column j;
select * from alter_distpol_g_double_19_false_false order by 1;
alter table alter_distpol_g_double_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_19_false_false order by 1;
drop type break_double_19_false_false cascade;
alter table alter_distpol_g_double_19_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_19_false_false order by 1;
alter table alter_distpol_g_double_23_false_true drop column j;
select * from alter_distpol_g_double_23_false_true order by 1;
alter table alter_distpol_g_double_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_23_false_true order by 1;
drop type break_double_23_false_true cascade;
alter table alter_distpol_g_double_23_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_23_false_true order by 1;
alter table alter_distpol_g_double_23_false_false drop column j;
select * from alter_distpol_g_double_23_false_false order by 1;
alter table alter_distpol_g_double_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_23_false_false order by 1;
drop type break_double_23_false_false cascade;
alter table alter_distpol_g_double_23_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_23_false_false order by 1;
alter table alter_distpol_g_double_32_false_true drop column j;
select * from alter_distpol_g_double_32_false_true order by 1;
alter table alter_distpol_g_double_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_32_false_true order by 1;
drop type break_double_32_false_true cascade;
alter table alter_distpol_g_double_32_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_32_false_true order by 1;
alter table alter_distpol_g_double_32_false_false drop column j;
select * from alter_distpol_g_double_32_false_false order by 1;
alter table alter_distpol_g_double_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_32_false_false order by 1;
drop type break_double_32_false_false cascade;
alter table alter_distpol_g_double_32_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_32_false_false order by 1;
alter table alter_distpol_g_double_196_false_true drop column j;
select * from alter_distpol_g_double_196_false_true order by 1;
alter table alter_distpol_g_double_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_196_false_true order by 1;
drop type break_double_196_false_true cascade;
alter table alter_distpol_g_double_196_false_true set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_196_false_true order by 1;
alter table alter_distpol_g_double_196_false_false drop column j;
select * from alter_distpol_g_double_196_false_false order by 1;
alter table alter_distpol_g_double_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_196_false_false order by 1;
drop type break_double_196_false_false cascade;
alter table alter_distpol_g_double_196_false_false set with(reorganize = true) distributed randomly;
select * from alter_distpol_g_double_196_false_false order by 1;
