-- Tests for the internal "text_position_setup/next" functions. They are
-- used to implement replace(text, text), and a few other string functions.
--
-- It's been modified in GPDB, compared to upstream version, to deal with
-- large inputs. Hence we test large values.

select string_to_array('abcDdefDghiDaaaDDDDDDDDDDDDDDäääDöööDäääD', 'DDD');
select replace('XabXcdefX', 'X', 'Y');

select replace(repeat('X', 20000),  'XX', '');
select replace(repeat('X', 20001),  'XX', '');

-- Tests with a large strings.
CREATE TEMP TABLE strtest (i int, x text);
insert into strtest select g, repeat('ä', g) from generate_series(100000-10, 100000+10) g;

select i, i % 50, replace(x, 'ääääääääääääääääääääääääääääääääääääääääääääääääää', '') from strtest order by i;
select i, i % 50, replace('1' ||x, 'ääääääääääääääääääääääääääääääääääääääääääääääääää', '') from strtest order by i;
select i, i % 50, replace('12' ||x, 'ääääääääääääääääääääääääääääääääääääääääääääääääää', '') from strtest order by i;
select i, i % 50, replace('123' ||x, 'ääääääääääääääääääääääääääääääääääääääääääääääääää', '') from strtest order by i;
select i, i % 50, replace('1234' ||x, 'ääääääääääääääääääääääääääääääääääääääääääääääääää', '') from strtest order by i;
select i, i % 50, replace('12345' ||x, 'ääääääääääääääääääääääääääääääääääääääääääääääääää', '') from strtest order by i;

select array_dims(string_to_array(repeat('a|', 100000), '|'));
select array_dims(string_to_array(repeat('ab|', 100000), '|'));
select array_dims(string_to_array(repeat('abc|', 100000), '|'));
select array_dims(string_to_array(repeat('abcd|', 100000), '|'));
select array_dims(string_to_array(repeat('xLONGDELIMITER', 100000), 'LONGDELIMITER'));
