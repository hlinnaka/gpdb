-- Create a simple Append-only table.
-- Insert to it. In two different statements, to get some updates going
-- on the aosegments table associated with it.
CREATE TABLE upgrade_simple_ao(id int4) WITH (appendonly=true);
INSERT INTO upgrade_simple_ao SELECT generate_series(1, 1000);
INSERT INTO upgrade_simple_ao SELECT generate_series(1001, 2000);

-- Also test an append-only table with a numeric column (and some others).
-- The on-disk format of numerics changed between 8.2 and 8.3, make sure
-- that it can be read out correctly after upgrade.
CREATE TABLE upgrade_ao(id int4, n numeric, t text) WITH (appendonly=true);
INSERT INTO upgrade_ao SELECT g, g::numeric / 100, 't' || g FROM generate_series(1, 1000) g;
INSERT INTO upgrade_ao SELECT g, g::numeric / 100, 't' || g FROM generate_series(1001, 2000) g;
