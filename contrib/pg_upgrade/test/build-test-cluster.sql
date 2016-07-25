CREATE TABLE upgrade_ao(id int4) WITH (appendonly=true);
INSERT INTO upgrade_ao SELECT generate_series(1, 1000);
INSERT INTO upgrade_ao SELECT generate_series(1001, 2000);

