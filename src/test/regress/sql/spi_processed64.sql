CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

CREATE TABLE biginserttest (id int4);

-- Pretend that the insert below inserted more than 4 billion rows.
--
-- Use type 'skip', because we don't want to throw an ERROR or worse. There
-- is special handling at the code that checks for this fault, to bump up
-- the row counter regardless of the fault type.
select gp_inject_fault('executor_run_high_processed', 'reset', 2);
select gp_inject_fault('executor_run_high_processed', 'skip', 2);

do $$
declare
  num_rows int8;
begin

  insert into biginserttest select g from generate_series(1, 100) g;
  GET DIAGNOSTICS num_rows = ROW_COUNT;

  raise notice 'inserted % rows', num_rows;
end;
$$;

select gp_inject_fault('executor_run_high_processed', 'reset', 2);
