/***********************************
* Script purpose: vacuum analyze all Medicare tables
* except for reference tables and table_counts
***********************************/

--when were tables last vacuum analyzed?
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'medicare_national';

select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'medicare_texas';

--code to generate code to vacuum analyze all tables
select 'vacuum analyze ' || schemaname || '.' || relname || ';'
from pg_stat_all_tables
  where schemaname = 'medicare_national' and
  order by n_live_tup;
 
 select 'vacuum analyze ' || schemaname || '.' || relname || ';'
from pg_stat_all_tables
  where schemaname = 'medicare_texas' and
  order by n_live_tup;

 
 
 