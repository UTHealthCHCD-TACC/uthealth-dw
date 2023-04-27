/**************************
 * PSQL Monitoring Script
 **************************/

--see what queries with your username are running
select usename, pid, state, waiting, query_start , query
from pg_catalog.pg_stat_activity where
usename in ('xrzhang') and ---- put your username
state = 'active'
order by state, usename;

--see when tables were last analyzed in staging_clean
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'staging_clean'
order by last_analyze desc;

--see when tables were last analyzed in dw_staging
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'dw_staging'
order by last_analyze desc;

--see what queries are running, periodt
select usename, pid, state, waiting, query_start , query, *
from pg_catalog.pg_stat_activity where
state = 'active'
order by state, usename;