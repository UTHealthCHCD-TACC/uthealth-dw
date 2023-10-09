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

--see when tables were last analyzed in data_warehouse
select schemaname, relname, case when last_vacuum is not null then last_vacuum else last_analyze end as last_vacuum_analyze
from pg_stat_all_tables
where schemaname = 'data_warehouse'
order by case when last_vacuum is not null then last_vacuum else last_analyze end desc;

--see what queries are running, periodt
select usename, pid, state, waiting, query_start , query
from pg_catalog.pg_stat_activity where
state = 'active'
order by state, usename;

--any query
select usename, pid, state, waiting, query_start , query, *
from pg_catalog.pg_stat_activity
order by state, usename;

/******************
 * Kill Query
 ******************/
select pg_terminate_backend(101404);


/****************
 * See server activity
 */

select * from pg_stat_activity;

--semi useful
select * from pg_stat_database;

select * from pg_stat_user_tables;

select * from pg_stat_user_indexes;

select * from pg_stat_bgwriter;


select count(*) from medicare_texas.mbsf_abcd_summary where year::int = 2019;


--get column names AND data type
select column_name, udt_name ||
	case when character_maximum_length is not null then '(' || character_maximum_length || ')'
	else '' end
from information_schema.columns
where table_schema = 'medicaid' and table_name = 'enrl'
order by ordinal_position;





select year, sum(case when ) from truven.ccaef where year





