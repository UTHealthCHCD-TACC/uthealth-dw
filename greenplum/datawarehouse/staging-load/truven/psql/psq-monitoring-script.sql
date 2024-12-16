/**************************
 * PSQL Monitoring Script
 **************************/

--see what queries with your username are running
select usename, pid, state, waiting, query_start , query
from pg_catalog.pg_stat_activity where
usename in ('xrzhang') and ---- put your username
state = 'active'
order by state, usename;

--see what queries are running (actives only)
select usename, pid, state, waiting, query_start , query
from pg_catalog.pg_stat_activity
where state = 'active'
order by state, usename;

--see what queries are running, periodt
select usename, pid, state, waiting, query_start , query
from pg_catalog.pg_stat_activity
order by state, usename;

--Kill query
select pg_terminate_backend(8625);

/************************************
 * Get last vacuum analyze time
 */

--see when tables were last analyzed in truven
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'truven'
order by last_analyze desc;

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
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'data_warehouse'
order by case when last_vacuum is not null then last_vacuum else last_analyze end desc;

/******************
 * Check if pay column is populated
 */

(select year, pay
from truven.mdcro where year = 2021
limit 3)
union all
(select year, pay
from truven.mdcro where year = 2022
limit 3)
union all
(select year, pay
from truven.mdcro where year = 2023
limit 3)
;

/*******************
 * spot-check some tables
 */

select * from dw_staging.truc_claim_detail where year = 2023;
select * from dw_staging.trum_claim_detail where year = 2023;

select * from staging_clean.ccaed_etl where year = 2023;
select * from dw_staging.truc_pharmacy_claims where year = 2023;

/*********************
 * spot-check some counts
 */

select count(*) as truc_staging_pharmacy_rows
from dw_staging.truc_pharmacy_claims
where year = 2023;
--162497376
--162621801 (from counts)

select (162621801 - 162497376) * 1.0 /162621801 as pct;
--0.00076511881700289373
--that seems right, some small % will be missing enrolid

select count(*) from dw_staging.truc_claim_detail where year = 2023;
--547764289
--519702994 (from counts, ccaeo)
-- 28573516 (from counts, ccaes)

select 519702994+ 28573516;
--548276510

select (548276510-547764289) * 1.0 / 548276510;
--0.00093423845570185015

--ok let's hope the other tables are ok

/***********************
 * etc
 */

--see sizes of databases
select datname, pg_database_size(datname), pg_size_pretty(pg_database_size(datname))
from pg_database;


