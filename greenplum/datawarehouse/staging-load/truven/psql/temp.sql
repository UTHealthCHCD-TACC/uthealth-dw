
select 'Script Start' || current_timestamp as message;

select 'Task 1 - count from truven-ccaeo started at: ' || current_timestamp as message;
select count(*) as count_from_bcarrier from truven.ccaeo;

select 'Task 2 - count from dme started at ' || current_timestamp as message;
select count(*) as count_from_dme from medicare_texas.dme_claims_k;

select 'Script completed at: ' || current_timestamp as message;

select usename, pid, state, waiting, query_start , query, *
from pg_catalog.pg_stat_activity where
usename in ('xrzhang') and ---- put your username
state = 'active'
order by state, usename;