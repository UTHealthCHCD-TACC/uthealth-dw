-- Generate Query
select 'select '''||tablename||''' as table_name, year, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('truven')
order by schemaname, tablename;

--Get Stats
select table_name, year, 'refresh', cnt
from truven.table_counts
union
select table_name, year, 'current', cnt
from truven.table_counts
order by 1, 2, 3;

-- Save Results Query
drop table truven.table_counts;
create table truven.table_counts
as
select 'ccaea' as table_name, year, count(*) as cnt from truven.ccaea group by 1, 2 union
select 'ccaed' as table_name, year, count(*) as cnt from truven.ccaed group by 1, 2 union
select 'ccaef' as table_name, year, count(*) as cnt from truven.ccaef group by 1, 2 union
select 'ccaei' as table_name, year, count(*) as cnt from truven.ccaei group by 1, 2 union
select 'ccaeo' as table_name, year, count(*) as cnt from truven.ccaeo group by 1, 2 union
select 'ccaep' as table_name, year, count(*) as cnt from truven.ccaep group by 1, 2 union
select 'ccaes' as table_name, year, count(*) as cnt from truven.ccaes group by 1, 2 union
select 'ccaet' as table_name, year, count(*) as cnt from truven.ccaet group by 1, 2 union
select 'hpm_abs' as table_name, year, count(*) as cnt from truven.hpm_abs group by 1, 2 union
select 'hpm_elig' as table_name, year, count(*) as cnt from truven.hpm_elig group by 1, 2 union
select 'hpm_ltd' as table_name, year, count(*) as cnt from truven.hpm_ltd group by 1, 2 union
select 'hpm_std' as table_name, year, count(*) as cnt from truven.hpm_std group by 1, 2 union
select 'hpm_wc' as table_name, year, count(*) as cnt from truven.hpm_wc group by 1, 2 union
select 'mdcra' as table_name, year, count(*) as cnt from truven.mdcra group by 1, 2 union
select 'mdcrd' as table_name, year, count(*) as cnt from truven.mdcrd group by 1, 2 union
select 'mdcrf' as table_name, year, count(*) as cnt from truven.mdcrf group by 1, 2 union
select 'mdcri' as table_name, year, count(*) as cnt from truven.mdcri group by 1, 2 union
select 'mdcro' as table_name, year, count(*) as cnt from truven.mdcro group by 1, 2 union
select 'mdcrp' as table_name, year, count(*) as cnt from truven.mdcrp group by 1, 2 union
select 'mdcrs' as table_name, year, count(*) as cnt from truven.mdcrs group by 1, 2 union
select 'mdcrt' as table_name, year, count(*) as cnt from truven.mdcrt group by 1, 2;