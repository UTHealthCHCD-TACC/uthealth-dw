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
select current_date, 'ccaea' as table_name, year, count(*) as cnt from truven.ccaea group by 1, 2, 3 union
select current_date, 'ccaed' as table_name, year, count(*) as cnt from truven.ccaed group by 1, 2, 3 union
select current_date, 'ccaef' as table_name, year, count(*) as cnt from truven.ccaef group by 1, 2, 3 union
select current_date, 'ccaei' as table_name, year, count(*) as cnt from truven.ccaei group by 1, 2, 3 union
select current_date, 'ccaeo' as table_name, year, count(*) as cnt from truven.ccaeo group by 1, 2, 3 union
select current_date, 'ccaep' as table_name, year, count(*) as cnt from truven.ccaep group by 1, 2, 3 union
select current_date, 'ccaes' as table_name, year, count(*) as cnt from truven.ccaes group by 1, 2, 3 union
select current_date, 'ccaet' as table_name, year, count(*) as cnt from truven.ccaet group by 1, 2, 3 union
select current_date, 'hpm_abs' as table_name, year, count(*) as cnt from truven.hpm_abs group by 1, 2, 3 union
select current_date, 'hpm_elig' as table_name, year, count(*) as cnt from truven.hpm_elig group by 1, 2, 3 union
select current_date, 'hpm_ltd' as table_name, year, count(*) as cnt from truven.hpm_ltd group by 1, 2, 3 union
select current_date, 'hpm_std' as table_name, year, count(*) as cnt from truven.hpm_std group by 1, 2, 3 union
select current_date, 'hpm_wc' as table_name, year, count(*) as cnt from truven.hpm_wc group by 1, 2, 3 union
select current_date, 'mdcra' as table_name, year, count(*) as cnt from truven.mdcra group by 1, 2, 3 union
select current_date, 'mdcrd' as table_name, year, count(*) as cnt from truven.mdcrd group by 1, 2, 3 union
select current_date, 'mdcrf' as table_name, year, count(*) as cnt from truven.mdcrf group by 1, 2, 3 union
select current_date, 'mdcri' as table_name, year, count(*) as cnt from truven.mdcri group by 1, 2, 3 union
select current_date, 'mdcro' as table_name, year, count(*) as cnt from truven.mdcro group by 1, 2, 3 union
select current_date, 'mdcrp' as table_name, year, count(*) as cnt from truven.mdcrp group by 1, 2, 3 union
select current_date, 'mdcrs' as table_name, year, count(*) as cnt from truven.mdcrs group by 1, 2, 3 union
select current_date, 'mdcrt' as table_name, year, count(*) as cnt from truven.mdcrt group by 1, 2, 3;