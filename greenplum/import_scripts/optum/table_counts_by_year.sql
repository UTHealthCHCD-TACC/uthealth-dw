-- Generate Query
select 'select '''||tablename||''' as table_name, year, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('optum_dod')
order by schemaname, tablename;

--Get Stats
select table_name, year, 'refresh', cnt
from optum_dod.table_counts
union
select table_name, year, 'current', cnt
from optum_dod.table_counts
order by 1, 2, 3;

-- Save Results Query
drop table optum_dod.table_counts;
create table optum_dod.table_counts
as
select current_date, 'confinement' as table_name, year, count(*) as cnt from optum_dod.confinement group by 1, 2, 3 union
select current_date, 'diagnostic' as table_name, year, count(*) as cnt from optum_dod.diagnostic group by 1, 2, 3 union
select current_date, 'lab_result' as table_name, year, count(*) as cnt from optum_dod.lab_result group by 1, 2, 3 union
select current_date, 'medical' as table_name, year, count(*) as cnt from optum_dod.medical group by 1, 2, 3 union
select current_date, 'procedure' as table_name, year, count(*) as cnt from optum_dod.procedure group by 1, 2, 3 union
select current_date, 'rx' as table_name, year, count(*) as cnt from optum_dod.rx group by 1, 2, 3 union
select current_date, 'mbr_enroll_r' as table_name, extract_ym, count(*) as cnt from optum_dod.mbr_enroll_r group by 1, 2, 3 union
select current_date, 'mbr_co_enroll_r' as table_name, extract_ym, count(*) as cnt from optum_dod.mbr_co_enroll_r group by 1, 2, 3
union select current_date, 'mbrwdeath' as table_name, extract_ym, count(*) as cnt from optum_dod.mbrwdeath group by 1, 2, 3;