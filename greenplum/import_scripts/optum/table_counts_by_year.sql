-- Generate Query
select 'select '''||tablename||''' as table_name, year, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('optum_zip')
order by schemaname, tablename;

--Get Stats
select table_name, year, 'refresh', cnt
from optum_zip.table_counts
union
select table_name, year, 'current', cnt
from optum_zip.table_counts
order by 1, 2, 3;

-- Save Results Query
drop table optum_zip.table_counts;
create table optum_zip.table_counts
as
select 'confinement' as table_name, year, count(*) as cnt from optum_zip.confinement group by 1, 2 union
select 'diagnostic' as table_name, year, count(*) as cnt from optum_zip.diagnostic group by 1, 2 union
select 'lab_result' as table_name, year, count(*) as cnt from optum_zip.lab_result group by 1, 2 union
select 'medical' as table_name, year, count(*) as cnt from optum_zip.medical group by 1, 2 union
select 'procedure' as table_name, year, count(*) as cnt from optum_zip.procedure group by 1, 2 union
select 'rx' as table_name, year, count(*) as cnt from optum_zip.rx group by 1, 2 union
select 'mbr_enroll' as table_name, extract_ym, count(*) as cnt from optum_zip.mbr_enroll group by 1, 2 union
select 'mbr_co_enroll' as table_name, extract_ym, count(*) as cnt from optum_zip.mbr_co_enroll group by 1, 2; 
--union select 'mbrwdeath' as table_name, extract_ym, count(*) as cnt from optum_dod.mbrwdeath group by 1, 2;