select extract_ym, year, count(*) as total_count, min(admit_date) as min_admit_date, max(admit_date) as max_admit_date, count(distinct prov) as distinct_prov
from optum_zip.confinement 
group by 1, 2
order by 1, 2;

select extract_ym, year, count(*), min(FST_DT), max(FST_DT)
from optum_zip.diagnostic
group by 1, 2
order by 1, 2;

select extract_ym, year, count(*), min(FST_DT), max(FST_DT)
from optum_zip.lab_result
group by 1, 2
order by 1, 2;

select extract_ym, year, count(*), min(FST_DT), max(FST_DT)
from optum_zip.medical
group by 1, 2
order by 1, 2;

select extract_ym, year, count(*), min(FST_DT), max(FST_DT)
from optum_zip.procedure
group by 1, 2
order by 1, 2;

select extract_ym, year, count(*), min(fill_dt), max(fill_dt)
from optum_zip.rx
group by 1, 2
order by 1, 2;


----------
select extract_ym, count(*), min(eligeff), max(eligend)
from optum_zip.mbr_co_enroll
group by 1
order by 1;

select extract_ym, count(*), min(eligeff), max(eligend)
from optum_zip.mbr_enroll
group by 1
order by 1;

select extract_ym, count(*), count(distinct prov_unique)
from optum_zip.provider
group by 1
order by 1;

select extract_ym, count(*), count(distinct prov_unique)
from optum_zip.provider_bridge
group by 1
order by 1;

