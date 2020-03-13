select year, count(*) as total_count, min(admit_date) as min_admit_date, max(admit_date) as max_admit_date, count(distinct prov) as distinct_prov
from optum_dod_refresh.confinement 
group by 1
order by 1;

select year, count(*), min(FST_DT), max(FST_DT)
from optum_dod_refresh.diagnostic
group by 1
order by 1;

select year, count(*), min(FST_DT), max(FST_DT)
from optum_dod_refresh.lab_result
group by 1
order by 1;

select year, count(*), min(FST_DT), max(FST_DT)
from optum_dod_refresh.medical
group by 1
order by 1;

select year, count(*), min(fill_dt), max(fill_dt)
from optum_dod_refresh.mbr_co_enroll
group by 1
order by 1;

select year, count(*), min(fill_dt), max(fill_dt)
from optum_dod_refresh.mbr_enroll
group by 1
order by 1;

select year, count(*), min(FST_DT), max(FST_DT)
from optum_dod_refresh.procedure
group by 1
order by 1;

select year, count(*), min(fill_dt), max(fill_dt)
from optum_dod_refresh.provider
group by 1
order by 1;

select year, count(*), min(fill_dt), max(fill_dt)
from optum_dod_refresh.provider_bridge
group by 1
order by 1;

select year, count(*), min(fill_dt), max(fill_dt)
from optum_dod_refresh.rx
group by 1
order by 1;

--Move over ref rables
select * from optum_dod.ref_admit_channel rac 
select * from reference_tables.ref_admit_source;

select * from optum_dod.ref_admit_type;
select * from reference_tables.ref_admit_type;
