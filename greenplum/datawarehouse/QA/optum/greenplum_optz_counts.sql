-- optumzip counts

-- mbr_enroll
drop table if exists dev.ip_optz_mbr_count;
select null::int as "year", count(patid) as row_count, count(distinct patid) as pat_count, null::int as clm_count
  into dev.ip_optz_mbr_count
  from optum_zip.mbr_enroll
 group by 1;
 
alter table dev.ip_optz_mbr_count add column table_source text;
update dev.ip_optz_mbr_count 
   set table_source = 'mbr_enroll';

-- mbr_co_enroll
drop table if exists dev.ip_optz_mbr_co_count;
select null::int as "year", count(patid) as row_count, count(distinct patid) as pat_count, null::int as clm_count
  into dev.ip_optz_mbr_co_count
  from optum_zip.mbr_co_enroll
 group by 1;
 
alter table dev.ip_optz_mbr_co_count add column table_source text;
update dev.ip_optz_mbr_co_count 
   set table_source = 'mbr_co_enroll';

-- diagnostic
drop table if exists dev.ip_optz_diag_count;

select a."year", 0 as row_count, count(distinct a.patid) as pat_count, 0 as clm_count
  into dev.ip_optz_diag_count 
  from (select distinct "year", patid
  		from optum_zip.diagnostic) a
  group by a."year";
 
update dev.ip_optz_diag_count a
set clm_count = b.clm_count
from (select distinct "year", count(distinct clmid) as clm_count
  from optum_zip.diagnostic
  group by "year") b
where a.year = b.year;

update dev.ip_optz_diag_count a
set row_count = b.clm_count
from (select "year", count(clmid) as clm_count
  from optum_zip.diagnostic
  group by "year") b
where a.year = b.year;

alter table dev.ip_optz_diag_count add column table_source text;
update dev.ip_optz_diag_count 
   set table_source = 'diagnostic';
 
-- medical
drop table if exists dev.ip_optz_medical_count;

select a."year", 0 as row_count, count(distinct a.patid) as pat_count, 0 as clm_count
  into dev.ip_optz_medical_count 
  from (select distinct "year", patid
  		from optum_zip.medical) a
  group by a."year";
 
update dev.ip_optz_medical_count a
set clm_count = b.clm_count
from (select distinct "year", count(distinct clmid) as clm_count
  from optum_zip.medical
  group by "year") b
where a.year = b.year;

update dev.ip_optz_medical_count a
set row_count = b.clm_count
from (select "year", count(clmid) as clm_count
  from optum_zip.medical
  group by "year") b
where a.year = b.year;

alter table dev.ip_optz_medical_count add column table_source text;
update dev.ip_optz_medical_count 
   set table_source = 'medical'  ;
  
  
-- procedure
drop table if exists dev.ip_optz_procedure_count;

select a."year", 0 as row_count, count(distinct a.patid) as pat_count, 0 as clm_count
  into dev.ip_optz_procedure_count 
  from (select distinct "year", patid
  		from optum_zip."procedure") a
  group by a."year";
 
update dev.ip_optz_procedure_count a
set clm_count = b.clm_count
from (select distinct "year", count(distinct clmid) as clm_count
  from optum_zip."procedure"
  group by "year") b
where a.year = b.year;

update dev.ip_optz_procedure_count a
set row_count = b.clm_count
from (select "year", count(clmid) as clm_count
  from optum_zip."procedure"
  group by "year") b
where a.year = b.year;

alter table dev.ip_optz_procedure_count add column table_source text;
update dev.ip_optz_procedure_count 
   set table_source = 'procedure'  ;
  
-- rx
drop table if exists dev.ip_optz_rx_count;

select a."year", 0 as row_count, count(distinct a.patid) as pat_count, 0 as clm_count
  into dev.ip_optz_rx_count 
  from (select distinct "year", patid
  		from optum_zip.rx) a
  group by a."year";
 
update dev.ip_optz_rx_count a
set clm_count = b.clm_count
from (select distinct "year", count(distinct clmid) as clm_count
  from optum_zip.rx
  group by "year") b
where a.year = b.year;

update dev.ip_optz_rx_count a
set row_count = b.clm_count
from (select "year", count(clmid) as clm_count
  from optum_zip.rx
  group by "year") b
where a.year = b.year;

alter table dev.ip_optz_rx_count add column table_source text;
update dev.ip_optz_rx_count 
   set table_source = 'rx'  ;

-- confinement
drop table if exists dev.ip_optz_confinement_count;

select a."year", 0 as row_count, count(distinct a.patid) as pat_count, 0 as clm_count
  into dev.ip_optz_confinement_count 
  from (select distinct "year", patid
  		from optum_zip.confinement) a
  group by a."year";
 
update dev.ip_optz_confinement_count a
set clm_count = b.clm_count
from (select distinct "year", count(distinct conf_id) as clm_count
  from optum_zip.confinement
  group by "year") b
where a.year = b.year;

update dev.ip_optz_confinement_count a
set row_count = b.clm_count
from (select "year", count(conf_id) as clm_count
  from optum_zip.confinement
  group by "year") b
where a.year = b.year;

alter table dev.ip_optz_confinement_count add column table_source text;
update dev.ip_optz_confinement_count 
   set table_source = 'confinement'  ;
  
-- lab result
drop table if exists dev.ip_optz_lab_count;

select a."year", 0 as row_count, count(distinct a.patid) as pat_count, 0 as clm_count
  into dev.ip_optz_lab_count 
  from (select distinct "year", patid
  		from optum_zip.lab_result) a
  group by a."year";
 
update dev.ip_optz_lab_count a
set clm_count = b.clm_count
from (select distinct "year", count(distinct labclmid) as clm_count
  from optum_zip.lab_result
  group by "year") b
where a.year = b.year;

update dev.ip_optz_lab_count a
set row_count = b.clm_count
from (select "year", count(labclmid) as clm_count
  from optum_zip.lab_result
  group by "year") b
where a.year = b.year;

alter table dev.ip_optz_lab_count add column table_source text;
update dev.ip_optz_lab_count 
   set table_source = 'lab_result';  
 
-- provider
drop table if exists dev.ip_optz_provider_count;
  
select null::int as year, count(*) as row_count, 
		null::int as pat_count, null::int as clm_count,
		'provider'::text as table_source
  into dev.ip_optz_provider_count
  from optum_zip.provider;
  
-- provider bridge
drop table if exists dev.ip_optz_provider_bridge_count;

select null::int as year, count(*) as row_count, 
		null::int as pat_count, null::int as clm_count,
		'provider_bridge'::text as table_source
  into dev.ip_optz_provider_bridge_count
  from optum_zip.provider_bridge;
  
-- look up tables
drop table if exists dev.ip_optz_lu_count;

create table dev.ip_optz_lu_count as
( 
select null::int as year, count(*) as row_count, 
		null::int as pat_count, null::int as clm_count,
		'lu_diagnosis'::text as table_source
  from optum_zip.lu_diagnosis
union
select null::int as year, count(*) as row_count, 
		null::int as pat_count, null::int as clm_count,
		'lu_ndc'::text as table_source
  from optum_zip.lu_ndc
union
select null::int as year, count(*) as row_count, 
		null::int as pat_count, null::int as clm_count,
		'lu_procedure'::text as table_source
  from optum_zip.lu_procedure
);
-- combining all count tables  
  
drop table if exists dev.ip_optz_count;
create table dev.ip_optz_count as(
select 'gp_optz' as data_source, to_char(NOW()::date, 'MMDDYYYY') as date_created, *
  from (
     	select *
   		  from dev.ip_optz_mbr_count
   		 union
 		select *
  		  from dev.ip_optz_diag_count
  		 union
  		select *
  		  from dev.ip_optz_medical_count
  		 union
   		select *
  		  from dev.ip_optz_procedure_count
  		 union
  		select *
  		  from dev.ip_optz_rx_count
       union
      select *
        from dev.ip_optz_mbr_co_count
       union
      select *
        from dev.ip_optz_confinement_count
       union
      select *
        from dev.ip_optz_lab_count
       union
      select *
        from dev.ip_optz_provider_count
       union
      select *
        from dev.ip_optz_provider_bridge_count
       union
      select *
        from dev.ip_optz_lu_count
  ) as a
 order by a.table_source
 );

drop table if exists dev.ip_optz_mbr_count;
drop table if exists dev.ip_optz_diag_count;
drop table if exists dev.ip_optz_medical_count;
drop table if exists dev.ip_optz_procedure_count;
drop table if exists dev.ip_optz_rx_count;
drop table if exists dev.ip_optz_mbr_co_count;
drop table if exists dev.ip_optz_confinement_count;
drop table if exists dev.ip_optz_lab_count;
drop table if exists dev.ip_optz_provider_count;
drop table if exists dev.ip_optz_provider_bridge_count;
drop table if exists dev.ip_optz_lu_count;

select *
  from dev.ip_optz_count
  order by year, table_source;
