-- Find all pregnant females from 2016 who are diabetic before or at the time of their pregnancy

-- Lookup codes of interest
select *
from optum_zip.lu_diagnosis
where diag_desc ilike '%diabet%' or diag_desc ilike '%pregna%';

--Slow Way

select count(*)
from optum_zip.member m 
--Pregnant
join optum_zip.diagnostic preg on m.patid=preg.patid
join optum_zip.lu_diagnosis preg_lu on preg.diag=preg_lu.diag_cd and preg_lu.diag_desc ilike '%pregnan%'
--Diabetes
join optum_zip.diagnostic diabetic on m.patid=diabetic.patid
join optum_zip.lu_diagnosis diabetic_lu on diabetic.diag=diabetic_lu.diag_cd and diabetic_lu.diag_desc ilike '%diabet%' and diabetic.year <= 2016
--Filter
where m.gdr_cd='F'
and preg.year = 2016;


--Speed it up

--Year Cache - 114 secs
create table dev.diag_2016
as 
select d.*
from optum_zip.diagnostic d
where d.year=2016;

--Female Cache - 5 sec
create table dev.female
as
select m.*
from optum_zip.member m
where m.gdr_cd='F';


--Pregnant Cache - 6 secs
create table dev.pregnant_2016
as
select distinct m.patid, d.clmid
from dev.female m
join dev.diag_2016 d on m.patid=d.patid
join optum_zip.lu_diagnosis d_lu on d.diag=d_lu.diag_cd and d_lu.diag_desc ilike '%pregnan%'

--Diabetic Cache - 37 secs
create table dev.diabetic_2016
as
select distinct m.patid
from dev.female m 
join optum_zip.diagnostic d on m.patid=d.patid
join optum_zip.lu_diagnosis d_lu on d.diag=d_lu.diag_cd and d_lu.diag_desc ilike '%diabet%'
where d.year <= 2016

--Join Cache - 1 sec
create table dev.pregnant_diabetic_2016
as
select p.*
from dev.pregnant_2016 p
join dev.diabetic_2016 d on p.patid=d.patid;

--Analyze - < 1 sec
select count(distinct patid)
from dev.pregnant_2016;

select count(distinct patid)
from dev.pregnant_diabetic_2016;

--Join to get facilities - 13 secs
select pd.patid, f.*
from dev.pregnant_diabetic_2016 pd
join optum_zip.facility_detail f on pd.clmid=f.clmid;



