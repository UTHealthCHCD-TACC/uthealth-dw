/*
 * The enrollment_date table maps patient enrollments in a particular dataset to every month/year covered in the data
 */

-- Date Table
drop table data_warehouse.dim_date;
create table data_warehouse.dim_date as		
select TO_CHAR(datum,'yyyymmdd')::INT AS id,
       datum AS date,
       EXTRACT(MONTH FROM datum) AS month,
       TO_CHAR(datum,'Month') AS month_name
FROM (	  select datum
      FROM GENERATE_SERIES ('2007-01-01'::DATE, '2022-12-31'::DATE, '1 month') AS datum
) DQ
ORDER BY 1;

-- Add Columens
alter table data_warehouse.dim_date add column year smallint;
update data_warehouse.dim_date set year = extract(year from date);

--UT FY
--NOTE: Doesn't handle 00->99
alter table data_warehouse.dim_date add column fy_ut char(2);
alter table data_warehouse.dim_date drop column fy_ut_temp;
alter table data_warehouse.dim_date add column fy_ut_temp int;
update data_warehouse.dim_date set fy_ut_temp = cast(substring(cast(year as varchar), 3) as int); 
update data_warehouse.dim_date set fy_ut = case 
											when month >= 9 then LPAD(cast(cast(fy_ut_temp+1 as int) as varchar), 2, '0') 
											else LPAD(cast(cast(fy_ut_temp as int) as varchar), 2, '0')
										end;
alter table data_warehouse.dim_date drop column fy_ut_temp;									
select * from data_warehouse.dim_date;

--Map Enrollment to dim_date
drop table data_warehouse.member_enrollment_month;
create table data_warehouse.member_enrollment_month (
id bigserial,
date_id bigint,
source char(4), 

--Member demographics
mbr_id_src varchar,
uth_mbr_id varchar,
gndr_cd char(1), 
mbr_dob smallint, 
fam_id_src varchar, 
dod char(5),
state char(2),

--Plan information
plan_typ varchar,
plan_typ_src varchar
)
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.member_enrollment_month_id_seq cache 200;

--Modify columns
alter table member_enrollment_month alter column state type varchar;

--Optum DOD - 6 min
insert into data_warehouse.member_enrollment_month(date_id, source, mbr_id_src, uth_mbr_id, 
gndr_cd, mbr_dob, fam_id_src, dod, state, 
plan_typ, plan_typ_src)
select d.id, 'OPTD', patid, md5('OPTD' || cast(patid as varchar)), 
gdr_cd, yrdob, family_id, null, state, 
null, product
from optum_zip.member m
join dim_date d on d.date between date_trunc('month', m.eligeff) and m.eligend; --date_trunc to match 1/1/2010 in dim_date to eligeff of 1/15/2010

--Optum ZIP - 6 min
insert into data_warehouse.member_enrollment_month(date_id, source, mbr_id_src, uth_mbr_id, 
gndr_cd, mbr_dob, fam_id_src, dod, state, 
plan_typ, plan_typ_src)
select d.id, 'OPTZ', patid, md5('OPTZ' || cast(patid as varchar)), 
gdr_cd, yrdob, family_id, split_part(zipcode_5, '_', 1), null, 
null, product
from optum_zip.member m
join dim_date d on d.date between date_trunc('month', m.eligeff) and m.eligend; --date_trunc to match 1/1/2010 in dim_date to eligeff of 1/15/2010

--Truven
insert into data_warehouse.member_enrollment_month(date_id, source, mbr_id_src, uth_mbr_id, 
gndr_cd, 
mbr_dob, fam_id_src, dod, state, 
plan_typ, plan_typ_src)
select d.id, 'trvc', enrolid, md5('trvc' || cast(enrolid as varchar)),
case when sex=1 then 'M' else 'F' end, 
dobyr, efamid, null, s.abbr, 
null, plantyp
from truven.ccaet c
join dev.truven_state_codes s on c.egeoloc=s.truven_code
join dim_date d on d.date between date_trunc('month', c.dtstart) and c.dtend;

insert into data_warehouse.member_enrollment_month(date_id, source, mbr_id_src, uth_mbr_id, 
gndr_cd, 
mbr_dob, fam_id_src, dod, state, 
plan_typ, plan_typ_src)
select d.id, 'trvm', enrolid, md5('trvc' || cast(enrolid as varchar)),
case when sex=1 then 'M' else 'F' end, 
dobyr, efamid, null, s.abbr, 
null, plantyp
from truven.mdcrt c
join dev.truven_state_codes s on c.egeoloc=s.truven_code
join dim_date d on d.date between date_trunc('month', c.dtstart) and c.dtend;


--Verify
select source, count(*)
from data_warehouse.member_enrollment_month
group by 1;