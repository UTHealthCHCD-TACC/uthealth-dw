/*
 * The member_enrollment_monthly table creates on record for each month/year that a member was enrolled
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!!!!   
 *   !!!!!!!!      Use dw-dim_member_id_src.sql in Git !!!
 */


------------------------------------------------------------------------------------------------------------------------
drop table data_warehouse.member_enrollment_monthly;

create table data_warehouse.member_enrollment_monthly (
    --ID columns
	id bigserial,
	data_source char(4), 
	month_year_id char(7),
	uth_member_id bigint,	
	
	--demographics
	gender_cd char(1),
	state varchar,
	zip5 char(5),
	zip3 char(3),
	age_derived int,
	dob_derived date, 
	death_date date,
	
	--enrollment type
	plan_type char(4),
	bus_cd char(3),
	claim_created_flag bool default false
)
WITH (appendonly=true, orientation=column)
distributed randomly;

alter sequence data_warehouse.member_enrollment_monthly_id_seq cache 200;
------------------------------------------------------------------------------------------------------------------------


--Optum DOD 
select * 
from data_warehouse.dim_member_id_src a 
where not exists ( select patid from optum_dod.member b where b.patid::text = a.member_id_src ) and a.data_source = 'optd';

select * 
from optum_dod.member a 
where not exists ( select 1 from data_warehouse.dim_member_id_src b where b.member_id_src = a.patid::text and b.data_source = 'optd');


--Optum DOD load
insert into data_warehouse.member_enrollment_monthly (
	data_source, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)	
select 'optd', b.month_year_id, a.uth_member_id,
       c.gender_cd, state, null, null, 
       b.year_int - yrdob, (yrdob::varchar || '-12-31')::date as birth_dt, (select max(death_ym) from optum_dod.member_wdeath dod where dod.patid = m.patid ) as death_dt,  
       d.plan_type, bus
from optum_dod.member m
  join data_warehouse.dim_member_id_src a
    on a.member_id_src = m.patid::text
   and a.data_source = 'optd'
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) and m.eligend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd 
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
where not exists (select 1 from data_warehouse.dim_member_id_src e where e.data_source ='optd' and e.member_id_src = m.patid::text)
;



--Optum ZIP validation
select * 
from data_warehouse.dim_member_id_src a 
where not exists ( select patid from optum_zip.member b where b.patid::text = a.member_id_src ) and a.data_source = 'optz';

select * 
from optum_zip.member a 
where not exists ( select 1 from data_warehouse.dim_member_id_src b where b.member_id_src = a.patid::text and b.data_source = 'optz');


--Optum ZIP load
insert into data_warehouse.member_enrollment_monthly (
	data_source, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)
select 
	   'optz', b.month_year_id, a.uth_member_id,
       c.gender_cd, null, split_part(zipcode_5, '_', 1), substring(zipcode_5::text,1,3),
       b.year_int - yrdob, (yrdob::varchar || '-12-31')::date, null, 
       d.plan_type, bus
from optum_zip.member m
  join data_warehouse.dim_member_id_src a
    on a.member_id_src = m.patid::text
   and a.data_source = 'optz'
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) and m.eligend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
 where not exists (select 1 from data_warehouse.dim_member_id_src e where e.data_source ='optz' and e.member_id_src = m.patid::text)
 ; 


 --Truven Commercial
select * 
from truven.ccaet a 
where not exists ( select 1 from data_warehouse.dim_member_id_src b where b.member_id_src = a.enrolid::text and b.data_source = 'trvc');


 
 --Truven Commercial
insert into data_warehouse.member_enrollment_monthly (
	data_source, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)	
select 
	   'trvc', b.month_year_id, a.uth_member_id,
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, trunc(m.empzip,0)::text,
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null, 
       d.plan_type, 'COM'
from truven.ccaet m
  join data_warehouse.dim_member_id_src a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'trvc'
  join reference_tables.ref_truven_state_codes s 
    on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b 
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp
;





--Truven Medicaid  
insert into data_warehouse.member_enrollment_monthly (
	data_source, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)		
select 
       'trvm', b.month_year_id, a.uth_member_id,
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, trunc(m.empzip,0)::text,
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null,
       d.plan_type, 'MCR'
from truven.mdcrt m
  join data_warehouse.dim_member_id_src a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'trvm'
  join reference_tables.ref_truven_state_codes s 
	on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp
;




select count(distinct uth_member_id), data_source
from data_warehouse.member_enrollment_monthly
group by data_source
;



