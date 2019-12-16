/*
 * The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!   
 *   	             Use dw-create-load-dim_member_id_src.sql in Git      
 */


drop table if exists data_warehouse.member_enrollment_monthly;

--Create member_enrollment_monthly table ---------------------------------------------------------

create table data_warehouse.member_enrollment_monthly (
    --ID columns
	data_source char(4), 
	month_year_id int4,
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
	bus_cd char(4),
	employee_status text, 
	claim_created_flag bool default false
)
WITH (appendonly=true, orientation=column)
distributed by(uth_member_id, month_year_id)
partition by list (data_source)
	subpartition by range(month_year_id)
		subpartition template 
	(
		--start(200601) end(201901) every(100),
		--default subpartition spXXXX
		subpartition sp2007 start(200701) INCLUSIVE,
		subpartition sp2008 start(200801) INCLUSIVE,
		subpartition sp2009 start(200901) INCLUSIVE,
		subpartition sp2010 start(201001) INCLUSIVE,
		subpartition sp2011 start(201101) INCLUSIVE,
		subpartition sp2012 start(201201) INCLUSIVE,
		subpartition sp2013 start(201301) INCLUSIVE,
		subpartition sp2014 start(201401) INCLUSIVE,
		subpartition sp2015 start(201501) INCLUSIVE,
		subpartition sp2016 start(201601) INCLUSIVE,
		subpartition sp2017 start(201701) INCLUSIVE,
		subpartition sp2018 start(201801) INCLUSIVE
		end(201901) EXCLUSIVE
	)
(
   partition optz values('optz'),
   partition trvc values('trvc'),
   partition trvm values('trvm'),
   partition optd values('optd'),
   partition mdcr values('mdcr')
   --default partition xxxx 
);


---------------------------------------------------------------------------------------------------

analyze data_warehouse.member_enrollment_monthly;

    ---------------- data loads --------------------

-- Optum DOD --------------------------------------------------------------------------------------
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
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.patid::text
   and a.data_source = 'optd'
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) and m.eligend
    and b.year_int between 2015 and 2017
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd 
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
;
---------------------------------------------------------------------------------------------------



-- Optum ZIP --------------------------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)
select 
	   'optz', b.month_year_id, a.uth_member_id,
       c.gender_cd, e.state, substring(zipcode_5,1,5), substring(zipcode_5,1,3),
       b.year_int - yrdob, (yrdob::varchar || '-12-31')::date, null, 
       d.plan_type, bus
from optum_zip.member m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.patid::text
   and a.data_source = 'optz'
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) and m.eligend
   and b.year_int between 2015 and 2017
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
  left outer join reference_tables.ref_zip_crosswalk e 
   on e.zip = substring(zipcode_5,1,5)
; 
---------------------------------------------------------------------------------------------------



-- Truven Commercial --------18 minutes ----------------------------------------------------------------------
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
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'trvc'
  join reference_tables.ref_truven_state_codes s 
    on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b 
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
    and b.year_int between 2015 and 2017
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp
;
---------------------------------------------------------------------------------------------------


-- Truven Medicare Advantage ----------------------------------------------------------------------
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
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'trvm'
  join reference_tables.ref_truven_state_codes s 
	on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
    and b.year_int between 2015 and 2017
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp
;
---------------------------------------------------------------------------------------------------


-- Medicare  --------------------------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)	
select 'mdcr', b.month_year_id, a.uth_member_id,
	   c.gender_cd,case when e.state_cd is null then 'XX' else e.state_cd end, m.zip_cd, substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, bene_death_dt::date,
	   'ABCD' as plan_type, 'MDCR'
from medicare.mbsf_abcd_summary m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mdcr'
  join reference_tables.ref_month_year b
    on b.year_int = bene_enrollmt_ref_yr::int
    and b.year_int between 2015 and 2017
   and 
   (	month_int = case when m.mdcr_entlmt_buyin_ind_01 in ('1','3','A','C') then 1 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_02 in ('1','3','A','C') then 2 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_03 in ('1','3','A','C') then 3 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_04 in ('1','3','A','C') then 4 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_05 in ('1','3','A','C') then 5 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_06 in ('1','3','A','C') then 6 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_07 in ('1','3','A','C') then 7 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_08 in ('1','3','A','C') then 8 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_09 in ('1','3','A','C') then 9 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_10 in ('1','3','A','C') then 10 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_11 in ('1','3','A','C') then 11 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_12 in ('1','3','A','C') then 12 else 0 end
    )
  left outer join reference_tables.ref_gender c
    on c.data_source = 'mdcr'
   and c.gender_cd_src = m.sex_ident_cd
  left outer join reference_tables.ref_medicare_state_codes e 
     on e.medicare_state_cd = m.state_code
 -- left outer join data_warehouse.ref_plan_type d
 --   on d.data_source = 'mdcr'
 -- and d.plan_type_src::int = m.plantyp
;
	


analyze data_warehouse.member_enrollment_monthly;


select dbo.set_all_perms();

select count(*), data_source , substring(month_year_id::text,1,4)
from data_warehouse.member_enrollment_monthly
group by data_source , substring(month_year_id::text,1,4);

--- create indexes -------------------------------------------------------------------------------- 
create index enrollment_id_index on data_warehouse.member_enrollment_monthly (uth_member_id);

create index enrollment_seq_index on data_warehouse.member_enrollment_monthly (id);

create index enrollment_data_source_index on data_warehouse.member_enrollment_monthly (data_source);



