/*
 * The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!   
 *   	             Use dw-create-load-dim_member_id_src.sql in Git      
 */

drop table if exists data_warehouse.member_enrollment_monthly cascade;


create table data_warehouse.member_enrollment_monthly (
	data_source char(4), 
	year int2,
	month_year_id int4,
	uth_member_id bigint,
	consecutive_enrolled_months int2,
	gender_cd char(1),
	state varchar,
	zip5 char(5),
	zip3 char(3),
	age_derived int,
	dob_derived date, 
	death_date date,
	plan_type char(4),
	bus_cd char(4),
	employee_status text, 
	claim_created_flag bool default false,
	row_identifier bigserial,
	rx_coverage int2
)
WITH (appendonly=true, orientation=column)
distributed by(uth_member_id);


alter sequence data_warehouse.member_enrollment_monthly_row_identifier_seq cache 200;

vacuum analyze data_warehouse.member_enrollment_monthly;

    ---------------- data loads --------------------
    
delete from data_warehouse.member_enrollment_monthly where data_source in ('optd','optz');

-- Optum DOD --------------------------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)	
select 'optd', b.year_int, b.month_year_id, a.uth_member_id,
       c.gender_cd, state, null, null, 
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, (select max(death_ym) from optum_dod.mbrwdeath dod where dod.patid = m.patid ) as death_dt,  
       d.plan_type, bus
from optum_dod.mbr_enroll m
  join data_warehouse.dim_uth_member_id a
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
;
---------------------------------------------------------------------------------------------------

vacuum analyze data_warehouse.member_enrollment_monthly;


-- Optum ZIP --------------------------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)
select 
	   'optz',b.year_int, b.month_year_id, a.uth_member_id,
       c.gender_cd, e.state, substring(zipcode_5,1,5), substring(zipcode_5,1,3),
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, null, 
       d.plan_type, bus
from optum_zip.mbr_enroll m
  join data_warehouse.dim_uth_member_id a
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
  left outer join reference_tables.ref_zip_crosswalk e 
   on e.zip = substring(zipcode_5,1,5)
; 
---------------------------------------------------------------------------------------------------


vacuum analyze data_warehouse.member_enrollment_monthly;

delete from data_warehouse.member_enrollment_monthly where data_source = 'truv';

-- Truven Commercial ----------------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, employee_status, rx_coverage         
	)		
select 
	   'truv', b.year_int, b.month_year_id, a.uth_member_id,
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, trunc(m.empzip,0)::text,
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null, 
       d.plan_type, 'COM', eestatu, m.rx
from truven.ccaet m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'truv'
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
---------------------------------------------------------------------------------------------------

create table dev.truven_mdcrt
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from truven.mdcrt
distributed by(enrolid);



-- Truven Medicare Advantage ----------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, employee_status, rx_coverage      
	)		
select 
       'truv', b.year_int,b.month_year_id, a.uth_member_id,
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, trunc(m.empzip,0)::text,
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null,
       d.plan_type, 'MCR', eestatu, m.rx
from dev.truven_mdcrt m  
--truven.mdcrt m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'truv'
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
---------------------------------------------------------------------------------------------------


-- Medicare  --------------------------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)	
select 'mdcr',b.year_int, b.month_year_id, a.uth_member_id,
	   c.gender_cd,case when e.state_cd is null then 'XX' else e.state_cd end, m.zip_cd, substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, bene_death_dt::date,
	   'ABCD' as plan_type, 'MDCR'
from medicare_texas.mbsf_abcd_summary m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mdcr'
  join reference_tables.ref_month_year b
    on b.year_int = bene_enrollmt_ref_yr::int
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
;
	


-- Medicare National --------------------------------------------------------------------------------------
insert into data_warehouse.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd         
	)	
select 'mcrn',b.year_int, b.month_year_id, a.uth_member_id,
	   c.gender_cd,case when e.state_cd is null then 'XX' else e.state_cd end, m.zip_cd, substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, bene_death_dt::date,
	   'ABCD' as plan_type, 'MDCR'
from medicare_national.mbsf_abcd_summary m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mcrn'
  join reference_tables.ref_month_year b
    on b.year_int = bene_enrollmt_ref_yr::int
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
;


---------------------------/End---------------------------------------

vacuum analyze data_warehouse.member_enrollment_monthly;



select count(*), data_source , year
from data_warehouse.member_enrollment_monthly
group by data_source , year
order by data_source , year 



select count(*) , year 
from truven.ccaea 
group by year;
--- 

---logic to add consecutive enrolled months
---------------------------------------------


---- delete duplicate rows
delete from data_warehouse.member_enrollment_monthly where row_identifier in ( 
--select * from data_warehouse.member_enrollment_monthly mem where row_identifier in ( 
	select row_identifier
	from
	(		
	select row_number() over(partition by uth_member_id, month_year_id order by month_year_id) as rn
		      ,*
		from data_warehouse.member_enrollment_monthly 		
	) sub
	where rn > 1
);



with row_build_cte as ( 
	select row_identifier 
	      ,row_number() over(partition by uth_member_id, my_grp order by  month_year_id) as in_streak
	from ( 
		   select a.row_identifier
		         ,a.month_year_id
		         ,a.uth_member_id
		         ,b.my_row_counter - row_number() over(partition by a.uth_member_id order by a.month_year_id) as my_grp
		   from data_warehouse.member_enrollment_monthly 	 a 
		     join reference_tables.ref_month_year b 
		       on a.month_year_id = b.month_year_id 	   		    
		 ) sub    
) 
update data_warehouse.member_enrollment_monthly c 
set consecutive_enrolled_months = d.in_streak 
from row_build_cte d
where c.row_identifier = d.row_identifier
;

vacuum analyze data_warehouse.member_enrollment_monthly;

select data_source, uth_member_id, month_year_id, consecutive_enrolled_months from data_warehouse.member_enrollment_monthly where data_source = 'truv';


----


select count(*), data_source, year 
from data_warehouse.member_enrollment_monthly mem 
group by data_source, year 
order by data_source, year 

