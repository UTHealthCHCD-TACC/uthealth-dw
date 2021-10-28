/* ******************************************************************************************************
 *  The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 *  Run the relevant code section for the dataset in (---------------- data loads --------------------)
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!    
 *   	             Use dw-create-load-dim_member_id_src.sql in Git    
 * 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wc002  || 6/28/21 || added logic to exclude enrollment records after death optum dod
 * ******************************************************************************************************
 *  wallingTACC  || 8/23/2021 || Cleaning up comments
 * ******************************************************************************************************
 *  wc003  || 9/02/2021 || Changing process to load dw_staging. Add mapping for null race to assign 0 (Unknown).
 * ******************************************************************************************************
 *  jw001  || 9/20/2021 || Cut to its own script file from longer file
 *  ******************************************************************************************************
*/



----  // BEGIN SCRIPT 

---create working table in dw_staging 
drop table if exists dw_staging.member_enrollment_monthly ;

create table dw_staging.member_enrollment_monthly 
(
	data_source char(4),
	year int2, 
	uth_member_id bigint,
	month_year_id int4, 
	consecutive_enrolled_months int4, 
	gender_cd char(1), 
	race_cd char(1),
	age_derived int4, 
	dob_derived date, 
	state text, 
	zip5 char(5), 
	zip3 char(3), 
	death_date date, 
	plan_type text, 
	bus_cd char(4), 
	employee_status text, 
	claim_created_flag boolean default false,
	rx_coverage int2, 
	fiscal_year int2,
	row_id bigserial
) distributed by (row_id);

                                                                        
alter sequence dw_staging.member_enrollment_monthly_row_id_seq cache 200;


-------------insert existing records from data warehouse. except for this data source
insert into dw_staging.member_enrollment_monthly 
select * 
from data_warehouse.member_enrollment_monthly 
where data_source not in ('optd','optz')
;

vacuum analyze dw_staging.member_enrollment_monthly;

-- ***** Optum DOD ***** --------------------------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage, fiscal_year, race_cd       
	)		
select 'optd', b.year_int, b.month_year_id, a.uth_member_id,
       c.gender_cd, m.state, null, null, 
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, 
       case when death_ym is null then null else death_ym end as death_dt,  
       d.plan_type, bus, 1 as rx, b.year_int, r.race_cd 
from optum_dod.mbr_enroll_r m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.patid::text
   and a.data_source = 'optd'
  left outer join optum_dod.mbrwdeath dth 
    on dth.patid = m.patid 
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) and case when dth.death_ym is not null then dth.death_ym else m.eligend end   ---wcc002
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd 
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
  left outer join reference_tables.ref_race r 
    on r.race_cd_src = m.race 
   and r.data_source = 'optd'
;
---------------------------------------------------------------------------------------------------

-- ***** Optum ZIP ***** --------------------------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly  (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage, fiscal_year, race_cd         
	)	
select 
	   'optz',b.year_int, b.month_year_id, a.uth_member_id,
       c.gender_cd, e.state, substring(zipcode_5,1,5), substring(zipcode_5,1,3),
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, null, 
       d.plan_type, bus, 1 as rx, b.year_int, r.race_cd  
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
    left outer join reference_tables.ref_race r --wc003 
    on r.race_cd_src = null
   and r.data_source = 'optz' 
;


----END SCRIPT