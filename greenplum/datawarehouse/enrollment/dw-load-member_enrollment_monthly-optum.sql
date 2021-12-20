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
 *  wc004  || 11/06/2021 || moved table creation to new script. formatting. changed bus_cd mapping
 *  ******************************************************************************************************
*/



----  // BEGIN SCRIPT 



-- ***** Optum DOD ***** 18mins--------------------------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_derived, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	family_id 
	)		
select 'optd', 
	   b.year_int, 
	   b.month_year_id, 
	   a.uth_member_id,
       c.gender_cd, 
       m.state, 
       null, 
       null, 
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, 
       case when death_ym is null then null else to_date(death_ym::text,'YYYYMM')  end as death_dt,  
       d.plan_type, 
       case when bus = 'MCR' then 'MA' when bus = 'COM' then 'COM' else 'UNK' end as bus_cd, 
       1 as rx, 
       b.fy_ut, 
       r.race_cd,
       m.family_id::text
from optum_dod.mbr_enroll_r m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.member_id_src 
   and a.data_source = 'optd'
  left outer join optum_dod.mbrwdeath dth 
    on dth.patid = m.patid 
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) 
                        and case when dth.death_ym is not null then to_date(death_ym::text,'YYYYMM') 
                            else m.eligend 
                            end   ---wcc002
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
----------------------------------------------------------------------

-- ***** Optum ZIP ***** --------------------------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly  (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_derived, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	family_id 
	)	
select 
	   'optz',
	   b.year_int, 
	   b.month_year_id, 
	   a.uth_member_id,
       c.gender_cd, 
       e.state, 
       substring(zipcode_5,1,5), 
       substring(zipcode_5,1,3),
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, 
       null, 
       d.plan_type, 
       case when bus = 'MCR' then 'MA' when bus = 'COM' then 'COM' else 'UNK' end as bus_cd, 
       1 as rx, 
       b.fy_ut , 
       r.race_cd,
       m.family_id::text 
from optum_zip.mbr_enroll m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.member_id_src 
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

analyze dw_staging.member_enrollment_monthly;

----END SCRIPT

select count(*), data_source, year 
from dw_staging.member_enrollment_monthly 
group by 2,3 order by 2,3;