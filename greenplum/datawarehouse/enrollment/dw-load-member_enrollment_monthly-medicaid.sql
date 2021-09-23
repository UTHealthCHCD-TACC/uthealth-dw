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

create table dw_staging.member_enrollment_monthly  (
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
where data_source not in ('mdcd')
;

vacuum analyze dw_staging.member_enrollment_monthly;

--- ***** Medicaid *****
insert into dw_staging.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage, fiscal_year, race_cd   
	)		
select 'mdcd', substring(elig_date,1,4)::int2 as year, elig_date::int as my, b.uth_member_id, 
       a.sex, z.state, a.zip, substring(a.zip,1,3) as zip3, 
       floor(a.age::float), a.dob::date, null as dth, 
       null as plan_type, 'MCD' as bus, 1 as rx, year_fy , r.race_cd
from medicaid.enrl  a 
  join data_warehouse.dim_uth_member_id b  
     on b.data_source = 'mdcd'
    and b.member_id_src = a.client_nbr 
  left outer join reference_tables.ref_zip_code z 
     on a.zip = z.zip 
  left outer join reference_tables.medicaid_lu_contract c 
     on c.plan_cd = a.contract_id 
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = a.race 
    and r.data_source = 'mdcd'
;

---medicaid chip
insert into dw_staging.member_enrollment_monthly(
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage  ,fiscal_year, race_cd   
	)	
select 'mdcd', substring(elig_month,1,4)::int2 as year, elig_month::int as my, b.uth_member_id, 
       a.gender_cd , z.state,  substring(a.mailing_zip,1,5) , substring(a.mailing_zip,1,3) as zip3, 
       floor(a.age::float), to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD') as dob, null as dth, 
       null as plan_type, 'MCD' as bus, 1 as rx, year_fy , r.race_cd
from medicaid.chip_uth  a 
  join data_warehouse.dim_uth_member_id b  
     on b.data_source = 'mdcd'
    and b.member_id_src = a.client_nbr 
  left outer join reference_tables.ref_zip_code z 
     on  substring(a.mailing_zip,1,5) = z.zip 
  left outer join reference_tables.medicaid_lu_contract c 
     on c.plan_cd = a.plan_cd 
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = a.ethnicity 
    and r.data_source = 'mdcd'
;



----END SCRIPT