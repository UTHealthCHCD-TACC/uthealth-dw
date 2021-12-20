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
analyze dw_staging.member_enrollment_monthly;

--- ***** Medicaid *****
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
	race_cd   
	)		
select 'mdcd', 
       substring(elig_date,1,4)::int2 as year, 
       elig_date::int as month_year_id, 
       b.uth_member_id, 
       a.sex, 
       z.state, 
       a.zip, 
       substring(a.zip,1,3) as zip3, 
       floor(a.age::float), 
       a.dob::date, 
       null as dth, 
       null as plan_type, 
       null as bus, 
       1 as rx, 
       year_fy, 
       r.race_cd
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
	race_cd   
	)	
select 'mdcd', 
       substring(elig_month,1,4)::int2 as year, 
       elig_month::int as month_year_id, 
       b.uth_member_id, 
       a.gender_cd, 
       z.state,  
       substring(a.mailing_zip,1,5), 
       substring(a.mailing_zip,1,3) as zip3, 
       floor(a.age::float), 
       to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD') as dob, 
       null as dth, 
       null as plan_type, 
       null as bus, 
       1 as rx, 
       year_fy, 
       r.race_cd
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