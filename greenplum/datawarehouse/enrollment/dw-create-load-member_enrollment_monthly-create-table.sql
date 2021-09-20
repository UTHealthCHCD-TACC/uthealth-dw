/* ******************************************************************************************************
 *  The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 *  This file runs the cleanup for duplicate rows and gets the value for consecutive enrolled months
 *  Run the relevant code section for the dataset in (---------------- data loads --------------------)
 * 
 *    
 * !!!!!!!!!    dw_staging.member_enrollment_monthly must be populated first  !!!!!!!!!   
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
) distributed by (uth_member_id);



---
                                                                           
alter sequence dw_staging.member_enrollment_monthly_row_id_seq cache 200;

vacuum analyze dw_staging.member_enrollment_monthly;

----/END SCRIPT
