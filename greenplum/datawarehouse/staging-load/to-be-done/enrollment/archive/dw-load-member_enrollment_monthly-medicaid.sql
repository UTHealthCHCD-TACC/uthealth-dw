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
 *  jw	   || 10/10/2022 || added htw and months old and changed age derived and added dual and htw flag
 *  ******************************************************************************************************
*/


----  // BEGIN SCRIPT 
vacuum analyze dw_staging.member_enrollment_monthly;

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
	race_cd,
	dual,
	htw,
	age_months,
	table_id_src,
	member_id_src,
	load_date 
	)		
select 'mdcd', 
       substring(elig_date,1,4)::int2 as year, 
       elig_date::int as month_year_id, 
       b.uth_member_id, 
       a.sex, 
       z.state, 
       a.zip, 
       substring(a.zip,1,3) as zip3, 
       extract ( years from age(
		       ('12-31-' || substring(elig_date,1,4))::Date,
		       dob::date
       			)) as age_derived,
       a.dob::date as dob_derived, 
       null as death_date , 
       c.mco_program_nm  as plan_type, 
       null as bus, 
       1 as rx, 
       year_fy, 
       r.race_cd,
       case 
       	when a.me_sd = 'Q' then 1 
       	else 0
       end as dual,
       0 as htw,
       extract ( months from 
			       age((substring(elig_date,5,6) || '-01-' || substring(elig_date,1,4))::Date,
			       dob::date )) +
		       extract ( years from 
			       age((substring(elig_date,5,6) || '-01-' || substring(elig_date,1,4))::Date,
			       dob::date
			       )) * 12    as months_old,
	 'enrl' as table_id_src,
	 a.client_nbr as member_id_src,
	 current_date as load_date
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
	race_cd,
	dual,
	htw,
	age_months,
	table_id_src,
	member_id_src,
	load_date 
	)	
select 'mdcd', 
       substring(elig_month,1,4)::int2 as year, 
       elig_month::int as month_year_id, 
       b.uth_member_id, 
       a.gender_cd, 
       z.state,  
       substring(a.mailing_zip,1,5), 
       substring(a.mailing_zip,1,3) as zip3, 
       extract ( years from age(
		       ('12-31-' || substring(elig_month,1,4))::Date,
		       to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD')
       			)) as age_derived, 
       to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD') as dob, 
       null as dth, 
       'CHIP' as plan_type, 
       null as bus, 
       1 as rx, 
       year_fy, 
       r.race_cd,
       0 as dual,
       0 as htw,
	       extract ( months from 
	       age((substring(elig_month,5,6) || '-01-' || substring(elig_month,1,4))::Date,
	       to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD')
	       )) +
	       extract ( years from 
	       age((substring(elig_month,5,6) || '-01-' || substring(elig_month,1,4))::Date,
	       to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD')
	       )) * 12 as months_old,
	      'chip_uth' as table_id_src,
	      a.client_nbr as member_id_src,
	      current_date as load_date
from medicaid.chip_uth  a 
  join data_warehouse.dim_uth_member_id b  
     on b.data_source = 'mdcd' 
    and b.member_id_src = a.client_nbr 
  left outer join reference_tables.ref_zip_code z 
     on substring(a.mailing_zip,1,5) = z.zip 
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = a.ethnicity 
    and r.data_source = 'mdcd'
;

vacuum analyze dw_staging.member_enrollment_monthly;

/*
---------htw----------------------
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
	race_cd,
	dual,
	htw,
	age_months 
	)	
select 'mdcd', 
       substring(elig_date,1,4)::int2 as year, 
       elig_date::int as month_year_id, 
       b.uth_member_id, 
       a.sex, 
       z.state, 
       a.zip, 
       substring(a.zip,1,3) as zip3, 
       extract ( years from age(
		       ('12-31-' || substring(elig_date,1,4))::Date,
		       dob::date
       			)) as age_derived,
       a.dob::date as dob_derived, 
       null as dth, 
       c.mco_program_nm  as plan_type, 
       null as bus, 
       1 as rx, 
       case 
	   		when substring(elig_date,5,6)::int between 9 and 12 
	   		then substring(elig_date,1,4)::int + 1
	 	else substring(elig_date,1,4)::int
	   end  as derp, 
       r.race_cd,
       case 
       	when a.me_sd = 'Q' then 1 
       	else 0
       end as dual,
       1 as htw,
       extract ( months from 
			       age((substring(elig_date,5,6) || '-01-' || substring(elig_date,1,4))::Date,
			       dob::date )) +
		       extract ( years from 
			       age((substring(elig_date,5,6) || '-01-' || substring(elig_date,1,4))::Date,
			       dob::date
			       )) * 12    as months_old
from medicaid.htw_enrl  a 
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


vacuum analyze dw_staging.member_enrollment_monthly;


*/



----END SCRIPT