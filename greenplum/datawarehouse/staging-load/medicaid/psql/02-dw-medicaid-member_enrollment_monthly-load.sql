/*********************************************************************
 * Script purpose:
 * 	Medicaid enrollment data was cleaned in previous script,
 * 	Now just needs to be split into
 * 		CHIP Perinatal
 * 		HTW
 * 		General Medicaid
 * 	CHIP Perinatal and HTW will remain at the month-year level without
 * 	any yearly aggregation
 * 
 * Author: Xiaorui					Date: 03/28/23
 * 
 * Author 	| Date 		| Change
 * ------------------------------------------------------
 * Xiaorui	| 08/04/23	| Modified the dim_uth_member_id script so that people are assigned the correct
 * 						  data_source up-front; Changed this script so that it looks for the uth_member_id
 * 						  in the correct place
 * 						  Also made dual text instead of int (Accomodate Medicare's 'P' dual status)
 * 						  
 *********************************************************************/

--TWEAKED FOR PSQL

select 'Medicaid Monthly Enroll Load script started at ' || current_timestamp as message;

/**************************************************
 * Initialize empty monthly enrollment table
 **************************************************/
--Create general Medicaid enrollment table
drop table if exists dw_staging.mcd_member_enrollment_monthly;

create table dw_staging.mcd_member_enrollment_monthly  
(like data_warehouse.member_enrollment_monthly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);

--adds a row_id that is used later to calculate consecutive_enrolled_months
alter table dw_staging.mcd_member_enrollment_monthly add column row_id bigserial;
alter sequence dw_staging.mcd_member_enrollment_monthly_row_id_seq cache 200;

vacuum analyze dw_staging.mcd_member_enrollment_monthly;

select 'Loading data in: ' || current_timestamp as message;

--select * from dw_staging.mcd_member_enrollment_monthly;

/********************************************
 * Load data into monthly table
 *******************************************/
insert into dw_staging.mcd_member_enrollment_monthly (
	data_source, 
	year,
	fiscal_year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	dob_derived,
	age_cy,
	age_fy,
	plan_type, 
	rx_coverage, 
	race_cd,
	dual,
	htw,
	age_months,
	table_id_src,
	member_id_src,
	load_date 
	)		
	select 
	case when plan_type = 'CHIP PERI' then 'mcpp'
		when me_code = 'W' then 'mhtw'
		else 'mdcd' end as data_source,
	year,
	year_fy,
	month_year_id,
	null as uth_member_id,
	sex,
	state,
	zip,
	zip3,
	dob,
	extract( years from age(yr_end_date, dob)) as age_cy,
	extract( years from age(fy_end_date, dob)) as age_fy,
	plan_type,
	1 as rx_coverage,
	race,
	case 
       	when smib = '1' then '1' else '0'
       end as dual,
    case 
       	when me_code = 'W' then 1 else 0
       end as htw,
    ((extract(months from age(elig_date_month, dob))) + ((extract(years from age(elig_date_month, dob))) * 12)) as months_old,
    table_id_src,
    client_nbr as member_id_src,
    current_date as load_date
from dw_staging.medicaid_enroll_etl;

select 'Vacuum analyze: ' || current_timestamp as message;
vacuum analyze dw_staging.mcd_member_enrollment_monthly;


select 'Updating uth_member_id: ' || current_timestamp as message;

--insert in uth_member_id
update dw_staging.mcd_member_enrollment_monthly a
set uth_member_id = b.uth_member_id
from data_warehouse.dim_uth_member_id b
where b.data_source in ('mdcd', 'mhtw', 'mcpp') and
	a.data_source = b.data_source and
	a.member_id_src = b.member_id_src;

select 'Vacuum analyze: ' || current_timestamp as message;

vacuum analyze dw_staging.mcd_member_enrollment_monthly;

--check to see if that worked
--select * from dw_staging.mcd_member_enrollment_monthly where uth_member_id is null;

select 'Build consecutive months: ' || current_timestamp as message;

---**script to build consecutive enrolled months	
drop table if exists dev.temp_consec_enrollment;

create table dev.temp_consec_enrollment 
with (appendonly=true, orientation=column) as 
select row_id::bigint as row_id
      ,row_number() over(partition by data_source, uth_member_id, my_grp order by  month_year_id) as in_streak
from ( 
	   select a.data_source,
	   		a.row_id,
	        a.month_year_id,
	        a.uth_member_id,
	        b.my_row_counter - row_number() over(partition by a.data_source, a.uth_member_id
	        	order by a.month_year_id) as my_grp
	   from dw_staging.mcd_member_enrollment_monthly a 
	     join reference_tables.ref_month_year b 
	       on a.month_year_id = b.month_year_id 	   		    
	 ) inr    
distributed by (row_id);

analyze dev.temp_consec_enrollment;

--update consec enrolled months  9m 
update dw_staging.mcd_member_enrollment_monthly a 
   set consecutive_enrolled_months = b.in_streak 
  from dev.temp_consec_enrollment b 
 where a.row_id = b.row_id;

select 'Cleanup and final vacuum analyze: ' || current_timestamp as message;

--**cleanup
drop table if exists dev.temp_consec_enrollment;

---/drop sequence, rebuild table distributed on uth_member_id 
alter table dw_staging.mcd_member_enrollment_monthly drop column row_id;
vacuum analyze dw_staging.mcd_member_enrollment_monthly;
alter table dw_staging.mcd_member_enrollment_monthly owner to uthealth_dev;

--check
--select * from dw_staging.mcd_member_enrollment_monthly;


