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
 *********************************************************************/

/**************************************************
 * Initialize empty monthly enrollment tables
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
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn'),
  partition mhtw values ('mhtw'),
  partition mcpp values ('mcpp')
 );

--adds a row_id that is used later to calculate consecutive_enrolled_months
alter table dw_staging.mcd_member_enrollment_monthly add column row_id bigserial;
alter sequence dw_staging.mcd_member_enrollment_monthly_row_id_seq cache 200;

/********************************************
 * Load data into monthly table
 *******************************************/
insert into dw_staging.mcd_member_enrollment_monthly (
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
	select 
	case when a.plan_type = 'CHIP PERI' then 'mcpp'
		when a.me_code = 'W' then 'mhtw'
		else 'mdcd' end as data_source,
	year, 
	month_year_id,
	b.uth_member_id,
	a.sex,
	a.state,
	a.zip,
	a.zip3,
	extract( years from age(a.yr_end_date, dob)),
	dob,
	null as death_date,
	a.plan_type,
	null as bus_cd, 
	1 as rx_coverage,
	year_fy,
	a.race,
	case 
       	when a.smib = '1' then 1 else 0
       end as dual,
    case 
       	when a.me_code = 'W' then 1 else 0
       end as htw,
    ((extract(months from age(elig_date_month, dob))) + ((extract(years from age(elig_date_month, dob))) * 12)) as months_old,
    a.table_id_src,
    a.client_nbr as member_id_src,
    current_date as load_date
from dw_staging.medicaid_enroll_etl a 
  join data_warehouse.dim_uth_member_id b 
     on b.data_source = 'mdcd' 
    and b.member_id_src = a.client_nbr;

vacuum analyze dw_staging.mcd_member_enrollment_monthly;

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

--**cleanup
drop table if exists dev.temp_consec_enrollment;

---/drop sequence, rebuild table distributed on uth_member_id 
alter table dw_staging.mcd_member_enrollment_monthly drop column row_id;
vacuum analyze dw_staging.mcd_member_enrollment_monthly;
alter table dw_staging.mcd_member_enrollment_monthly owner to uthealth_dev;


--check
--select * from dw_staging.mcd_member_enrollment_monthly;


