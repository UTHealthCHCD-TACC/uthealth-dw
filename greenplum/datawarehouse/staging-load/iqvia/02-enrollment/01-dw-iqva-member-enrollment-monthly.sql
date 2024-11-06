/*************************************************************************************************************
 * Script Purpose | Prepares IQVIA data to be inserted into the data_warehouse.member_enrollment_monthly
 * 				  | table. This script will create one record in the member_enrollment_monthly table for 
 * _______________| each month/year that a member was enrolled in coverage.
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 11/14/23 || Sharrah   || Script created.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 02/01/24 || Sharrah   || Script updated to take the year from the variable 'from_date'.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 03/18/24 || Sharrah   || Script updated to add monthly enrollment information for patients who only exist
 *          ||           || in the iqvia.claims table.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 04/04/24 || Sharrah   || Script updated to reverse changes made to script on 03/18/24.
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA member enrollment monthly ETL script started at: ' || current_timestamp as message;


--=== Create empty member enrollment monthly table for IQVIA: ===--

-- Drop existing table:
drop table if exists dw_staging.iqva_member_enrollment_monthly;

-- Create table:
create table dw_staging.iqva_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with(
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) 
distributed by (uth_member_id);

-- Alter table:
alter table dw_staging.iqva_member_enrollment_monthly add column row_id bigserial;
alter sequence dw_staging.iqva_member_enrollment_monthly_row_id_seq cache 200;



--=== Gather IQVIA monthly enrollment info and distribute the table on pat_id: ===-- 

-- Timestamp:
select 'Redistributing IQVIA enroll tables started at: ' || current_timestamp as message;

-- Drop existing table:
drop table if exists staging_clean.iqva_enroll_redistributed;

-- Note: There are no null pat_ids in either enroll tables
create table staging_clean.iqva_enroll_redistributed with(
	appendonly=true,
	orientation=column,
	compresstype=zlib
) 
as select 
	erl2.*,
	es.der_sex,
	es.der_yob,
	es.pat_state,
	es.pat_zip3,
	es.mh_cd 
  from iqvia.enroll2 erl2 left join iqvia.enroll_synth es on erl2.pat_id = es.pat_id 
  distributed by (pat_id);

-- Vacuum analyze:
vacuum analyze staging_clean.iqva_enroll_redistributed;



--=== Insert IQVIA enroll data into monthly enrollment table: ===--

-- Timestamp:
select 'Inserting IQVIA enroll data into dw_staging.iqva_member_enrollment_monthly started at: ' || current_timestamp as message;

insert into dw_staging.iqva_member_enrollment_monthly(
	data_source,
	year,
	uth_member_id,
	month_year_id,
	gender_cd,
	race_cd,
	age_cy,
	dob_derived,
	state,
	zip3,
	plan_type,
	bus_cd,
	rx_coverage,
	fiscal_year,
	behavioral_coverage,
	load_date,
	table_id_src,
	member_id_src
	)	
select 
       'iqva' as data_source, 
       substring(iq.month_id,1,4)::int as year,
       a.uth_member_id as uth_member_id,
       iq.month_id::int as month_year_id,
       case
	       when iq.der_sex is null then 'U' -- patients missing der_sex (NULL) will have gender_cd mapped as 'U'
       	   else iq.der_sex
       end as gender_cd,
       '0' as race_cd,
       case 
	       when iq.der_yob = '0000' or iq.der_yob = '0' then 86 
	       when iq.der_yob::int > substring(iq.month_id,1,4)::int then null 
	       else (substring(iq.month_id,1,4)::int - iq.der_yob::int) -- patients missing der_yob (NULL) will have age_cy mapped as null
	   end as age_cy,
       case 
	       when iq.der_yob = '0000' or iq.der_yob = '0' then null 
	       when iq.der_yob::int > substring(iq.month_id,1,4)::int then null 
	       else (der_yob || '/12/31')::date -- patients missing der_yob (NULL) will have dob_derived mapped as null
	   end as dob_derived,
       iq.pat_state as state,
       case
       		when iq.pat_zip3 !~ '[0-9]+' then null -- to handle values such as '???', '.', 'CA', or 'FL'
       		when iq.pat_zip3 = '0' then '000'
			when length(iq.pat_zip3) = 1 then null 
			when length(iq.pat_zip3) = 2 then rpad(iq.pat_zip3, 3, '0')
			else iq.pat_zip3
       end as zip3,
       case when iq.prd_type = '-' then null else c.plan_type end as plan_type, 
       case
	   		when iq.pay_type = 'C' then 'COM'
	   		when iq.pay_type = 'K' then 'CHIP'
	   		when iq.pay_type = 'M' then 'MDCD'
	   		when iq.pay_type = 'R' then 'MA'
	   		when iq.pay_type = 'S' then 'SI'
	   		when iq.pay_type = 'T' then 'MS'
	   		else null
	   end as bus_cd,
       case 
	       when iq.mstr_enroll_cd in ('Y', 'R') then 1 
	       when iq.mstr_enroll_cd = 'M' then 0
	       else null 
       end as rx_coverage, 
       b.fy_ut as fiscal_year, 
       case 
	       when iq.mh_cd = 'Y' then 1 
	       when iq.mh_cd = 'N' then 0 
	       when iq.mh_cd = 'U' then null 
	       else null 
	   end as behavioral_coverage,
       current_date as load_date,
       'enroll' as table_id_src,
       iq.pat_id as member_id_src
from staging_clean.iqva_enroll_redistributed iq
  join data_warehouse.dim_uth_member_id a 
    on a.member_id_src = iq.pat_id 
   and a.data_source = 'iqva'
  left join reference_tables.ref_month_year b
  	on b.month_year_id = iq.month_id::int
  left join reference_tables.ref_plan_type c
  	on c.data_source  = 'iqva' 
   and c.plan_type_src = iq.prd_type;



--=== Populate the 'consecutive_enrolled_months' column for IQVIA: ===--

-- Timestamp:
select 'Get consecutive enrollment for IQVIA started at: ' || current_timestamp as message;

-- Drop existing table:
drop table if exists dev.iqva_temp_consec_enrollment;

create table dev.iqva_temp_consec_enrollment
with (appendonly = true, orientation = column) as 
select row_id::bigint as row_id,
       row_number() over(partition by uth_member_id, my_grp order by month_year_id) as in_streak
from ( 
	   select a.row_id,
	          a.month_year_id,
	          a.uth_member_id,
	          b.my_row_counter - row_number() over(partition by a.uth_member_id order by a.month_year_id) as my_grp
	   from dw_staging.iqva_member_enrollment_monthly a 
	     join reference_tables.ref_month_year b 
	       on a.month_year_id = b.month_year_id 	   		    
	 )x    
distributed by (row_id);

-- Analyze:
analyze dev.iqva_temp_consec_enrollment;

-- Update the 'consecutive_enrolled_months' column:
update dw_staging.iqva_member_enrollment_monthly a 
   set consecutive_enrolled_months = b.in_streak 
  from dev.iqva_temp_consec_enrollment b 
 where a.row_id = b.row_id;



--== Cleanup ===--

-- Timestamp:
select 'IQVIA cleanup started at: ' || current_timestamp as message;

-- Drop existing tables that are no longer needed:
drop table if exists staging_clean.iqva_enroll_redistributed;
drop table if exists dev.iqva_temp_consec_enrollment;

-- Remove row_id from the monthly enrollment table:
alter table dw_staging.iqva_member_enrollment_monthly drop column row_id;

-- Vacuum analyze:
vacuum analyze dw_staging.iqva_member_enrollment_monthly;

-- Grant Access:
grant select on dw_staging.iqva_member_enrollment_monthly to uthealth_analyst;


-- Final timestamp:
select 'IQVIA member enrollment monthly ETL script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--= Various checks: =--

-- View populated table:
--select * from dw_staging.iqva_member_enrollment_monthly order by uth_member_id, month_year_id limit 1000; 

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Total row count:
select 'dw_staging.iqva_member_enrollment_monthly total row count: ' as message, count(*) from dw_staging.iqva_member_enrollment_monthly; -- CNT: 4058328099

-- Number of rows from the raw enroll2 table:
select 'iqvia.enroll2 total row count: ' as message, count(*) from iqvia.enroll2; -- CNT: 4058328099

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Number of patients:
select 'dw_staging.iqva_member_enrollment_monthly total number of patients (uth_member_id): ' as message, count(distinct uth_member_id) from dw_staging.iqva_member_enrollment_monthly; -- CNT: 115613220
select 'dw_staging.iqva_member_enrollment_monthly total number of patients (member_id_src): ' as message, count(distinct member_id_src) from dw_staging.iqva_member_enrollment_monthly; -- CNT: 115613220

-- Number of patients from the raw enroll2 table:
select 'iqvia.enroll2 total patient count (pat_id)' as message, count(distinct pat_id) from iqvia.enroll2; -- CNT: 115613220

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct years:
select distinct year from dw_staging.iqva_member_enrollment_monthly order by 1; -- 2006 thru 2023

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Ensure there are no nulls in consecutive_enrolled_months;
select * from dw_staging.iqva_member_enrollment_monthly where consecutive_enrolled_months is null;  -- no rows returned




