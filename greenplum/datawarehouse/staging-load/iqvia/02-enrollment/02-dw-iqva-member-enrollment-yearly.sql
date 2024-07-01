/*************************************************************************************************************
 * Script Purpose | Prepares IQVIA data to be inserted into the data_warehouse.member_enrollment_yearly 
 * 				  | table. This script will delete and recreate member_enrollment_yearly records based on 
 * _______________| member_enrollment_monthly for a given dataset.
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 11/22/23 || Sharrah   || Script created.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 04/04/24 || Sharrah   || Script updated to add yearly enrollment information for patients who only exist
 *          ||           || in the iqvia.claims table, and for patients who have claims for a year they were 
 *          ||           || not enrolled in.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 04/22/24 || Sharrah   || Script updated to first, pull patient demographic information from the 
 *          ||           || enroll_synth table, and then after the enroll_synth_old table.
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA member enrollment yearly ETL script started at: ' || current_timestamp as message;


--=== Create empty member enrollment yearly table for IQVIA: ===--

-- Drop existing table:
drop table if exists dw_staging.iqva_member_enrollment_yearly;

-- Create table:
create table dw_staging.iqva_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);



--=== Insert IQVIA enroll data into dw_staging.iqva_member_enrollment_yearly from dw_staging.iqva_member_enrollment_monthly: ===--

-- Timestamp:
select 'Inserting IQVIA enroll data from dw_staging.iqva_member_enrollment_monthly started at: ' || current_timestamp as message;

insert into dw_staging.iqva_member_enrollment_yearly(
         data_source, 
         year, 
         uth_member_id, 
         gender_cd,
         race_cd,
		 age_derived, 
		 dob_derived,
		 state,
		 zip3,
		 plan_type,
		 bus_cd, 
		 rx_coverage, 
		 behavioral_coverage,
		 load_date,
         member_id_src,
         table_id_src)
select distinct on(year, uth_member_id)
        data_source, 
        year, 
        uth_member_id,
        gender_cd,
        race_cd,
	    age_cy, 
	    dob_derived,
	    state,
	    zip3,
	    plan_type,
        bus_cd,
	    rx_coverage,
	    behavioral_coverage,
	    current_date,
	    member_id_src,
	    table_id_src 
from dw_staging.iqva_member_enrollment_monthly;

 -- Analyze:
analyze dw_staging.iqva_member_enrollment_yearly;



--=== Insert IQVIA data into dw_staging.iqva_member_enrollment_yearly for patients who have claims for a year they were not enrolled in: ===--

/*************************************************************************************************************************
 * 
 * Note: 
 * 	Below, pat_ids and years (substring(month_id, 1, 4)) are pulled from the iqvia.claims 
 *  table (dev.sa_iqvia_derv_claimno) for patients who do not exist in the enroll2 table
 *  and for patients who have claims that occured in years they were not enrolled in.
 * 	Additionally, uth_member_ids (from the data_warehouse.uth_member_id table) and additional 
 *  demographic information (from the iqvia.enroll_synth and iqvia.enroll_synth_old tables) are collected
 * 	for these patients. This information will then be inserted into dw_staging.iqva_member_enrollment_yearly with
 * 	the claim_created_flag set to true. 
 * 
 * 	This is to create rows with claim_created_flag = T for the year(s) that a patient is not enrolled in,
 *  but has a claim in. 
 * 
 *  Total_enrolled_months for these rows will be zero.
 * 
 * 
 *************************************************************************************************************************/

--== Pull pat_ids and years (substring(month_id, 1, 4)) from the iqvia.claims table (dev.sa_iqvia_derv_claimno) for patients 
--== who do not exist in the enroll2 table and for patients who have claims that occured in years they were not enrolled in:

-- Drop existing table:
drop table if exists staging_clean.iqva_claim_created_pat_ids_years;

create table staging_clean.iqva_claim_created_pat_ids_years with(
	appendonly=true,
	orientation=column,
	compresstype=zlib
) as
	select distinct a.pat_id, substring(a.month_id, 1, 4) as year
	from dev.sa_iqvia_derv_claimno a -- iqvia.claims table with the generated derv_claimnos
		left join iqvia.enroll2 erl2
			on a.pat_id = erl2.pat_id 
	       and substring(a.month_id, 1, 4) = substring(erl2.month_id, 1, 4)	
	where erl2.pat_id is null and erl2.month_id is null; -- find where there are no matches on pat_id and year (substring(month_id), 1, 4)) between iqvia.claims and iqvia.enroll2. This will be the yearly info that needs to be added to enrollment_yearly table

-- Vacuum analyze:
vacuum analyze staging_clean.iqva_claim_created_pat_ids_years;



--== First, grab demographic information from enroll_synth:

-- Drop existing table:
drop table if exists staging_clean.iqva_claim_created_enroll_synth_demographics;

-- Note: There are no null pat_ids in either enroll tables
create table staging_clean.iqva_claim_created_enroll_synth_demographics with(
	appendonly=true,
	orientation=column,
	compresstype=zlib
) 
as select 
	a.*, 
	es.der_sex,
	es.der_yob,
	es.pat_state,
	es.pat_zip3,
	es.mh_cd 
   from staging_clean.iqva_claim_created_pat_ids_years a -- table containing pat_ids and years (substring(month_id, 1, 4)) from iqvia.claims that do not exist in enroll2
		left join iqvia.enroll_synth es -- gather demographic info for a patient, if it exists, from the enroll_synth table
			on a.pat_id = es.pat_id
   where es.pat_id is not null -- filter where pat_id is not null in the enroll_synth table to retain patient demographic information for pat_ids that exist in both iqvia.claims and iqvia.enroll_synth
   distributed by (pat_id);

-- Vacuum analyze:
vacuum analyze staging_clean.iqva_claim_created_enroll_synth_demographics;



--== Next, grab demographic information from enroll_synth_old for pat_ids that are not in the enroll_synth table:

-- Drop existing table:
drop table if exists staging_clean.iqva_claim_created_enroll_synth_old_demographics;

-- Note: There are no null pat_ids in either enroll tables
create table staging_clean.iqva_claim_created_enroll_synth_old_demographics with(
	appendonly=true,
	orientation=column,
	compresstype=zlib
) 
as select 
	a.*,
	eso.der_sex,
	eso.der_yob,
	eso.pat_state,
	eso.pat_zip3,
	eso.mh_cd 
   from (
   		select x.* 
  		from staging_clean.iqva_claim_created_pat_ids_years x -- table containing pat_ids and years (substring(month_id, 1, 4)) from iqvia.claims that do not exist in enroll2
  			left join staging_clean.iqva_claim_created_enroll_synth_demographics y
  				on x.pat_id = y.pat_id 
  		where y.pat_id is null -- join the staging_clean.iqva_claim_created_pat_ids_years table to staging_clean.iqva_claim_created_enroll_synth_demographics and filter for where there are no matches on pat_id. These will be the pat_ids that are not in the enroll_synth table
  	) a 
  		left join iqvia.enroll_synth_old eso -- gather demographic info for a patient, if it exists, from the enroll_synth_old table
			on a.pat_id = eso.pat_id
   distributed by (pat_id);
  
-- Vacuum analyze:
vacuum analyze staging_clean.iqva_claim_created_enroll_synth_old_demographics;



--== Insert data into dw_staging.iqva_member_enrollment_yearly, set claim_created_flag = T:

-- Timestamp:
select 'Inserting IQVIA data from iqvia.claims and iqvia.enroll_synth started at: ' || current_timestamp as message;

insert into dw_staging.iqva_member_enrollment_yearly(
         data_source, 
         year, 
         uth_member_id, 
         gender_cd,
         race_cd,
		 age_derived, 
		 dob_derived,
		 state,
		 zip3,
		 claim_created_flag,
		 behavioral_coverage,
		 load_date,
         member_id_src,
         table_id_src)
select distinct on(es.year, b.uth_member_id)
       'iqva' as data_source, 
       es.year::int,
       b.uth_member_id as uth_member_id,
       case
	       when es.der_sex is null then 'U' -- patients missing der_sex (NULL) will have gender_cd mapped as 'U'
       	   else es.der_sex
       end as gender_cd,
       '0' as race_cd,
       case 
	       when es.der_yob = '0000' or es.der_yob = '0' then 86 
	       when es.der_yob::int > es.year::int then null 
	       else es.year::int - es.der_yob::int -- patients missing der_yob (NULL) will have age_derived mapped as null
	   end as age_derived,
       case 
	       when es.der_yob = '0000' or es.der_yob = '0' then null 
	       when es.der_yob::int > es.year::int then null 
	       else (es.der_yob || '/12/31')::date -- patients missing der_yob (NULL) will have dob_derived mapped as null
	   end as dob_derived,
       es.pat_state as state,
       case
       		when es.pat_zip3 !~ '[0-9]+' then null -- to handle values such as '???', '.', 'CA', or 'FL'
       		when es.pat_zip3 = '0' then '000'
			when length(es.pat_zip3) = 1 then null 
			when length(es.pat_zip3) = 2 then rpad(es.pat_zip3, 3, '0')
			else es.pat_zip3
       end as zip3,
       true as claim_created_flag,
       case 
	       when es.mh_cd = 'Y' then 1 
	       when es.mh_cd = 'N' then 0 
	       when es.mh_cd = 'U' then null 
	       else null 
	   end as behavioral_coverage,
       current_date as load_date,
       es.pat_id as member_id_src,
       'claims' as table_id_src
from (select * from staging_clean.iqva_claim_created_enroll_synth_demographics union all select * from staging_clean.iqva_claim_created_enroll_synth_old_demographics) es 
	join data_warehouse.dim_uth_member_id b 
    	on es.pat_id = b.member_id_src 
;

 -- Analyze:
analyze dw_staging.iqva_member_enrollment_yearly;



--=== IQVIA enrolled month assignment: ===--

-- Timestamp:
select 'IQVIA enrolled month assignment started at: ' || current_timestamp as message;

-- Drop Existing Table:
drop table if exists staging_clean.iqva_temp_member_enrollment_month;

-- Create Table Containing Each Year and Month a Member Was Enrolled In:
create table staging_clean.iqva_temp_member_enrollment_month
with (appendonly=true, orientation=row)
as
select distinct uth_member_id, year, month_year_id, month_year_id % year as month
from dw_staging.iqva_member_enrollment_monthly
distributed by(uth_member_id);

-- Analyze:
analyze staging_clean.iqva_temp_member_enrollment_month;

-- Loop will iterate through each month and assign 1 if member was enrolled in that month and that year, 0 if otherwise:
do $$
declare 
	--month_counter integer := 1;
	i int;
	my_update_column text[]:= array['enrolled_jan','enrolled_feb','enrolled_mar',
	'enrolled_apr','enrolled_may','enrolled_jun','enrolled_jul','enrolled_aug',
	'enrolled_sep','enrolled_oct','enrolled_nov','enrolled_dec'];
begin

-- Add month flags:
	for i in 1..12
	loop
	execute
	'update dw_staging.iqva_member_enrollment_yearly y
		set ' || my_update_column[i] || '= case when exists(
		select 1 from staging_clean.iqva_temp_member_enrollment_month m
		where y.uth_member_id = m.uth_member_id
		and y.year = m.year
		and m.month = ' || i || ') then 1 else 0 end';
	raise notice 'Month of %', my_update_column[i];
    --month_counter = month_counter + 1;
	--i = month_counter + 1;
	end loop;

end $$;

-- Calculate total_enrolled_months:
update dw_staging.iqva_member_enrollment_yearly
set total_enrolled_months=enrolled_jan + enrolled_feb + enrolled_mar + enrolled_apr + 
	enrolled_may + enrolled_jun + enrolled_jul + enrolled_aug + 
	enrolled_sep + enrolled_oct + enrolled_nov + enrolled_dec;
                         
-- Vacuum analyze:
vacuum analyze dw_staging.iqva_member_enrollment_yearly;



--=== IQVIA clean member demographics: ===--

-- Timestamp:
select 'IQVIA member demographics cleanup started at: ' || current_timestamp as message;

/*******************************************************************************************************************
 * > Note:
 * 	  Note that the enroll_synth tables have 1 row per member. So, it's imposible for variables (der_yob, 
 *    der_sex, pat_state, pat_zip3, etc.) from the enroll_synth to differ within a year. 
 * 	  However, mstr_enroll_cd, prd_type, pay_type, etc. are recorded on a monthly basis.
 * 
 *    Clean variables by using most frequent > most recent.
 * 
 *******************************************************************************************************************/

do $$
declare
	col_list text[]:= array['plan_type', 'bus_cd', 'rx_coverage'];
	col_list_len int = array_length(col_list,1);
begin

/*******************************************************************************************************************
 * 
 * Below is logic for the yearly rollup of various columns.
 * All logic finds the most common occurence in a given year and assigns that value.
 * 
 *******************************************************************************************************************/

	for col_counter in 1.. col_list_len
	loop

		execute 'create table staging_clean.iqva_temp_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select count(*), max(month_year_id) as my, uth_member_id, ' || col_list[col_counter] || ', year
				 from dw_staging.iqva_member_enrollment_monthly
				 where ' || col_list[col_counter] || ' is not null
				 group by 3, 4, 5;'
		;
	
	    raise notice 'staging_clean.iqva_temp_enrl_% created', col_list[col_counter];
		
		execute 'create table staging_clean.iqva_final_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
				 from staging_clean.iqva_temp_enrl_' || col_list[col_counter] ||'
				 distributed by(uth_member_id);'
		;
		
		raise notice 'staging_clean.iqva_final_enrl_% created', col_list[col_counter];
	
	
		execute 'update dw_staging.iqva_member_enrollment_yearly a set ' || col_list[col_counter] ||' = b.' || col_list[col_counter] ||'
				 from staging_clean.iqva_final_enrl_' || col_list[col_counter] ||' b 
				 where a.uth_member_id = b.uth_member_id
				   and a.year = b.year
				   and b.my_grp = 1;'
		;
		
		execute 'drop table staging_clean.iqva_temp_enrl_' || col_list[col_counter] ||';';
		execute 'drop table staging_clean.iqva_final_enrl_' || col_list[col_counter] ||';';
		raise notice 'updated column %', col_list[col_counter];
	
	end loop;

end $$;



--== Vacuum analyze and cleanup: ===--

-- Timestamp:
select 'IQVIA vacuum analyze and final clean up started at: ' || current_timestamp as message;

-- Vacuum analyze:
vacuum analyze dw_staging.iqva_member_enrollment_yearly;

-- Drop existing tables that are no longer needed:
drop table if exists staging_clean.iqva_temp_member_enrollment_month;
drop table if exists staging_clean.iqva_claim_created_pat_ids_years;
drop table if exists staging_clean.iqva_claim_created_enroll_synth_demographics;
drop table if exists staging_clean.iqva_claim_created_enroll_synth_old_demographics;

-- Grant Access:
grant select on dw_staging.iqva_member_enrollment_yearly to uthealth_analyst;


-- Final timestamp:
select 'IQVIA member enrollment yearly ETL script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--= Various checks: =--

/*

-- View populated table: 
--select * from dw_staging.iqva_member_enrollment_yearly limit 1000;
--select * from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is true limit 1000;
--select * from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is not true and bus_cd is not null limit 1000;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

--== Quick Check - Number of rows per year should equal the number of pat_ids for that year (confirmed numbers match):
select 'dw_staging.iqva_member_enrollment_yearly total row count:' as message;
select year, count(*) from dw_staging.iqva_member_enrollment_yearly group by year order by year; -- CNT: 

select 'dw_staging.iqva_member_enrollment_yearly total patient count (uth_member_id):' as message;
select year, count(distinct(uth_member_id)) from dw_staging.iqva_member_enrollment_yearly group by year order by year; -- CNT: 

select 'dw_staging.iqva_member_enrollment_yearly total patient count (member_id_src):' as message;
select year, count(distinct(member_id_src)) from dw_staging.iqva_member_enrollment_yearly group by year order by year; -- CNT: 

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Number of patients in dw_staging.iqva_member_enrollment_yearly from the claims table:
select 'dw_staging.iqva_member_enrollment_yearly number of patients from iqvia.claims (uth_member_id): ' as message, year, count(distinct uth_member_id) from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is true
group by year order by year; -- CNT: 

select 'dw_staging.iqva_member_enrollment_yearly number of patients from iqvia.claims (member_id_src): ' as message, year, count(distinct member_id_src) from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is true
group by year order by year; -- CNT: 

-- Number of patients from the raw claims table:
select 'iqvia.claims total patient count (pat_id)' as message, year, count(distinct a.pat_id)
from iqvia.claims a
	left join (select pat_id, month_id from iqvia.enroll2) b
		on a.pat_id = b.pat_id
		and substring(a.month_id, 1, 4) = substring(b.month_id, 1, 4)
where b.pat_id is null and b.month_id is null
group by year order by year; -- CNT:

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Number of patients in dw_staging.iqva_member_enrollment_monthly from the enroll2 table:
select 'dw_staging.iqva_member_enrollment_yearly number of patients from iqvia.enroll2 (uth_member_id): ' as message, year, count(distinct uth_member_id) from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is not true
group by year order by year;-- CNT: 

select 'dw_staging.iqva_member_enrollment_yearly number of patients from iqvia.enroll2 (member_id_src): ' as message, year, count(distinct member_id_src) from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is not true
group by year order by year; -- CNT: 

-- Number of patients from the raw enroll2 table:
select 'enroll2 total patient count (pat_id)' as message, year, count(distinct pat_id) from iqvia.enroll2 group by year order by year; -- CNT: 

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Ensure that where claim_created_flag is T, there are no matches on member_id_src and year (substring(b.month_id, 1, 4)) to the enroll2 table. NO rows should be returned from the join:
select * 
from dw_staging.iqva_member_enrollment_yearly a 
	inner join iqvia.enroll2 b
on a.member_id_src = b.pat_id and a.year = substring(b.month_id, 1, 4)::int
where claim_created_flag is true; -- Returned no rows

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct years:
select distinct year from dw_staging.iqva_member_enrollment_yearly order by 1; -- 2006 thru 2023

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Check for null member_id_src and uth_member_id:
select * from dw_staging.iqva_member_enrollment_yearly where member_id_src is null; -- no rows returned
select * from dw_staging.iqva_member_enrollment_yearly where uth_member_id is null; -- no rows returned


-- Check distinct total_enrolled_months for where claim_created_flag = true (should be 0):
select distinct total_enrolled_months from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is true; -- 0

-- Check distinct total_enrolled_months for where claim_created_flag = true (should be not be 0, lowest value should be 1):
select distinct total_enrolled_months from dw_staging.iqva_member_enrollment_yearly where claim_created_flag is not true order by 1; -- 1 thru 12

*/

