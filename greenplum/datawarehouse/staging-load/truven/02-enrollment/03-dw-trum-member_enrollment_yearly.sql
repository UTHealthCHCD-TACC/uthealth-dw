/* ******************************************************************************************************
 * Deletes and recreates member_enrollment_yearly records based on member_enrollment_monthly for a given dataset.
 * This includes creating all derived columns.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created
 * ******************************************************************************************************
 *  wallingTACC || 8/23/2021 || updated comments.
 * ******************************************************************************************************
 * jw002  || 9/08/2021 || added function for individual month flags
 * ******************************************************************************************************
 * wc002  || 9/09/2021 || move to dw_staging
 * ******************************************************************************************************
 * wc003  || 11/11/2021 || run as single script
 * ******************************************************************************************************
 * xrzhang || 03/20/2023 || Edited script to include MSA column, fixed the issue with enrl_months,
 * 							added in current_date for load_date, other small changes
 * ******************************************************************************************************
 * xrzhang || 04/28/2023 || Added fresh table creation + changed table name to truv_member_enrollment_monthly
 * 							When cleaning variables, do not consider nulls
 * ******************************************************************************************************
 * xrzhang || 07/18/2023 || Split truv into trum and truc		
 * ******************************************************************************************************
 * 
 *  */

select 'Truven MDCR member enrollment yearly etl script started at ' || current_timestamp as message;

---Drop existing table
drop table if exists dw_staging.trum_member_enrollment_yearly;

--Create empty member enrollment yearly
create table dw_staging.trum_member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);

--note that for truven, 2011-2018 have zip3 ONLY, no MSA
--and 2019 onwards have ONLY MSA, no zip3

select 'Inserting data started at ' || current_timestamp as message;

--Get distinct enrolids + clean demographics per member per year
--note that many variables in Truven are already cleaned such as DOB
insert into dw_staging.trum_member_enrollment_yearly  (
         data_source, 
         year, 
         uth_member_id, 
		 age_derived, 
		 dob_derived,
		 msa,
		 death_date,
		 bus_cd, 
		 claim_created_flag, 
		 rx_coverage, 
		 race_cd,
         family_id,
		 behavioral_coverage,
		 load_date,
         member_id_src,
         table_id_src)
select distinct on( year, uth_member_id )
       'trum', 
        year, 
        uth_member_id, 
	    age_cy, 
	    dob_derived,
	    msa,
	    death_date,
        bus_cd,
	    claim_created_flag, 
	    rx_coverage, 
	    race_cd,
	    family_id,
	    behavioral_coverage,
	    current_date,
	    member_id_src,
	    table_id_src 
from dw_staging.trum_member_enrollment_monthly;

--analyze yearly table
analyze dw_staging.trum_member_enrollment_yearly;

select 'Enrolled month assignment started at ' || current_timestamp as message;

--create table with the year and month each member was enrolled in
drop table if exists staging_clean.temp_member_enrollment_month;

create table staging_clean.temp_member_enrollment_month
with (appendonly=true, orientation=row)
as
select distinct uth_member_id, year, month_year_id, month_year_id % year as month
from dw_staging.trum_member_enrollment_monthly
distributed by(uth_member_id);

--analyze it
analyze staging_clean.temp_member_enrollment_month;

/**************************************************
 * Loop that will iterate through each month and assign 1
 * if member was enrolled in that month that year and 0 otherwise
 *************************************************/

do $$
declare 
	--month_counter integer := 1;
	i int;
	my_update_column text[]:= array['enrolled_jan','enrolled_feb','enrolled_mar',
	'enrolled_apr','enrolled_may','enrolled_jun','enrolled_jul','enrolled_aug',
	'enrolled_sep','enrolled_oct','enrolled_nov','enrolled_dec'];
begin

--Add month flags
	for i in 1..12
	loop
	execute
	'update dw_staging.trum_member_enrollment_yearly y
		set ' || my_update_column[i] || '= case when exists(
		select 1 from staging_clean.temp_member_enrollment_month m
		where y.uth_member_id = m.uth_member_id
		and y.year = m.year
		and m.month = ' || i || ') then 1 else 0 end';
	raise notice 'Month of %', my_update_column[i];
    --month_counter = month_counter + 1;
	--i = month_counter + 1;
	end loop;

end $$;

--Calculate total_enrolled_months
update dw_staging.trum_member_enrollment_yearly
set total_enrolled_months=enrolled_jan + enrolled_feb + enrolled_mar + enrolled_apr + 
	enrolled_may + enrolled_jun + enrolled_jul + enrolled_aug + 
	enrolled_sep + enrolled_oct + enrolled_nov + enrolled_dec;
                         
--vacuum analyze
vacuum analyze dw_staging.trum_member_enrollment_yearly;

select 'Member demographic clean-up started at ' || current_timestamp as message;

/****************************************************
 * Clean member demographics
 ****************************************************/
--DOB is 1-1 with member_id_src and does not require cleaning

do $$
declare
	col_list text[]:= array['state', 'zip3', 'msa','gender_cd','plan_type','employee_status'];
	col_list_len int = array_length(col_list,1);
begin

-----------------------------------------------------------------------------------------------------------------------
-----************** logic for yearly rollup of various columns
-- all logic finds the most common occurence in a given year and assigns that value  28min
-----------------------------------------------------------------------------------------------------------------------
	for col_counter in 1.. col_list_len
	loop

		execute 'create table staging_clean.temp_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select count(*), max(month_year_id) as my, uth_member_id, ' || col_list[col_counter] || ', year
				 from dw_staging.trum_member_enrollment_monthly
				 where ' || col_list[col_counter] || ' is not null
				 group by 3, 4, 5;'
		;
	
	    raise notice 'staging_clean.temp_enrl_% created', col_list[col_counter];
		
		execute 'create table staging_clean.final_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
				 from staging_clean.temp_enrl_' || col_list[col_counter] ||'
				 distributed by(uth_member_id);'
		;
		
		raise notice 'staging_clean.final_enrl_% created', col_list[col_counter];
	
	
		execute 'update dw_staging.trum_member_enrollment_yearly a set ' || col_list[col_counter] ||' = b.' || col_list[col_counter] ||'
				 from staging_clean.final_enrl_' || col_list[col_counter] ||' b 
				 where a.uth_member_id = b.uth_member_id
				   and a.year = b.year
				   and b.my_grp = 1;'
		;
		
		execute 'drop table staging_clean.temp_enrl_' || col_list[col_counter] ||';';
		execute 'drop table staging_clean.final_enrl_' || col_list[col_counter] ||';';
		raise notice 'updated column %', col_list[col_counter];
	
	end loop;

end $$;

select 'Vacuum analyze and final clean up started at ' || current_timestamp as message;

--vacuum analyze
vacuum analyze dw_staging.trum_member_enrollment_yearly;

/**************************
 * Hot fix for load_date - this code exist bc the original code didn't include load_date
 * It's since been changed so theoretically this shouldn't need to be run anymore
 * 
UPDATE dw_staging.truv_member_enrollment_yearly
SET load_date = CURRENT_DATE;
 */

-- Drop temp table we created earlier
drop table if exists staging_clean.temp_member_enrollment_month;

select 'Truven MDCR member enrollment yearly etl script completed at ' || current_timestamp as message;

