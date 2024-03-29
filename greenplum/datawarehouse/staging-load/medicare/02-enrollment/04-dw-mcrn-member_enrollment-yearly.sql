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
 * xzhang || 05/26/2023 || Modified using Medicaid yearly enrollment script as a shell
 * 							now includes new columns like load date, member_id_src, table_id_src
 * 							sex is cleaned separately, though it's unnecessary b/c medicare is pretty clean
 * 							zip3 is updated separately (for speed)
 * ******************************************************************************************************
 * xzhang || 06/01/2023 || Removed columns from cleanup that don't need to be cleaned up
 * ******************************************************************************************************
 * xzhang || 06/23/2023 || Added Dual to columns to clean up to reflect changes in monthly table
 * ******************************************************************************************************
 */

/***************************
 * INITIALIZE TABLE
 ****************************/
drop table if exists dw_staging.mcrn_member_enrollment_yearly;

create table dw_staging.mcrn_member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

/*********************************
 * Insert data from monthly table
 ********************************/
insert into dw_staging.mcrn_member_enrollment_yearly (
         data_source, 
         year, 
         uth_member_id, 
         gender_cd,
         race_cd,
		 age_derived, 
		 dob_derived,
		 state,
		 zip5,
		 zip3,
		 death_date,
		 bus_cd, 
		 load_date,
         member_id_src,
         table_id_src)
select distinct on(data_source, year, uth_member_id)
       data_source, 
       year, 
       uth_member_id,
       gender_cd,
       race_cd,
	   age_cy, 
	   dob_derived,
	   state,
	   zip5,
	   zip3,
	   death_date,
       bus_cd,
	   load_date,
	   member_id_src,
	   table_id_src
from dw_staging.mcrn_member_enrollment_monthly;

/*********************************
 * Fill in enrolled months data
 ********************************/
--create temp table that get the year and month of enrollment by fiscal year
drop table if exists dw_staging.temp_member_enrollment_month;

create table dw_staging.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct data_source, uth_member_id, "year", month_year_id, month_year_id % 100 as month
from dw_staging.mcrn_member_enrollment_monthly
distributed by(uth_member_id);

analyze dw_staging.temp_member_enrollment_month;

--fill in the enrollment by month
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
	'update dw_staging.mcrn_member_enrollment_yearly y
		set ' || my_update_column[i] || '= case when exists(
		select 1 from dw_staging.temp_member_enrollment_month m
		where y.uth_member_id = m.uth_member_id
		and y.year = m.year
		and m.month = ' || i || ') then 1 else 0 end';
	raise notice 'Month of %', my_update_column[i];
    --month_counter = month_counter + 1;
	--i = month_counter + 1;
	end loop;

end $$;

-- Drop temp table
drop table if exists dw_staging.temp_member_enrollment_month;

--update total enrolled months
update dw_staging.mcrn_member_enrollment_yearly
set total_enrolled_months = enrolled_jan + enrolled_feb + enrolled_mar + 
	enrolled_apr + enrolled_may + enrolled_jun + enrolled_jul + enrolled_aug + 
	enrolled_sep + enrolled_oct + enrolled_nov + enrolled_dec;

vacuum analyze dw_staging.mcrn_member_enrollment_yearly;

/*****************************************************************
 * Clean variables using most frequent > most recent
 * Note that original Medicare enrollment tables have 1 row per member per year
 * and the monthly table is expanded from that data
 * so it's impossible for sex, race, etc. to differ within a year.
 * However, plan_type and rx_coverage are recorded on a monthly basis
 ****************************************************************/
do $$
declare
	col_list text[]:= array['plan_type', 'rx_coverage', 'dual'];
	col_list_len int = array_length(col_list,1);
begin

	for col_counter in 1.. col_list_len
	loop

		execute 'create table dw_staging.temp_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select data_source, year, uth_member_id, ' || col_list[col_counter] || ',
					count(*) as count, max(month_year_id) as my
				 from dw_staging.mcrn_member_enrollment_monthly
				 where ' || col_list[col_counter] || ' is not null
				 group by 1, 2, 3, 4;'
		;
		raise notice '% table 1 created', col_list[col_counter];
		
		execute 'create table dw_staging.final_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select * , row_number() over(partition by data_source, uth_member_id, year
					order by count desc, my desc) as rn
				 from dw_staging.temp_enrl_' || col_list[col_counter] ||'
				 distributed by(uth_member_id);'
		;
		
		raise notice '% table 2 created', col_list[col_counter];
	
		execute 'update dw_staging.mcrn_member_enrollment_yearly a
				 set ' || col_list[col_counter] ||' = b.' || col_list[col_counter] ||'
				 from dw_staging.final_enrl_' || col_list[col_counter] ||' b 
				 where a.data_source = b.data_source
					and a.uth_member_id = b.uth_member_id
					and a.year = b.year
				   	and (a.'|| col_list[col_counter] ||' is null or
					a.'|| col_list[col_counter] ||' != b.' || col_list[col_counter] ||')
				   and b.rn = 1;'
		;
		
		execute 'drop table dw_staging.temp_enrl_' || col_list[col_counter] ||';';
		execute 'drop table dw_staging.final_enrl_' || col_list[col_counter] ||';';
		raise notice 'updated column %', col_list[col_counter];
	
	end loop;

end $$;

--vacuum after do loop
vacuum analyze dw_staging.mcrn_member_enrollment_yearly;





