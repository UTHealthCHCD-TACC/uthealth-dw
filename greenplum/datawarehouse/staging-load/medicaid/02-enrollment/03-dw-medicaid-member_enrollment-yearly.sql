/* ******************************************************************************************************
 * Deletes and recreates mcd_member_enrollment_yearly records based on member_enrollment_monthly for a given dataset.
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
 * jwozney modified this script late 2022
 * ******************************************************************************************************
 * xzhang  || 03/30/2023 || modified to accomodate htw and chip perinatal
 * ******************************************************************************************************
 * xzhang  || 09/05/2023 || Mod to accomodate dual as char instead of int + commented out add columns
 */

/***************************
 * INITIALIZE TABLE
 ****************************/
drop table if exists dw_staging.mcd_member_enrollment_yearly;

create table dw_staging.mcd_member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

/*add enrl_month_dual and enrl_months_nondual
alter table dw_staging.mcd_member_enrollment_yearly
	add column enrl_months_nondual int2,
	add column enrl_months_dual int2;
*/

--get rid of fiscal year, we don't need it!
alter table dw_staging.mcd_member_enrollment_yearly
drop column if exists fiscal_year;

/*********************************
 * Insert data from monthly table
 ********************************/
insert into dw_staging.mcd_member_enrollment_yearly (
         data_source, 
         year, 
         uth_member_id, 
		 age_derived, 
		 dob_derived, 
		 death_date,
		 bus_cd, 
		 claim_created_flag, 
		 rx_coverage, 
         family_id,
		 behavioral_coverage,
		 load_date,
         member_id_src,
         table_id_src)
select distinct on(data_source, year, uth_member_id)
       data_source, 
       year, 
       uth_member_id, 
	   age_cy, 
	   dob_derived, 
	   death_date,
       bus_cd,
	   claim_created_flag, 
	   rx_coverage, 
	   family_id,
	   behavioral_coverage,
	   load_date,
	   member_id_src,
	   table_id_src
from dw_staging.mcd_member_enrollment_monthly;

/*********************************
 * Fill in enrolled months data
 ********************************/
--create temp table that get the year and month of enrollment by fiscal year
drop table if exists dw_staging.temp_member_enrollment_month;

create table dw_staging.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct data_source, uth_member_id, "year", dual, month_year_id, month_year_id % "year" as month
from dw_staging.mcd_member_enrollment_monthly
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
	'update dw_staging.mcd_member_enrollment_yearly y
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

--Calculate dual, non-dual, and total enrolled months
drop table if exists dw_staging.temp_enrolled_months_by_dual;

create table dw_staging.temp_enrolled_months_by_dual
with (appendonly=true, orientation=column) as
select data_source, uth_member_id, year,
	sum(case when dual = '0' then 1 else 0 end) as enrl_months_nondual,
	sum(case when dual = '1' then 1 else 0 end) as enrl_months_dual,
	count(*) as total_enrolled_months
from dw_staging.temp_member_enrollment_month
group by data_source, year, uth_member_id
distributed by(uth_member_id);

analyze dw_staging.temp_enrolled_months_by_dual;

--merge numbers into yearly enrollment table
update dw_staging.mcd_member_enrollment_yearly a
set enrl_months_nondual = b.enrl_months_nondual,
	enrl_months_dual = b.enrl_months_dual,
	total_enrolled_months = b.total_enrolled_months
from dw_staging.temp_enrolled_months_by_dual b
where a.data_source = b.data_source
	and a.year = b.year
	and a.uth_member_id = b.uth_member_id;

vacuum analyze dw_staging.mcd_member_enrollment_yearly;

-- Drop temp table
drop table if exists dw_staging.temp_member_enrollment_month;

/************************************************
 * Clean sex - not in do loop b/c we want to disregard 'U' where possible
 ***********************************************/
create table dw_staging.temp_enrl_gender_cd
	 with (appendonly=true, orientation=column)
	 as
	 select count(*), max(month_year_id) as my, uth_member_id, gender_cd , year,
	 	max(case when gender_cd = 'U' then 0 else 1 end) as not_u
	 from dw_staging.mcd_member_enrollment_monthly
	 group by 3, 4, 5;

create table dw_staging.final_enrl_gender_cd
	 with (appendonly=true, orientation=column)
	 as
	 select * , row_number() over(partition by uth_member_id, year
	 order by not_u desc, count desc, my desc) as rn
	 from dw_staging.temp_enrl_gender_cd
	 distributed by(uth_member_id);
	
update dw_staging.mcd_member_enrollment_yearly a set gender_cd = b.gender_cd 
	 from dw_staging.final_enrl_gender_cd b 
	 where a.uth_member_id = b.uth_member_id
	   and a.year = b.year
	   and b.rn = 1;

drop table if exists dw_staging.temp_enrl_gender_cd;
drop table if exists dw_staging.final_enrl_gender_cd;

/*****************************************************************
 * Clean zip, and race using most frequent > most recent
 ****************************************************************/
do $$
declare
	col_list text[]:= array['zip5', 'race_cd'];
	col_list_len int = array_length(col_list,1);
begin

	for col_counter in 1.. col_list_len
	loop

		execute 'create table dw_staging.temp_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select data_source, year, uth_member_id, ' || col_list[col_counter] || ',
					count(*) as count, max(month_year_id) as my
				 from dw_staging.mcd_member_enrollment_monthly
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
	
		execute 'update dw_staging.mcd_member_enrollment_yearly a
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
vacuum analyze dw_staging.mcd_member_enrollment_yearly;

/****************************************
 * Sort out the rest of the variables: Dual, HTW, zip3
 ****************************************/
--set dual to 1 if enrl_months_dual >= enrl_months_nondual
update dw_staging.mcd_member_enrollment_yearly
set dual = case when enrl_months_dual >= enrl_months_nondual then 1
	else 0 end,
	htw = case when data_source = 'mhtw' then 1 else 0 end,
	zip3 = substring(zip5, 1, 3)
	;

--set state according to zip code
update dw_staging.mcd_member_enrollment_yearly a
set state = b.state
from reference_tables.ref_zip_code b
where a.zip5 = b.zip;

--vacuum analyze
vacuum analyze dw_staging.mcd_member_enrollment_yearly;

/*check
select * from dw_staging.mcd_member_enrollment_yearly;
select count(*) from dw_staging.mcd_member_enrollment_yearly where state is null; --50737
select count(*) from dw_staging.mcd_member_enrollment_yearly where state is not null; --61778636
select 50737.0/61778636;
*/



