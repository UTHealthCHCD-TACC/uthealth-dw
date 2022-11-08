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
 */

drop table if exists dw_staging.member_enrollment_yearly;

create table dw_staging.member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
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
  partition mcrn values ('mcrn')
 )
;


--300 minutes
do $$
declare
	month_counter integer := 1;
	my_update_column text[]:= array['enrolled_jan','enrolled_feb','enrolled_mar',
	'enrolled_apr','enrolled_may','enrolled_jun','enrolled_jul','enrolled_aug',
	'enrolled_sep','enrolled_oct','enrolled_nov','enrolled_dec'];
begin



--insert recs from monthly  14min
execute 'insert into dw_staging.member_enrollment_yearly (
         data_source, 
         year, 
         uth_member_id, 
		 age_derived, 
		 dob_derived, 
		 death_date,
		 bus_cd, 
		 claim_created_flag, 
		 rx_coverage, 
		 fiscal_year, 
         family_id,
		 behavioral_coverage,
         member_id_src )
select distinct on( data_source, year, uth_member_id )
       data_source, 
       year, 
       uth_member_id, 
	   age_derived, 
	   dob_derived, 
	   death_date,
       bus_cd,
	   claim_created_flag, 
	   rx_coverage, 
	   fiscal_year, 
	   family_id,
	   behavioral_coverage,
	   member_id_src
from dw_staging.member_enrollment_monthly;'
;

raise notice 'Records inserted into enrollment yearly';


--Create temp join table to populate month flags 6min
drop table if exists dw_staging.temp_member_enrollment_month;

execute 'create table dw_staging.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct uth_member_id, year, month_year_id, month_year_id % year as month
from dw_staging.member_enrollment_monthly
distributed by(uth_member_id);'
;

analyze dw_staging.temp_member_enrollment_month;

--Add month flags
--jw002 use execute function to loop through each month
	for array_counter in 1..12
	loop
	execute
	'update dw_staging.member_enrollment_yearly y
			set ' || my_update_column[array_counter] || '= 1
			from dw_staging.temp_member_enrollment_month m
			where y.uth_member_id = m.uth_member_id
			  and y.year = m.year
			  and m.month = ' || month_counter || ';';
	raise notice 'Month of %', my_update_column[array_counter];
  month_counter = month_counter + 1;
	array_counter = month_counter + 1;
	end loop;


--Calculate total_enrolled_months
update dw_staging.member_enrollment_yearly
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+
                          enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int;


-- Drop temp table
drop table if exists dw_staging.temp_member_enrollment_month;

raise notice 'total_enrolled_months updated';

end $$;


vacuum analyze dw_staging.member_enrollment_yearly;

do $$
declare
	month_counter integer := 1;
	my_update_column text[]:= array['enrolled_jan','enrolled_feb','enrolled_mar',
	'enrolled_apr','enrolled_may','enrolled_jun','enrolled_jul','enrolled_aug',
	'enrolled_sep','enrolled_oct','enrolled_nov','enrolled_dec'];
	col_list text[]:= array['state','zip5','zip3','gender_cd','dual','htw','employee_status'];
	col_list_len int = array_length(col_list,1);
begin

-----------------------------------------------------------------------------------------------------------------------
-----************** logic for yearly rollup of various columns
-- all logic finds the most common occurence in a given year and assigns that value  28min
-----------------------------------------------------------------------------------------------------------------------
	for col_counter in 1.. col_list_len
	loop

		execute 'create table dw_staging.temp_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select count(*), max(month_year_id) as my, uth_member_id, ' || col_list[col_counter] || ', year
				 from dw_staging.member_enrollment_monthly
				 group by 3, 4, 5;'
		;
	
	    raise notice '1';
		
		execute 'create table dw_staging.final_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
				 from dw_staging.temp_enrl_' || col_list[col_counter] ||'
				 distributed by(uth_member_id);'
		;
		
		raise notice '2';
	
		execute 'update dw_staging.member_enrollment_yearly a set ' || col_list[col_counter] ||' = b.' || col_list[col_counter] ||'
				 from dw_staging.final_enrl_' || col_list[col_counter] ||' b 
				 where a.uth_member_id = b.uth_member_id
				   and a.year = b.year
				   and b.my_grp = 1;'
		;
		
		execute 'drop table dw_staging.temp_enrl_' || col_list[col_counter] ||';';
		execute 'drop table dw_staging.final_enrl_' || col_list[col_counter] ||';';
		raise notice 'updated column %', col_list[col_counter];
	
	end loop;



end $$;

/*
 * FIX PLAN TYPE TO ACCOUNT FOR PRIORITY LIST 
 */

drop table if exists dw_staging.mdcd_plan_priority;

create table dw_staging.mdcd_plan_priority (
	plan_type text null, priority int null
	);

--add values to it
INSERT INTO dw_staging.mdcd_plan_priority(plan_type, priority)
  VALUES ('CHIP', 1 ),
	 ('STAR Kids', 2 ),
	 ('STAR+PLUS', 2 ),
	 ('STAR Health', 2 ),
	 ('STAR', 3 ),
	 ('MMP', 4 ),
	 ('FFS', 4 ),
	 ('PCCM', 4 );
                
select * from dw_staging.mdcd_plan_priority;

---------------------

drop table if exists dw_staging.mdcd_plan_count;

create table dw_staging.mdcd_plan_count 
with (appendonly=true, orientation=row) as (
select uth_member_id, "year", plan_type, 
       count(*) as "count", 
       max(month_year_id) as my
  from dw_staging.member_enrollment_monthly 
  group by 1,2,3
  )  distributed by(uth_member_id);
 analyze dw_staging.mdcd_plan_count ;

 
-----------------  row number by count, priority, then recent ------------------------------ 
drop table if exists dw_staging.mdcd_plan_rn;

create table dw_staging.mdcd_plan_rn 
with (appendonly=true, orientation=row) as (
select a.uth_member_id, year, b.plan_type, row_number ()  
 		over(partition by uth_member_id, year order by "count" desc, priority asc, my desc) as rn
  from dw_staging.mdcd_plan_count a 
  left outer join dw_staging.mdcd_plan_priority b 
  on a.plan_type = b.plan_type 
  )  distributed by(uth_member_id);
  
 
 analyze dw_staging.mdcd_plan_rn;

--------------  
select * from dw_staging.mdcd_plan_rn;

update dw_staging.member_enrollment_yearly a 
   set plan_type = b.plan_type 
  from dw_staging.mdcd_plan_rn  b 
 where a.uth_member_id = b.uth_member_id
   and a.year = b.year
   and b.rn = 1;	
  
/*
 * FIX RACE CD
 */
  
  drop table if exists dw_staging.mdcd_race_count;

create table dw_staging.mdcd_race_count 
with (appendonly=true, orientation=row) as (
select uth_member_id, "year", race_cd, 
       count(*) as "count", 
       max(month_year_id) as my
  from dw_staging.member_enrollment_monthly 
  group by 1,2,3
  )  distributed by(uth_member_id);
 analyze dw_staging.mdcd_plan_count ;
 
-----------------  row number by count, priority, then recent ------------------------------ 
drop table if exists dw_staging.mdcd_race_rn;

create table dw_staging.mdcd_race_rn 
with (appendonly=true, orientation=row) as (
select a.uth_member_id, year, race_cd , row_number ()  
 		over(partition by uth_member_id, year order by "count" desc, race_cd desc, my desc) as rn
  from dw_staging.mdcd_race_count a 
  )  distributed by(uth_member_id);
  ;
 
 analyze dw_staging.mdcd_race_rn;

--------------  
select * from dw_staging.mdcd_race_rn;

update dw_staging.member_enrollment_yearly a 
   set race_cd  = b.race_cd  
  from dw_staging.mdcd_race_rn  b 
 where a.uth_member_id = b.uth_member_id
   and a.year = b.year
   and b.rn = 1;	


/*
 * FINALIZE
 */

vacuum analyze dw_staging.member_enrollment_yearly;
alter table dw_staging.member_enrollment_yearly  owner to uthealth_dev;
grant select on dw_staging.member_enrollment_yearly to uthealth_analyst;


