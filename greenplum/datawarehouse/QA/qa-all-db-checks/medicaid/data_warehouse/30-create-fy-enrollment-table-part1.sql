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
 * xz001  || modified for FY 
 */

drop table if exists dev.member_enrollment_fiscal_yearly;

create table dev.member_enrollment_fiscal_yearly  
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

--get enrollment data from monthly table 15 min
insert into dev.member_enrollment_fiscal_yearly (
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
select distinct on( data_source, fiscal_year, uth_member_id )
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
from data_warehouse.member_enrollment_monthly_1_prt_mdcd;

analyze dev.member_enrollment_fiscal_yearly;

--Create temp join table to populate month flags 6 min
drop table if exists dev.temp_member_enrollment_month;

create table dev.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct uth_member_id, fiscal_year, month_year_id, month_year_id % year as month
from data_warehouse.member_enrollment_monthly_1_prt_mdcd
distributed by(uth_member_id);

analyze dev.temp_member_enrollment_month;

do $$
declare
	month_counter integer := 1;
	my_update_column text[]:= array['enrolled_jan','enrolled_feb','enrolled_mar',
	'enrolled_apr','enrolled_may','enrolled_jun','enrolled_jul','enrolled_aug',
	'enrolled_sep','enrolled_oct','enrolled_nov','enrolled_dec'];
begin

--Add month flags
--jw002 use execute function to loop through each month
	for array_counter in 1..12
	loop
	execute
	'update dev.member_enrollment_fiscal_yearly y
			set ' || my_update_column[array_counter] || '= 1
			from dev.temp_member_enrollment_month m
			where y.uth_member_id = m.uth_member_id
			  and y.fiscal_year = m.fiscal_year
			  and m.month = ' || month_counter || ';';
	raise notice 'Month of %', my_update_column[array_counter];
  month_counter = month_counter + 1;
	array_counter = month_counter + 1;
	end loop;

--Calculate total_enrolled_months
update dev.member_enrollment_fiscal_yearly
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+
                          enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int;

-- Drop temp table
drop table if exists dev.temp_member_enrollment_month;

raise notice 'total_enrolled_months updated';

end $$;

vacuum analyze dev.member_enrollment_fiscal_yearly;

/* did it work?
select count(*) from dev.member_enrollment_fiscal_yearly where total_enrolled_months = 12;
--5,087,050

select count(*) from data_warehouse.member_enrollment_yearly_1_prt_mdcd
where total_enrolled_months = 12
and year = 2020;
--4,369,353

these numbers are on the same order of magnitude so good enough

*/

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

		execute 'create table dev.temp_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select count(*), max(month_year_id) as my, uth_member_id, ' || col_list[col_counter] || ', fiscal_year
				 from data_warehouse.member_enrollment_monthly_1_prt_mdcd
				 group by 3, 4, 5;'
		;
	
	    raise notice '1';
		
		execute 'create table dev.final_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select * , row_number() over(partition by uth_member_id, fiscal_year order by count desc, my desc) as my_grp
				 from dev.temp_enrl_' || col_list[col_counter] ||'
				 distributed by(uth_member_id);'
		;
		
		raise notice '2';
	
		execute 'update dev.member_enrollment_fiscal_yearly a set ' || col_list[col_counter] ||' = b.' || col_list[col_counter] ||'
				 from dev.final_enrl_' || col_list[col_counter] ||' b 
				 where a.uth_member_id = b.uth_member_id
				   and a.fiscal_year = b.fiscal_year
				   and b.my_grp = 1;'
		;
		
		execute 'drop table dev.temp_enrl_' || col_list[col_counter] ||';';
		execute 'drop table dev.final_enrl_' || col_list[col_counter] ||';';
		raise notice 'updated column %', col_list[col_counter];
	
	end loop;

end $$;

/*check if it worked?
select count(*) from dev.member_enrollment_fiscal_yearly;
--61090088

select count(*) from data_warehouse.member_enrollment_yearly_1_prt_mdcd;
--65858906

select count(distinct member_id_src) from dev.member_enrollment_fiscal_yearly;
--13029774

select count(distinct member_id_src) from data_warehouse.member_enrollment_yearly_1_prt_mdcd;
--13029774

--good enough

*/

