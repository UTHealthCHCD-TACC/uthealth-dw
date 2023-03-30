/* ******************************************************************************************************
 * Makes yearly member enrollment table by fiscal year instead of calendar year
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * xrzhang || 03/21/202 || Created
 */

--initialize table
drop table if exists dw_staging.member_enrollment_fiscal_yearly;

create table dw_staging.member_enrollment_fiscal_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

--insert 1 row per member per year from monthly table
insert into dw_staging.member_enrollment_fiscal_yearly (
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
from dw_staging.mcd_member_enrollment_monthly;



drop table if exists dw_staging.temp_member_enrollment_month;

--create temp table that get the year and month of enrollment by fiscal year
create table dw_staging.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct uth_member_id, fiscal_year, month_year_id, month_year_id % year as month
from dw_staging.mcd_member_enrollment_monthly
distributed by(uth_member_id);

analyze dw_staging.temp_member_enrollment_month;

--load monthly data into yearly data and add the total enrolled months
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
	'update dw_staging.member_enrollment_fiscal_yearly y
		set ' || my_update_column[i] || '= case when exists(
		select 1 from dw_staging.temp_member_enrollment_month m
		where y.uth_member_id = m.uth_member_id
		and y.fiscal_year = m.fiscal_year
		and m.month = ' || i || ') then 1 else 0 end';
	raise notice 'Month of %', my_update_column[i];
    --month_counter = month_counter + 1;
	--i = month_counter + 1;
	end loop;

end $$;

--Calculate total_enrolled_months
update dw_staging.member_enrollment_fiscal_yearly
set total_enrolled_months=enrolled_jan + enrolled_feb + enrolled_mar + enrolled_apr + 
	enrolled_may + enrolled_jun + enrolled_jul + enrolled_aug + 
	enrolled_sep + enrolled_oct + enrolled_nov + enrolled_dec;

raise notice 'total_enrolled_months updated';

-- Drop temp table
drop table if exists dw_staging.temp_member_enrollment_month;

vacuum analyze dw_staging.member_enrollment_fiscal_yearly;

--first clean gender_cd
--this is slightly different from other variables bc we want to disregard 'U'

create table dw_staging.temp_enrl_gender_cd
	 with (appendonly=true, orientation=column)
	 as
	 select count(*), max(month_year_id) as my, uth_member_id, gender_cd , fiscal_year,
	 	max(case when gender_cd = 'U' then 0 else 1 end) as not_u
	 from dw_staging.mcd_member_enrollment_monthly
	 group by 3, 4, 5;

create table dw_staging.final_enrl_gender_cd
	 with (appendonly=true, orientation=column)
	 as
	 select * , row_number() over(partition by uth_member_id, fiscal_year order by not_u desc, count desc, my desc) as my_grp
	 from dw_staging.temp_enrl_gender_cd
	 distributed by(uth_member_id);
	
update dw_staging.member_enrollment_fiscal_yearly a set gender_cd  = b.gender_cd 
	 from dw_staging.final_enrl_gender_cd b 
	 where a.uth_member_id = b.uth_member_id
	   and a.fiscal_year = b.fiscal_year
	   and b.my_grp = 1;

drop table dw_staging.temp_enrl_gender_cd;
drop table dw_staging.final_enrl_gender_cd;


--clean all the rest of the variables
--XRZ took out employee_status 3/21/23 because... Medicaid doesn't have employee status
do $$
declare
	col_list text[]:= array['state','zip5','zip3','dual','htw'];
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
				 select count(*) as count, max(month_year_id) as my, uth_member_id, ' || col_list[col_counter] || ', fiscal_year
				 from dw_staging.mcd_member_enrollment_monthly
				 group by 3, 4, 5;'
		;
	
	    raise notice '1';
		
		execute 'create table dw_staging.final_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select * , row_number() over(partition by uth_member_id, fiscal_year order by count desc, my desc) as my_grp
				 from dw_staging.temp_enrl_' || col_list[col_counter] ||'
				 distributed by(uth_member_id);'
		;
		
		raise notice '2';
	
		execute 'update dw_staging.member_enrollment_fiscal_yearly a set ' || col_list[col_counter] ||' = b.' || col_list[col_counter] ||'
				 from dw_staging.final_enrl_' || col_list[col_counter] ||' b 
				 where a.uth_member_id = b.uth_member_id
				   and (a.'|| col_list[col_counter] ||' is null or
					a.'|| col_list[col_counter] ||' != b.' || col_list[col_counter] ||')
				   and a.fiscal_year = b.fiscal_year
				   and b.my_grp = 1;'
		;
		
		execute 'drop table dw_staging.temp_enrl_' || col_list[col_counter] ||';';
		execute 'drop table dw_staging.final_enrl_' || col_list[col_counter] ||';';
		raise notice 'updated column %', col_list[col_counter];
	
	end loop;

end $$;

