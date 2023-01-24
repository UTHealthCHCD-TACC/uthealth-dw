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


--insert recs from monthly 

insert into dw_staging.member_enrollment_yearly  (
         data_source, 
         year, 
         uth_member_id, 
		 age_derived, 
		 dob_derived, 
		 death_date,
		 bus_cd, 
		 claim_created_flag, 
		 rx_coverage, 
		 race_cd,
		 fiscal_year, 
         family_id,
		 behavioral_coverage,
         member_id_src,
         table_id_src)
select distinct on( year, uth_member_id )
       'truv', 
        year, 
        uth_member_id, 
	    age_derived, 
	    dob_derived, 
	    death_date,
        bus_cd,
	    claim_created_flag, 
	    rx_coverage, 
	    race_cd,
	    fiscal_year, 
	    family_id,
	    behavioral_coverage,
	    member_id_src,
	    table_id_src 
from dw_staging.member_enrollment_monthly_1_prt_truv;



analyze dw_staging.member_enrollment_yearly_1_prt_truv;




drop table if exists staging_clean.temp_member_enrollment_month;

create table staging_clean.temp_member_enrollment_month
with (appendonly=true, orientation=row)
as
select distinct uth_member_id, year, month_year_id, month_year_id % year as month
from dw_staging.member_enrollment_monthly_1_prt_truv
distributed by(uth_member_id);

analyze staging_clean.temp_member_enrollment_month;

/*
 * Enrollment Months 
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

--Add month flags
	for array_counter in 1..12
	loop
	execute
	'update dw_staging.member_enrollment_yearly_1_prt_truv y
			set ' || my_update_column[array_counter] || '= 1
			from staging_clean.temp_member_enrollment_month m
			where y.uth_member_id = m.uth_member_id
			  and y.year = m.year
			  and m.month = ' || month_counter || ';';
	raise notice 'Month of %', my_update_column[array_counter];
  month_counter = month_counter + 1;
	array_counter = month_counter + 1;
	end loop;


--Calculate total_enrolled_months
update dw_staging.member_enrollment_yearly_1_prt_truv
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+
                          enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int;


-- Drop temp table
drop table if exists staging_clean.temp_member_enrollment_month;




end $$;
--might want to raise notice here 


vacuum analyze dw_staging.member_enrollment_yearly_1_prt_truv;

/*
 * Personal Variables
 */

do $$
declare
	col_list text[]:= array['state','zip3','gender_cd','plan_type','employee_status'];
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
				 from dw_staging.member_enrollment_monthly_1_prt_truv
				 group by 3, 4, 5;'
		;
	
	    raise notice '1';
		
		execute 'create table staging_clean.final_enrl_' || col_list[col_counter] ||'
				 with (appendonly=true, orientation=column)
				 as
				 select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
				 from staging_clean.temp_enrl_' || col_list[col_counter] ||'
				 distributed by(uth_member_id);'
		;
		
		raise notice '2';
	
		execute 'update dw_staging.member_enrollment_yearly_1_prt_truv a set ' || col_list[col_counter] ||' = b.' || col_list[col_counter] ||'
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

vacuum analyze dw_staging.member_enrollment_yearly;

