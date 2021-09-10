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
 */

create table dw_staging.member_enrollment_yearly (
	data_source char(4),
	year int2,
	uth_member_id bigint,
	total_enrolled_months int2,
    gender_cd char(1),
    race_cd char(1),
	age_derived int,
	dob_derived date,    
	state varchar,
	zip5 char(5),
	zip3 char(3),
	death_date date,
	plan_type text,
	bus_cd char(4),
	employee_status text,
	enrolled_jan int2 default 0,
	enrolled_feb int2 default 0,
	enrolled_mar int2 default 0,
	enrolled_apr int2 default 0,
	enrolled_may int2 default 0,
	enrolled_jun int2 default 0,
	enrolled_jul int2 default 0,
	enrolled_aug int2 default 0,
	enrolled_sep int2 default 0,
	enrolled_oct int2 default 0,
	enrolled_nov int2 default 0,
	enrolled_dec int2 default 0,
	claim_created_flag bool default false,
    rx_coverage int2,
	fiscal_year int2
)
with (appendonly=true, orientation=column)
distributed by(uth_member_id);


--insert recs from monthly  14min
insert into dw_staging.member_enrollment_yearly (data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag, rx_coverage, fiscal_year, race_cd )
select distinct on( data_source, year, uth_member_id )
       data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,replace(plan_type,' ',''), bus_cd, employee_status, claim_created_flag, rx_coverage, fiscal_year, race_cd
from dw_staging.member_enrollment_monthly 
;

--Create temp join table  6min
drop table if exists dw_staging.temp_member_enrollment_month;

create table dw_staging.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct uth_member_id, year, month_year_id, month_year_id % year as month
from dw_staging.member_enrollment_monthly
distributed by(uth_member_id);

vacuum analyze dw_staging.temp_member_enrollment_month;

vacuum analyze dw_staging.member_enrollment_yearly;


--Add month flags
--jw002 use execute function to loop through each month
do $$
declare
month_counter integer := 1;
my_update_column text[]:= array['enrolled_jan','enrolled_feb','enrolled_mar',
'enrolled_apr','enrolled_may','enrolled_jun','enrolled_jul','enrolled_aug',
'enrolled_sep','enrolled_oct','enrolled_nov','enrolled_dec'];
begin
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
end $$;



--Calculate total_enrolled_months
update dw_staging.member_enrollment_yearly
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int


vacuum analyze dw_staging.member_enrollment_yearly;

--validate
select count(*), count(distinct uth_member_id ), data_source , year
from  dw_staging.member_enrollment_yearly
group by data_source , year
order by data_source, year ;


-- Drop temp table
drop table if exists dw_staging.temp_member_enrollment_month;


-----------------------------------------------------------------------------------------------------------------------
-----************** logic for yearly rollup of various columns
-- all logic finds the most common occurence in a given year and assigns that value  28min
-----------------------------------------------------------------------------------------------------------------------


---states
select count(*), min(month_year_id) as my, uth_member_id, state, year
 into dev.wc_state_yearly
from dw_staging.member_enrollment_monthly
group by uth_member_id, state, year
;

create table dev.wc_state_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_state_yearly
distributed by(uth_member_id);


update dw_staging.member_enrollment_yearly a set state = b.state
from dev.wc_state_yearly_final b
where a.uth_member_id = b.uth_member_id
and a.year = b.year
 and b.my_grp = 1;



drop table dev.wc_state_yearly;

drop table dev.wc_state_yearly_final;

---zip3
select count(*), min(month_year_id) as my, uth_member_id, zip3, year
 into dev.wc_zip3_yearly
from dw_staging.member_enrollment_monthly
group by uth_member_id, zip3, year
;

create table dev.wc_zip3_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_zip3_yearly
distributed by(uth_member_id);


update dw_staging.member_enrollment_yearly a set zip3 = b.zip3
from dev.wc_zip3_yearly_final b
where a.uth_member_id = b.uth_member_id
and a.year = b.year
 and b.my_grp = 1;




drop table dev.wc_zip3_yearly;

drop table dev.wc_zip3_yearly_final;


--- zip5
select count(*), min(month_year_id) as my, uth_member_id, zip5, year
 into dev.wc_zip5_yearly
from dw_staging.member_enrollment_monthly
group by uth_member_id, zip5, year
;


create table dev.wc_zip5_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_zip5_yearly
distributed by(uth_member_id);


update dw_staging.member_enrollment_yearly a set zip5 = b.zip5
from dev.wc_zip5_yearly_final b
where a.uth_member_id = b.uth_member_id
and a.year = b.year
 and b.my_grp = 1;


drop table dev.wc_zip5_yearly;

drop table dev.wc_zip5_yearly_final;


--- plan type
select count(*), min(month_year_id) as my, uth_member_id, plan_type, year
 into dev.wc_plan_type_yearly
from dw_staging.member_enrollment_monthly
group by uth_member_id, plan_type, year
;

create table dev.wc_plan_type_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_plan_type_yearly
distributed by(uth_member_id);


update dw_staging.member_enrollment_yearly a set plan_type = b.plan_type
from dev.wc_plan_type_yearly_final b
where a.uth_member_id = b.uth_member_id
and a.year = b.year
 and b.my_grp = 1;


drop table dev.wc_plan_type_yearly;

drop table dev.wc_plan_type_yearly_final;


---EE status
select count(*), min(month_year_id) as my, uth_member_id, employee_status, year
 into dev.wc_employee_status_yearly
from dw_staging.member_enrollment_monthly
group by uth_member_id, employee_status, year
;

create table dev.wc_employee_status_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_employee_status_yearly
distributed by(uth_member_id);


update dw_staging.member_enrollment_yearly a set employee_status = b.employee_status
from dev.wc_employee_status_yearly_final b
where a.uth_member_id = b.uth_member_id
and a.year = b.year
 and b.my_grp = 1;


drop table dev.wc_employee_status_yearly;

drop table dev.wc_employee_status_yearly_final;


vacuum analyze dw_staging.member_enrollment_yearly;

------------------------- END SCRIPT 

