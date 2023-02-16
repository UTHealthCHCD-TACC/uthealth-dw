drop table if exists dev.jw_function_monthly;

create table dev.jw_function_monthly
with (appendonly=true, orientation=column)
as
select 
data_source,
"year",
month_year_id % year as month,
uth_member_id,
consecutive_enrolled_months ,
gender_cd,
state,
zip5,
zip3,
age_derived,
dob_derived,
death_date,
plan_type,
bus_cd,
employee_status,
claim_created_flag,
row_identifier,
rx_coverage,
fiscal_year,
race_cd
from data_warehouse.member_enrollment_monthly
limit 1000
;

select * from dev.jw_function_monthly;

create table dev.jw_function_yearly
with (appendonly=true, orientation=column)
as
select *
from data_warehouse.member_enrollment_yearly
limit 0
;

insert into dev.jw_function_yearly (uth_member_id, year)
select distinct uth_member_id, year
from dev.jw_function_monthly
;

--delete from dev.jw_function_yearly;

do $$
declare
month_counter integer := 1;
--array_counter integer := 1;
my_update_column text[]:= array['enrolled_jan','enrolled_feb','enrolled_mar',
'enrolled_apr','enrolled_may','enrolled_jun','enrolled_jul','enrolled_aug',
'enrolled_sep','enrolled_oct','enrolled_nov','enrolled_dec'];
begin 
	for array_counter in 1..12
	loop
	execute
	'update dev.jw_function_yearly y
			set ' || my_update_column[array_counter] || '= 1
			from dev.jw_function_monthly m 
			where y.uth_member_id = m.uth_member_id 
			  and y.year = m.year 
			  and m.month = ' || month_counter || ';';
	raise notice 'Month of %', my_update_column[array_counter];
	raise notice 'Month equals %', month_counter;
  month_counter = month_counter + 1;
	array_counter = month_counter + 1;
	end loop;
end $$;




select * from dev.jw_function_yearly;
select * from dev.jw_function_monthly;

select a.uth_member_id, a."year", a."month", b. 
from dev.jw_function_monthly a
left join dev.jw_function_yearly b on a.uth_member_id = b.uth_member_id and a."year" = b."year" 
where a."month" = 5
order by a.uth_member_id, a.year, a.month;























