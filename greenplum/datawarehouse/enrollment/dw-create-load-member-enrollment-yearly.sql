/* ******************************************************************************************************
 * Deletes and recreates member_enrollment_yearly records based on member_enrollment_monthly for a given dataset.
 * This includes creating all derived columns.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wallingTACC || 8/23/2021 || updated comments.
 */


------------------------------------------------------------
vacuum analyze data_warehouse.member_enrollment_yearly;

vacuum analyze data_warehouse.member_enrollment_monthly;

---remove old
delete from data_warehouse.member_enrollment_yearly where data_source in ('mdcd');

--insert new recs from monthly 
insert into data_warehouse.member_enrollment_yearly (data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag, rx_coverage, fiscal_year, race_cd )
select distinct on( data_source, year, uth_member_id ) 
       data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,replace(plan_type,' ',''), bus_cd, employee_status, claim_created_flag, rx_coverage, fiscal_year, race_cd
from data_warehouse.member_enrollment_monthly a 
where data_source in ('mdcd')
;

drop table dev.temp_member_enrollment_month;

--Create temp join tables
create table dev.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct uth_member_id, year, month_year_id, month_year_id % year as month
from data_warehouse.member_enrollment_monthly
where data_source in ('mdcd')
distributed by(uth_member_id);

vacuum analyze dev.temp_member_enrollment_month;



--Add month flags
update data_warehouse.member_enrollment_yearly y
set enrolled_jan = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 1
;

update data_warehouse.member_enrollment_yearly y
set enrolled_feb = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 2
;


update data_warehouse.member_enrollment_yearly y
set enrolled_mar = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 3
;

update data_warehouse.member_enrollment_yearly y
set enrolled_apr = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 4
;

update data_warehouse.member_enrollment_yearly y
set enrolled_may = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 5
;

update data_warehouse.member_enrollment_yearly y
set enrolled_jun = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 6
;

update data_warehouse.member_enrollment_yearly y
set enrolled_jul = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 7
;

update data_warehouse.member_enrollment_yearly y
set enrolled_aug = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 8
;

update data_warehouse.member_enrollment_yearly y
set enrolled_sep = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 9
;

update data_warehouse.member_enrollment_yearly y
set enrolled_oct = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 10
;

update data_warehouse.member_enrollment_yearly y
set enrolled_nov = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 11
;

update data_warehouse.member_enrollment_yearly y
set enrolled_dec = 1
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 12
;

--Calculate total_enrolled_months
update data_warehouse.member_enrollment_yearly
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int




--validate 
select count(*), count(distinct uth_member_id ), data_source , year 
from  data_warehouse.member_enrollment_yearly
group by data_source , year 
order by data_source, year ;


-- Drop temp table
drop table dev.temp_member_enrollment_month;


select * from data_warehouse.member_enrollment_monthly mem where data_source = 'optd';

-----------------------------------------------------------------------------------------------------------------------
-----************** logic for yearly rollup of various columns
-- all logic finds the most common occurence in a given year and assigns that value
-----------------------------------------------------------------------------------------------------------------------


---states 
select count(*), min(month_year_id) as my, uth_member_id, state, year 
 into dev.wc_state_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, state, year 
;

create table dev.wc_state_yearly_final 
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_state_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set state = b.state 
from dev.wc_state_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;



drop table dev.wc_state_yearly;

drop table dev.wc_state_yearly_final;

---zip3
select count(*), min(month_year_id) as my, uth_member_id, zip3, year 
 into dev.wc_zip3_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, zip3, year 
;

create table dev.wc_zip3_yearly_final 
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_zip3_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set zip3 = b.zip3
from dev.wc_zip3_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;




drop table dev.wc_zip3_yearly;

drop table dev.wc_zip3_yearly_final;


--- zip5
select count(*), min(month_year_id) as my, uth_member_id, zip5, year 
 into dev.wc_zip5_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, zip5, year 
;


create table dev.wc_zip5_yearly_final 
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_zip5_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set zip5 = b.zip5
from dev.wc_zip5_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_zip5_yearly;

drop table dev.wc_zip5_yearly_final;


--- plan type
select count(*), min(month_year_id) as my, uth_member_id, plan_type, year 
 into dev.wc_plan_type_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, plan_type, year 
;

create table dev.wc_plan_type_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_plan_type_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set plan_type = b.plan_type
from dev.wc_plan_type_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_plan_type_yearly;

drop table dev.wc_plan_type_yearly_final;


---EE status
select count(*), min(month_year_id) as my, uth_member_id, employee_status, year 
 into dev.wc_employee_status_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, employee_status, year 
;

create table dev.wc_employee_status_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my desc) as my_grp
from dev.wc_employee_status_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set employee_status = b.employee_status
from dev.wc_employee_status_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_employee_status_yearly;

drop table dev.wc_employee_status_yearly_final;




---------------------- 6/14/2021 adding member_id_src 
update  data_warehouse.member_enrollment_yearly a set member_id_src = b.member_id_src 
from data_warehouse.dim_uth_member_id b  
   where a.uth_member_id = b.uth_member_id ;



----va
vacuum analyze data_warehouse.member_enrollment_yearly;

--spot check
select * from data_warehouse.member_enrollment_yearly 
where uth_member_id = 530680152


--final validate 
select count(*), count(distinct uth_member_id ), year , data_source 
from  data_warehouse.member_enrollment_yearly
group by year, data_source 
order by data_source , year ;


/* Original table creation. DO NOT RUN
 * 
drop table if exists data_warehouse.member_enrollment_yearly cascade;

create table data_warehouse.member_enrollment_yearly (
	data_source char(4), 
	year int2,
	uth_member_id bigint,
	total_enrolled_months int2,
	bus_cd char(4),
	gender_cd char(1),
	state varchar,
	zip5 char(5),
	zip3 char(3),
	age_derived int,
	dob_derived date, 
	race_cd char(2),
	death_date date,
	plan_type text,
	employee_status text, 
	rx_coverage int2, 
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
	fiscal_year int2,
	claim_created_flag bool default false,
	member_id_src text 
)
with (appendonly=true, orientation=column)
distributed by(uth_member_id);

*/

