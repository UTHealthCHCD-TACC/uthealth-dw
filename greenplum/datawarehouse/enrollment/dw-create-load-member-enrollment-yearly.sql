drop table if exists data_warehouse.member_enrollment_yearly cascade;


create table data_warehouse.member_enrollment_yearly (
	data_source char(4), 
	year int2,
	uth_member_id bigint,
	total_enrolled_months int2,
	enrolled_jan bool default false,
	enrolled_feb bool default false,
	enrolled_mar bool default false,
	enrolled_apr bool default false,
	enrolled_may bool default false,
	enrolled_jun bool default false,
	enrolled_jul bool default false,
	enrolled_aug bool default false,
	enrolled_sep bool default false,
	enrolled_oct bool default false,
	enrolled_nov bool default false,
	enrolled_dec bool default false,
	gender_cd char(1),
	state varchar,
	dod char(5),
	zip3 char(3),
	age_derived int,
	dob_derived date, 
	death_date date,
	plan_type char(4),
	bus_cd char(4),
	employee_status text, 
	claim_created_flag bool default false,
	row_identifier bigserial,
	rx_coverage int2
)
with (appendonly=true, orientation=column)
distributed by(uth_member_id);


alter sequence data_warehouse.member_enrollment_yearly_row_identifier_seq cache 200


------------------------------------------------------------
vacuum analyze data_warehouse.member_enrollment_yearly;

vacuum analyze data_warehouse.member_enrollment_monthly;


delete from data_warehouse.member_enrollment_yearly where data_source in ('optd','optz');


insert into data_warehouse.member_enrollment_yearly (data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag, rx_coverage )
select distinct on( data_source, year, uth_member_id ) 
       data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,replace(plan_type,' ',''), bus_cd, employee_status, claim_created_flag, rx_coverage
from data_warehouse.member_enrollment_monthly
where data_source in ('optz','optd')
order by data_source, year, uth_member_id, month_year_id 
;

drop table dev.temp_member_enrollment_month;

--Create temp join tables
create table dev.temp_member_enrollment_month
with (appendonly=true, orientation=column)
as
select distinct uth_member_id, year, month_year_id, month_year_id % year as month
from data_warehouse.member_enrollment_monthly
where data_source in ('optz','optd')
distributed by(uth_member_id);

vacuum analyze dev.temp_member_enrollment_month;


select * from dev.temp_member_enrollment_month;


--Add month flags
update data_warehouse.member_enrollment_yearly y
set enrolled_jan = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 1
;

update data_warehouse.member_enrollment_yearly y
set enrolled_feb = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 2
;

update data_warehouse.member_enrollment_yearly y
set enrolled_mar = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 3
;

update data_warehouse.member_enrollment_yearly y
set enrolled_apr = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 4
;

update data_warehouse.member_enrollment_yearly y
set enrolled_may = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 5
;

update data_warehouse.member_enrollment_yearly y
set enrolled_jun = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 6
;

update data_warehouse.member_enrollment_yearly y
set enrolled_jul = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 7
;

update data_warehouse.member_enrollment_yearly y
set enrolled_aug = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 8
;

update data_warehouse.member_enrollment_yearly y
set enrolled_sep = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 9
;

update data_warehouse.member_enrollment_yearly y
set enrolled_oct = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 10
;

update data_warehouse.member_enrollment_yearly y
set enrolled_nov = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 11
;

update data_warehouse.member_enrollment_yearly y
set enrolled_dec = true
from dev.temp_member_enrollment_month m 
where y.uth_member_id = m.uth_member_id 
  and y.year = m.year 
  and m.month = 12
;

--Calculate total_enrolled_months
update data_warehouse.member_enrollment_yearly
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int


-- Drop temp table
drop table dev.temp_member_enrollment_month;

--validate
select * from data_warehouse.member_enrollment_yearly where total_enrolled_months = 12 and data_source = 'optz';

vacuum analyze data_warehouse.member_enrollment_yearly;



---states are calculated base on most lived state in a given year 
select count(*), min(month_year_id) as my, uth_member_id, state, year 
 into dev.wc_state_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, state, year 

create table dev.wc_state_yearly_final 
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my asc) as my_grp
from dev.wc_state_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set state = b.state 
from dev.wc_state_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_state_yearly;

drop table dev.wc_state_yearly_final;

---same logic for zip3
select count(*), min(month_year_id) as my, uth_member_id, zip3, year 
 into dev.wc_zip3_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, zip3, year 

create table dev.wc_zip3_yearly_final 
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my asc) as my_grp
from dev.wc_zip3_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set zip3 = b.zip3
from dev.wc_zip3_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_zip3_yearly;

drop table dev.wc_zip3_yearly_final;

---same logic for zip5
select count(*), min(month_year_id) as my, uth_member_id, zip5, year 
 into dev.wc_zip5_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, zip5, year 
;

create table dev.wc_zip5_yearly_final 
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my asc) as my_grp
from dev.wc_zip5_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set zip5 = b.zip5
from dev.wc_zip5_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_zip5_yearly;

drop table dev.wc_zip5_yearly_final;


---same logic for plan type
select count(*), min(month_year_id) as my, uth_member_id, plan_type, year 
 into dev.wc_plan_type_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, plan_type, year 
;

create table dev.wc_plan_type_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my asc) as my_grp
from dev.wc_plan_type_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set plan_type = b.plan_type
from dev.wc_plan_type_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_plan_type_yearly;

drop table dev.wc_plan_type_yearly_final;


---same logic for EE status
select count(*), min(month_year_id) as my, uth_member_id, employee_status, year 
 into dev.wc_employee_status_yearly
from data_warehouse.member_enrollment_monthly
group by uth_member_id, employee_status, year 
;

create table dev.wc_employee_status_yearly_final
with (appendonly=true, orientation=column)
as
select * , row_number() over(partition by uth_member_id,year order by count desc, my asc) as my_grp
from dev.wc_employee_status_yearly
distributed by(uth_member_id);
  

update data_warehouse.member_enrollment_yearly a set employee_status = b.employee_status
from dev.wc_employee_status_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;


drop table dev.wc_employee_status_yearly;

drop table dev.wc_employee_status_yearly_final;



----cleanup
select * from data_warehouse.member_enrollment_yearly

vacuum analyze data_warehouse.member_enrollment_yearly;


select count(*), count(distinct uth_member_id ), year , data_source 
from  data_warehouse.member_enrollment_yearly
group by year, data_source 
order by data_source , year ;


