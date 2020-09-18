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
	zip5 char(5),
	zip3 char(3),
	age_derived int,
	dob_derived date, 
	death_date date,
	plan_type char(4),
	bus_cd char(4),
	employee_status text, 
	claim_created_flag bool default false,
	row_identifier bigserial
)
WITH (appendonly=true, orientation=column)
distributed by(uth_member_id);


alter sequence data_warehouse.member_enrollment_yearly_row_identifier_seq cache 200


------------------------------------------------------------
vacuum analyze data_warehouse.member_enrollment_yearly;

vacuum analyze data_warehouse.member_enrollment_monthly;



insert into data_warehouse.member_enrollment_yearly (data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag )
select distinct on( data_source, year, uth_member_id ) 
       data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag
from data_warehouse.member_enrollment_monthly
where data_source = 'mcrn'
;

drop table dev.temp_member_enrollment_month;

--Create temp join tables
create table dev.temp_member_enrollment_month
WITH (appendonly=true, orientation=column)
as
select uth_member_id, year, month_year_id, month_year_id % year as month
from data_warehouse.member_enrollment_monthly
where data_source = 'mcrn'
distributed by(uth_member_id);

vacuum analyze dev.temp_member_enrollment_month;


select * from dev.temp_member_enrollment_month;

select count(*), count(distinct uth_member_id), year, data_source
from dev.temp_member_enrollment_month
group by year ,  data_source
order by year , data_source ;

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

vacuum analyze data_warehouse.member_enrollment_yearly;


-- Drop temp table
drop table dev.temp_member_enrollment_month;

--Calculate total_enrolled_months
update data_warehouse.member_enrollment_yearly
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int


select * from data_warehouse.member_enrollment_yearly where total_enrolled_months > 10 and data_source = 'mcrn';



---states are calculated base on most lived state in a given year 
drop table dev.wc_state_yearly_final ;
  
select count(*), min(month_year_id) as my, uth_member_id, state, year 
 into dev.wc_state_yearly
from data_warehouse.member_enrollment_monthly
where data_source = 'mcrn'
group by uth_member_id, state, year 


select * , row_number() over(partition by uth_member_id,year order by count desc, my asc) as my_grp
into dev.wc_state_yearly_final
from dev.wc_state_yearly
--where uth_member_id = 100000030 --100000061
order by uth_member_id, year ;
  

update data_warehouse.member_enrollment_yearly a set state = b.state 
from dev.wc_state_yearly_final b 
where a.uth_member_id = b.uth_member_id
and a.year = b.year 
 and b.my_grp = 1;



vacuum analyze data_warehouse.member_enrollment_yearly;


-----------------------------------------


--Scratch
select count(*), count(distinct uth_member_id ), year , data_source 
from  data_warehouse.member_enrollment_yearly
group by year, data_source 
order by data_source , year ;

select * from  data_warehouse.member_enrollment_yearly where enrolled_jul is false limit 10;
select month_year_id, month_year_id % year as month from  data_warehouse.member_enrollment_monthly order by uth_member_id limit 10;

select year, enrolled_jan, count(*)
from data_warehouse.member_enrollment_yearly
group by 1, 2
order by 1, 2;

--Pick a random uth_member_id and verify
select *
from data_warehouse.member_enrollment_monthly mem 
where uth_member_id = 100312028
order by month_year_id ;

select *
from data_warehouse.member_enrollment_yearly mem 
where uth_member_id = 100312028
order by year;

select count(*), count(distinct uth_member_id)
from data_warehouse.dim_uth_member_id;

select count(*), count(distinct uth_claim_id)
from data_warehouse.dim_uth_claim_id;

select count(*), count(distinct uth_rx_claim_id)
from data_warehouse.dim_uth_rx_claim_id;

