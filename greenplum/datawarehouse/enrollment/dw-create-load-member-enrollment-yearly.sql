drop table if exists data_warehouse.member_enrollment_yearly cascade;


create table data_warehouse.member_enrollment_yearly (
	data_source char(4), 
	year int2,
	uth_member_id bigint,
	total_enrolled_months int2,
	enrolled_jan bool,
	enrolled_feb bool,
	enrolled_mar bool,
	enrolled_apr bool,
	enrolled_may bool,
	enrolled_jun bool,
	enrolled_jul bool,
	enrolled_aug bool,
	enrolled_sep bool,
	enrolled_oct bool, 
	enrolled_nov bool, 
	enrolled_dec bool,
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


alter sequence data_warehouse.member_enrollment_yearly_row_identifier_seq cache 200;

vacuum analyze data_warehouse.member_enrollment_monthly;


insert into data_warehouse.member_enrollment_yearly (data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag )
select distinct on( data_source, year, uth_member_id ) 
       data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag
from data_warehouse.member_enrollment_monthly
order by uth_member_id
;

--Create temp join tables
create table dev.temp_member_enrollment_month
WITH (appendonly=true, orientation=column)
as
select uth_member_id, year, month_year_id, month_year_id % year as month
from data_warehouse.member_enrollment_monthly
distributed by(uth_member_id);

analyze dev.temp_member_enrollment_month;

select count(*) from dev.temp_member_enrollment_month;

--Add month flags
--explain
update data_warehouse.member_enrollment_yearly y
set enrolled_may = true
from dev.temp_member_enrollment_month m 
where y.year=m.year and y.uth_member_id=m.uth_member_id and m.month=5;

update data_warehouse.member_enrollment_yearly
set enrolled_may = false 
where enrolled_may is null;

analyze data_warehouse.member_enrollment_yearly;

-- Drop temp table
drop table dev.temp_member_enrollment_month;

--Calculate total_enrolled_months
update data_warehouse.member_enrollment_yearly
set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int

--Set claim_created flag

--Scratch
select count(*) from  data_warehouse.member_enrollment_yearly;

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

