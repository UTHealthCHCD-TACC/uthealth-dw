drop table if exists data_warehouse.member_enrollment_yearly cascade;


create table data_warehouse.member_enrollment_yearly (
	data_source char(4), 
	year int2,
	uth_member_id bigint,
	total_enrolled_months int2,
	enrolled_jan int2,
	enrolled_feb int2,
	enrolled_mar int2,
	enrolled_apr int2,
	enrolled_may int2,
	enrolled_jun int2,
	enrolled_jul int2,
	enrolled_aug int2,
	enrolled_sep int2,
	enrolled_oct int2, 
	enrolled_nov int2, 
	enrolled_dec int2,
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

vacuum analyze data_warehouse.member_enrollment_yearly;


insert into data_warehouse.member_enrollment_yearly (data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag )
select distinct on( data_source, year, uth_member_id ) 
       data_source, year, uth_member_id, gender_cd, state, zip5, zip3, age_derived, dob_derived, death_date
      ,plan_type, bus_cd, employee_status, claim_created_flag
from data_warehouse.member_enrollment_monthly
order by uth_member_id
;



select * from  data_warehouse.member_enrollment_yearly order by uth_member_id;

