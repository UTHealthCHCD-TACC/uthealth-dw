/* find the minimum and maximum values for month_year_id */
select min(month_year_id) as min_year, max(month_year_id) as max_year
from data_warehouse.member_enrollment_monthly
/* result: 200701	201809 */
/* get list of data sources for top-level partition */
select distinct data_source from data_warehouse.member_enrollment_monthly
/* As of 02oct2019: mdcr, trvm, trvc, optz, optd */

/* create the partititioned monthly enrollment table */

create table data_warehouse.member_enrollment_monthly_02oct (
    --ID columns
	id bigserial,
	data_source char(4), 
	month_year_id int4,
	uth_member_id bigint,	
	
	--demographics
	gender_cd char(1),
	state varchar,
	zip5 char(5),
	zip3 char(3),
	age_derived int,
	dob_derived date, 
	death_date date,
	
	--enrollment type
	plan_type char(4),
	bus_cd char(4),
	claim_created_flag bool default false
)
WITH (appendonly=true, orientation=column)
distributed by(id)
partition by list (data_source)
	subpartition by range(month_year_id)
		subpartition template 
	(
		--start(200601) end(201901) every(100),
		--default subpartition spXXXX
		subpartition sp2007 start(200701) INCLUSIVE end(200713) every(1),
		subpartition sp2008 start(200801) INCLUSIVE end(200813) every(1),
		subpartition sp2009 start(200901) INCLUSIVE end(200913) every(1),
		subpartition sp2010 start(201001) INCLUSIVE end(201013) every(1),
		subpartition sp2011 start(201101) INCLUSIVE end(201113) every(1),
		subpartition sp2012 start(201201) INCLUSIVE end(201213) every(1),
		subpartition sp2013 start(201301) INCLUSIVE end(201313) every(1),
		subpartition sp2014 start(201401) INCLUSIVE end(201413) every(1),
		subpartition sp2015 start(201501) INCLUSIVE end(201513) every(1),
		subpartition sp2016 start(201601) INCLUSIVE end(201613) every(1),
		subpartition sp2017 start(201701) INCLUSIVE end(201713) every(1),
		subpartition sp2018 start(201801) INCLUSIVE end(201813) every(1)
	)
(
   partition optz values('optz'),
   partition trvc values('trvc'),
   partition trvm values('trvm'),
   partition optd values('optd'),
   partition mdcr values('mdcr')
   --default partition xxxx 
);

/* this didn't work, Will had to fix it after the table creation above was completed */
alter sequence data_warehouse.member_enrollment_monthly_id_seq cache 200;

/* now populate the new table from the old table - did it first for mdcr since it doesn't have a lot of day, then for the rest */
insert into data_warehouse.member_enrollment_monthly_02oct
select * from data_warehouse.member_enrollment_monthly where data_source <> 'mdcr'

/* now analyze the new table to make sure statistics are collected */
analyze data_warehouse.member_enrollment_monthly



