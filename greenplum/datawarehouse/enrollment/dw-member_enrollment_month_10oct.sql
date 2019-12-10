/* find the minimum and maximum values for month_year_id */
select min(month_year_id) as min_year, max(month_year_id) as max_year
from data_warehouse.member_enrollment_monthly
/* result: 200701	201809 */
/* get list of data sources for top-level partition */
select distinct data_source from data_warehouse.member_enrollment_monthly_10oct
/* As of 02oct2019: mdcr, trvm, trvc, optz, optd */

/* create the partititioned monthly enrollment table */
create table data_warehouse.member_enrollment_monthly_10oct2 (
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
distributed by(uth_member_id, month_year_id)
partition by list (data_source)
	subpartition by range(month_year_id)
		subpartition template 
	(
		--start(200601) end(201901) every(100),
		--default subpartition spXXXX
		subpartition sp2007 start(200701) INCLUSIVE,
		subpartition sp2008 start(200801) INCLUSIVE,
		subpartition sp2009 start(200901) INCLUSIVE,
		subpartition sp2010 start(201001) INCLUSIVE,
		subpartition sp2011 start(201101) INCLUSIVE,
		subpartition sp2012 start(201201) INCLUSIVE,
		subpartition sp2013 start(201301) INCLUSIVE,
		subpartition sp2014 start(201401) INCLUSIVE,
		subpartition sp2015 start(201501) INCLUSIVE,
		subpartition sp2016 start(201601) INCLUSIVE,
		subpartition sp2017 start(201701) INCLUSIVE,
		subpartition sp2018 start(201801) INCLUSIVE
		end(201901) EXCLUSIVE
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
--alter sequence data_warehouse.member_enrollment_monthly_id_seq cache 200;

/* now populate the new table from the old table - did it first for mdcr since it doesn't have a lot of day, then for the rest */
insert into data_warehouse.member_enrollment_monthly_10oct2
select * from data_warehouse.member_enrollment_monthly where data_source <> 'mdcr'

/* check to make sure that the correct partitions and number of partitions were created */
select * from pg_catalog.pg_partitions where tablename='member_enrollment_monthly_10oct2'

/* check to make sure that the data is getting evenly distributed across all the segments */
select gp_segment_id, count(id)
from data_warehouse.member_enrollment_monthly_10oct2
group by gp_segment_id
order by gp_segment_id

/* then make sure the correct permissions are set on the table? */
select dbo.set_table_perms()

/* now rename the old table and the new table use suffix z_ for tables that will eventually be dropped */
alter table data_warehouse.member_enrollment_monthly rename to z_member_enrollment_monthly_by_YEAR2;
alter table data_warehouse.member_enrollment_monthly_10oct2 rename to member_enrollment_monthly;
alter table data_warehouse.z_member_enrollment_orig_partitions rename to z_member_enrollment_monthly_by_year;

/* now analyze the new table to make sure statistics are collected */
vacuum analyze data_warehouse.member_enrollment_monthly;

/* this needs to be done by the DBA */
vacuum analyze;

drop table data_warehouse.z_member_enrollment_monthly_by_year2
