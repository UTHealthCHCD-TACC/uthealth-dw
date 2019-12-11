/* find the minimum and maximum values for month_year_id */
select min(month_year_id) as min_year, max(month_year_id) as max_year
from data_warehouse.member_enrollment_monthly
/* result: 200701	201809 */

/* create the partititioned monthly enrollment table */
create table data_warehouse.member_enrollment_monthly_3 (like data_warehouse.member_enrollment_monthly)
distributed by (id)
partition by list (data_source)
	subpartition by range(month_year_id)
		subpartition template 
	(
		start(200601) end(201901) every(100),
		default subpartition spXXXX
	)
(
   partition optz values('optz'),
   partition trvc values('trvc'),
   partition trvm values('trvm'),
   partition optd values('optd'),
   default partition xxxx 
)

/* now populate the new table from the old table */
insert into data_warehouse.member_enrollment_monthly_3
select * from data_warehouse.member_enrollment_monthly


select * from gp_toolkit.gp_size_of_table_disk where sotdschemaname='data_warehouse'

drop table data_warehouse.member_enrollment_monthly_2

alter table data_warehouse.member_enrollment_monthly rename to member_enrollment_monthly_old

alter table data_warehouse.member_enrollment_monthly_3 rename to member_enrollment_monthly

create index state_bmp_idx on data_warehouse.member_enrollment_monthly using bitmap (state)
create index plan_type_bmp_idx on data_warehouse.member_enrollment_monthly using bitmap (plan_type)
create index bus_cd_bmp_idx on data_warehouse.member_enrollment_monthly using bitmap (bus_cd)
create index gender_cd_bmp_idx on data_warehouse.member_enrollment_monthly using bitmap (gender_cd)
create index age_derived_bmp_idx on data_warehouse.member_enrollment_monthly using bitmap (age_derived)

analyze data_warehouse.member_enrollment_monthly

vacuum

select * from pg_catalog.pg_stat_user_tables

