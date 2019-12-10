alter table reference_tables.ref_zip_crosswalk
	set schema data_warehouse
	
select distinct data_source from data_warehouse.dim_member_id_src limit 10

select * from data_warehouse.ref_data_source

select * from truven.ccaet where enrolid=31973052702


select distinct data_source from data_warehouse.member_enrollment_monthly

explain analyze select count(id) from data_warehouse.member_enrollment_monthly where data_source='optz'



select * from pg_catalog.pg_stat_activity

create materialized view if not exists member_enrollment_monthly_bcbs_mv as
select id, month_year_id, uth_member_id, gender_cd, state, plan_type, death_date, bus_cd
from data_warehouse.member_enrollment_monthly
where data_source='optz'

select version()

select * from pg_catalog.pg_stat_user_tables where schemaname ='data_warehouse'
select * from pg_catalog.pg_stat_activity
select * from pg_catalog.pg_stat_last_operation
select * from pg_catalog.pg_stat_operations
select * from pg_catalog.pg_stat_all_indexes where schemaname='data_warehouse'
select * from pg_catalog.pg_index

show all

explain analyze 
select data_source as source, count(id) as rows
from data_warehouse.member_enrollment_monthly
group by data_source

select data_source as source,month_year_id, count(distinct uth_member_id) as members
from data_warehouse.member_enrollment_monthly_2
--where data_source='trvm'
group by data_source, month_year_id
order by data_source, month_year_id

select data_source as source,month_year_id, count(distinct uth_member_id) as members
from data_warehouse.member_enrollment_monthly
--where data_source='trvm'
group by data_source, month_year_id
order by data_source, month_year_id

select distinct month_year_id from data_warehouse.member_enrollment_monthly
order by month_year_id



select * from data_warehouse.member_enrollment_monthly limit 10

analyze data_warehouse.member_enrollment_monthly;

select * from gp_toolkit.gp_size_of_all_table_indexes

select sodddatname, (sodddatsize/1073741824/1024) as SizeInTB from gp_toolkit.gp_size_of_database
select * from gp_toolkit.gp_size_of_database
--1073741824
select * from gp_toolkit.gp_size_of_index
select * from gp_toolkit.gp_size_of_schema_disk

select sum(dfspace)/1024/1024/1024 as FreeInTB from gp_toolkit.gp_disk_free
select * from gp_toolkit.gp_disk_free order by dfhostname




