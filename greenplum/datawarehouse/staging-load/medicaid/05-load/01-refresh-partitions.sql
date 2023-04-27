/**********************************
 * Note that attach/detach are for PostgreSQL version 11+... so we can't do it on GP
 */

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;


/*******************************
 * Monthly Enrollment
 ******************************/

--delete old data
delete from data_warehouse.member_enrollment_monthly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;

--insert new data (general Medicaid)
insert into data_warehouse.member_enrollment_monthly_1_prt_mdcd
select * from dw_staging.mcd_member_enrollment_monthly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;

--insert CHIP Perinatal partition into monthly enrollment table
insert into data_warehouse.member_enrollment_monthly_1_prt_mcpp
select * from dw_staging.mcd_member_enrollment_monthly_1_prt_mcpp;
 
--insert HTW partition into monthly enrollment table
insert into data_warehouse.member_enrollment_monthly_1_prt_mhtw
select *  from dw_staging.mcd_member_enrollment_monthly_1_prt_mhtw;

--vacuum analyze
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mcpp;
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mhtw;

/********************************
 * change update_log
 *******************************/

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_monthly';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_monthly_1_prt_mdcd';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_monthly_1_prt_mcpp';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_monthly_1_prt_mhtw';

/*******************************
 * Yearly Enrollment
 ******************************/
 
--delete old data
delete from data_warehouse.member_enrollment_yearly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mdcd;

--insert new data (general Medicaid)
insert into data_warehouse.member_enrollment_yearly_1_prt_mdcd
select * from dw_staging.mcd_member_enrollment_yearly_1_prt_mdcd;

--insert new data (CHIP Perinatal)
insert into data_warehouse.member_enrollment_yearly_1_prt_mcpp
select * from dw_staging.mcd_member_enrollment_yearly_1_prt_mcpp;

--insert new data (HTW)
insert into data_warehouse.member_enrollment_yearly_1_prt_mhtw
select * from dw_staging.mcd_member_enrollment_yearly_1_prt_mhtw;

--vacuum analyze
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mdcd;
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mcpp;
vacuum analyze data_warehouse.member_enrollment_yearly_1_prt_mhtw;

/********************************
 * change update_log
 *******************************/

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_yearly';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_monthly_1_prt_mdcd';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_yearly_1_prt_mcpp';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_yearly_1_prt_mhtw';

/*******************************
 * Fiscal Yearly Enrollment'
 * 
 * Note that Medicaid is the only data source we are doing a fiscal yearly
 * enrollment with so... for now just delete the table and swap schema names
 ******************************/

drop table if exists data_warehouse.member_enrollment_fiscal_yearly;

create table data_warehouse.member_enrollment_fiscal_yearly
(like dw_staging.mcd_member_enrollment_fiscal_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition mdcd values ('mdcd'),
  partition mhtw values ('mhtw'),
  partition mcpp values ('mcpp')
 )
;

insert into data_warehouse.member_enrollment_fiscal_yearly
select * from dw_staging.mcd_member_enrollment_fiscal_yearly;

vacuum analyze data_warehouse.member_enrollment_fiscal_yearly;

/********************************
 * change update_log
 *******************************/

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_fiscal_yearly';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_fiscal_yearly_1_prt_mdcd';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_fiscal_yearly_1_prt_mcpp';

update data_warehouse.update_log
set data_last_updated = '04-03-2023'::date,
	details = 'CHIP Perinatal and HTW split out to their own partitions'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_fiscal_yearly_1_prt_mhtw';

