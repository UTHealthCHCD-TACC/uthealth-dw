/********************************************************************
 * Script purpose:
 * How do partitioned tables work?
 * A hands-on test
 * 
 * 
 * Author     | Date       | Change
 * ****************************************************************
 * X Zhang    | 04/05/2023 | Script created
 *******************************************************************/


--create a test table which is a narrow slice of data_warehouse.member_enrollment_fiscal_yearly
--this has 3 partitions: mdcd, mcpp, mhtw
drop table if exists backup.xz_partitions_test1;

create table backup.xz_partitions_test1
(like data_warehouse.member_enrollment_fiscal_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition mdcd values ('mdcd'),
  partition mcpp values ('mcpp'),
  partition mhtw values ('mhtw')
 );
 
insert into backup.xz_partitions_test1
select * from data_warehouse.member_enrollment_fiscal_yearly
where fiscal_year = 2020;
--Updated Rows	6052693

--create a table that is JUST the mcpp partition of the previous table
--this table is partitioned, not a normal table
drop table if exists backup.xz_partitions_test2;

create table backup.xz_partitions_test2
	(like backup.xz_partitions_test1 including defaults)
distributed by(uth_member_id)
partition by list(data_source)
	(partition mcpp values ('mcpp'));

insert into backup.xz_partitions_test2
select * from data_warehouse.member_enrollment_fiscal_yearly
where fiscal_year = 2020
and data_source = 'mcpp';


--create a table that is JUST the mcpp partition of previous table
--but this table is NOT partitioned, it IS a normal table
drop table if exists backup.xz_partitions_test3;

create table backup.xz_partitions_test3
	(like backup.xz_partitions_test1 including defaults)
distributed by (uth_member_id);

insert into backup.xz_partitions_test3
select * from data_warehouse.member_enrollment_fiscal_yearly
where fiscal_year = 2020
and data_source = 'mcpp';

--change this table so that we can tell if we updated the table properly
update backup.xz_partitions_test3
set employee_status = 'YAAS';

--vacuum analyze everything
vacuum analyze backup.xz_partitions_test1;
vacuum analyze backup.xz_partitions_test2;
vacuum analyze backup.xz_partitions_test3;

--If you try to swap in a partitioned table, the statement fails
alter table backup.xz_partitions_test1
exchange partition mcpp
with table backup.xz_partitions_test2;

--Here's where the magic happens
--this will swap the partition with the new table
alter table backup.xz_partitions_test1
exchange partition mcpp
with table backup.xz_partitions_test3;

--then you can just rename the swapped partition and stick it in backup
alter table backup.xz_partitions_test3 rename to xz_partitions_test3_old;
alter table backup.xz_partitions_test3_old set schema backup_stage;




select version();
--postgressql 9.4.24
--greenplum 6.16.3



