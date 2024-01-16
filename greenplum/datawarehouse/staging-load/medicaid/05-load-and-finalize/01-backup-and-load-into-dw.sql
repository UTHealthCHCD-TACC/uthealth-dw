/***********************************
 * This script does two things:
 * 	 1 - takes current Medicaid partitions and moves them into backup
 *   2 - swaps in updated Medicaid tables from dw_staging and moved them into DW proper
 * 
 * NOTE: sometimes the switch schema code has a lag of a few seconds.
 * DO NOT PANIC. Just check back in a minute or so. If it still didn't work, THEN panic.
 * 
 * Version control
 * 
 * Date		|| Programmer 	|| Notes
 * ********************************************
 * 09/13/23	|| Xiaorui		|| Created
 * ********************************************
 * 
 ***********************************/

/********************
 * Drop old tables from backup, if they exist
 *******************/
drop table if exists backup.mdcd_member_enrollment_monthly;
drop table if exists backup.mdcd_member_enrollment_yearly;
drop table if exists backup.mdcd_member_enrollment_fiscal_yearly;
drop table if exists backup.mdcd_claim_detail;
drop table if exists backup.mdcd_claim_diag;
drop table if exists backup.mdcd_claim_header;
drop table if exists backup.mdcd_claim_icd_proc;
drop table if exists backup.mdcd_pharmacy_claims;

drop table if exists backup.mhtw_member_enrollment_monthly;
drop table if exists backup.mhtw_member_enrollment_yearly;
drop table if exists backup.mhtw_member_enrollment_fiscal_yearly;
drop table if exists backup.mhtw_claim_detail;
drop table if exists backup.mhtw_claim_diag;
drop table if exists backup.mhtw_claim_header;
drop table if exists backup.mhtw_claim_icd_proc;
drop table if exists backup.mhtw_pharmacy_claims;

drop table if exists backup.mcpp_member_enrollment_monthly;
drop table if exists backup.mcpp_member_enrollment_yearly;
drop table if exists backup.mcpp_member_enrollment_fiscal_yearly;
drop table if exists backup.mcpp_claim_detail;
drop table if exists backup.mcpp_claim_diag;
drop table if exists backup.mcpp_claim_header;
drop table if exists backup.mcpp_claim_icd_proc;
drop table if exists backup.mcpp_pharmacy_claims;

/********************
 * Create empty tables in backup - General medicaid
 *******************/
create table backup.mdcd_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mdcd_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mdcd_member_enrollment_fiscal_yearly
(like data_warehouse.member_enrollment_fiscal_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mdcd_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mdcd_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mdcd_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mdcd_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mdcd_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/********************
 * Create empty tables in backup - HTW
 *******************/
create table backup.mhtw_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mhtw_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mhtw_member_enrollment_fiscal_yearly
(like data_warehouse.member_enrollment_fiscal_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mhtw_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mhtw_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mhtw_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mhtw_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mhtw_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/********************
 * Create empty tables in backup - CHIP Perinatal
 *******************/
create table backup.mcpp_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcpp_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcpp_member_enrollment_fiscal_yearly
(like data_warehouse.member_enrollment_fiscal_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcpp_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcpp_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcpp_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcpp_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcpp_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/*******************
 * Change owner of all tables to uthealth_dev
 * because otherwise the table exchange won't happen
 *******************/
alter table backup.mdcd_member_enrollment_monthly owner to uthealth_dev;
alter table backup.mdcd_member_enrollment_yearly owner to uthealth_dev;
alter table backup.mdcd_member_enrollment_fiscal_yearly owner to uthealth_dev;
alter table backup.mdcd_claim_detail owner to uthealth_dev;
alter table backup.mdcd_claim_diag owner to uthealth_dev;
alter table backup.mdcd_claim_header owner to uthealth_dev;
alter table backup.mdcd_claim_icd_proc owner to uthealth_dev;
alter table backup.mdcd_pharmacy_claims owner to uthealth_dev;

alter table backup.mhtw_member_enrollment_monthly owner to uthealth_dev;
alter table backup.mhtw_member_enrollment_yearly owner to uthealth_dev;
alter table backup.mhtw_member_enrollment_fiscal_yearly owner to uthealth_dev;
alter table backup.mhtw_claim_detail owner to uthealth_dev;
alter table backup.mhtw_claim_diag owner to uthealth_dev;
alter table backup.mhtw_claim_header owner to uthealth_dev;
alter table backup.mhtw_claim_icd_proc owner to uthealth_dev;
alter table backup.mhtw_pharmacy_claims owner to uthealth_dev;

alter table backup.mcpp_member_enrollment_monthly owner to uthealth_dev;
alter table backup.mcpp_member_enrollment_yearly owner to uthealth_dev;
alter table backup.mcpp_member_enrollment_fiscal_yearly owner to uthealth_dev;
alter table backup.mcpp_claim_detail owner to uthealth_dev;
alter table backup.mcpp_claim_diag owner to uthealth_dev;
alter table backup.mcpp_claim_header owner to uthealth_dev;
alter table backup.mcpp_claim_icd_proc owner to uthealth_dev;
alter table backup.mcpp_pharmacy_claims owner to uthealth_dev;

alter table data_warehouse.member_enrollment_monthly owner to uthealth_dev;
alter table data_warehouse.member_enrollment_yearly owner to uthealth_dev;
alter table data_warehouse.member_enrollment_fiscal_yearly owner to uthealth_dev;
alter table data_warehouse.claim_detail owner to uthealth_dev;
alter table data_warehouse.claim_diag owner to uthealth_dev;
alter table data_warehouse.claim_header owner to uthealth_dev;
alter table data_warehouse.claim_icd_proc owner to uthealth_dev;
alter table data_warehouse.pharmacy_claims owner to uthealth_dev;

alter table dw_staging.mcd_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.mcd_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.mcd_member_enrollment_fiscal_yearly owner to uthealth_dev;
alter table dw_staging.mcd_claim_detail owner to uthealth_dev;
alter table dw_staging.mcd_claim_diag owner to uthealth_dev;
alter table dw_staging.mcd_claim_header owner to uthealth_dev;
alter table dw_staging.mcd_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.mcd_pharmacy_claims owner to uthealth_dev;

/********************
 * Move current DW tables to backup - medicaid
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition mdcd
with table backup.mdcd_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition mdcd
with table backup.mdcd_member_enrollment_yearly;

alter table data_warehouse.member_enrollment_fiscal_yearly
exchange partition mdcd
with table backup.mdcd_member_enrollment_fiscal_yearly;

alter table data_warehouse.claim_detail
exchange partition mdcd
with table backup.mdcd_claim_detail;

alter table data_warehouse.claim_diag
exchange partition mdcd
with table backup.mdcd_claim_diag;

alter table data_warehouse.claim_header
exchange partition mdcd
with table backup.mdcd_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition mdcd
with table backup.mdcd_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition mdcd
with table backup.mdcd_pharmacy_claims;

/********************
 * Move current DW tables to backup - HTW
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition mhtw
with table backup.mhtw_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition mhtw
with table backup.mhtw_member_enrollment_yearly;

alter table data_warehouse.member_enrollment_fiscal_yearly
exchange partition mhtw
with table backup.mhtw_member_enrollment_fiscal_yearly;

alter table data_warehouse.claim_detail
exchange partition mhtw
with table backup.mhtw_claim_detail;

alter table data_warehouse.claim_diag
exchange partition mhtw
with table backup.mhtw_claim_diag;

alter table data_warehouse.claim_header
exchange partition mhtw
with table backup.mhtw_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition mhtw
with table backup.mhtw_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition mhtw
with table backup.mhtw_pharmacy_claims;

/********************
 * Move current DW tables to backup - CHIP Perinatal
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition mcpp
with table backup.mcpp_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition mcpp
with table backup.mcpp_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition mcpp
with table backup.mcpp_claim_detail;

alter table data_warehouse.claim_diag
exchange partition mcpp
with table backup.mcpp_claim_diag;

alter table data_warehouse.claim_header
exchange partition mcpp
with table backup.mcpp_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition mcpp
with table backup.mcpp_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition mcpp
with table backup.mcpp_pharmacy_claims;

/********************
 * Meat and Potatoes Part 1: Insert enrollment tables into DW
 * Cannot do the swap thing b/c the data sources are not all 'mdcd'
 *******************/
insert into data_warehouse.member_enrollment_monthly
select * from dw_staging.mcd_member_enrollment_monthly;

insert into data_warehouse.member_enrollment_yearly
select * from dw_staging.mcd_member_enrollment_yearly;

insert into data_warehouse.member_enrollment_fiscal_yearly
select * from dw_staging.mcd_member_enrollment_fiscal_yearly;

/* FY22 - fiscal_yearly table was messed up, so dropped
 * and remade
drop table data_warehouse.member_enrollment_fiscal_yearly;

create table data_warehouse.member_enrollment_fiscal_yearly
as 
select * from dw_staging.mcd_member_enrollment_fiscal_yearly
distributed by(uth_member_id);
*/

/***QA
select data_source, count(*) from data_warehouse.member_enrollment_yearly
where data_source in ('mdcd', 'mhtw', 'mcpp') and year = 2022
group by 1;
/*
mhtw	456676
mcpp	58136
mdcd	6111494 */

select data_source, count(*) from data_warehouse.member_enrollment_fiscal_yearly
where data_source in ('mdcd', 'mhtw', 'mcpp') and fiscal_year = 2022
group by 1;
/*
mhtw	474810
mcpp	75052
mdcd	6228776 */

select data_source, count(*) from data_warehouse.member_enrollment_monthly
where data_source in ('mdcd', 'mhtw', 'mcpp') and fiscal_year = 2022
group by 1;
/*
mhtw	4943332
mcpp	331519
mdcd	68767080 */
*/

/********************
 * Meat and Potatoes Part 2: Swapy swapy
 *******************/

alter table data_warehouse.claim_detail
exchange partition mdcd
with table dw_staging.mcd_claim_detail;

alter table data_warehouse.claim_diag
exchange partition mdcd
with table dw_staging.mcd_claim_diag;

alter table data_warehouse.claim_header
exchange partition mdcd
with table dw_staging.mcd_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition mdcd
with table dw_staging.mcd_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition mdcd
with table dw_staging.mcd_pharmacy_claims;


/*******
 * QA: Did things swap over properly?

select month_year_id, count(*) from data_warehouse.member_enrollment_monthly_1_prt_mdcd
group by month_year_id
having count(*) > 100
order by month_year_id desc limit 5;

--goes up to Aug 2022 so good, it worked

202208	5989079
202207	5942288
202206	5899360
202205	5858741
202204	5820676

select from_date_of_service, count(*) from data_warehouse.claim_icd_proc_1_prt_mdcd
group by from_date_of_service
having count(*) > 10
order by from_date_of_service desc limit 5;

2022-08-31	4746
2022-08-30	5142
2022-08-29	5849
2022-08-28	3322
2022-08-27	3223
 */

/********************************
 * update update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--11/23/2023: Added in claim_status, line_status, and dental claim type

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Added in claim_status, line_status, and dental claim type',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%'
		or table_name like '%mdcd'
		or table_name like '%mhtw'
		or table_name like '%mcpp')
	;

alter table data_warehouse.update_log owner to uthealth_analyst;
alter table backup.update_log owner to uthealth_analyst;

/*check
select *
from data_warehouse.update_log
where schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%'
		or table_name like '%mdcd'
		or table_name like '%mhtw'
		or table_name like '%mcpp')
order by table_name;
*/

--check 2
--select * from data_warehouse.update_log order by table_name;


