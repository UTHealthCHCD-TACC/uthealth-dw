/*********************************
 * 08/16/23 Xiaorui
 * 
 * Truven HPM tables for 2018 have some double-inserts (Sharrah found)
 * 
 * hpm_std
 * hpm_wc
 * 
 * This script:
 * 		1) backs up the truven.hpm_std and truven.hpm_wc tables to backup schema
 * 		2) creates temp tables that are select distinct * from .... the tables in question
 * 		3) deletes from truven.hpm_std and truven.hpm_wc all rows where year = 2018
 * 		4) inserts the rows from temp tables
 * 		5) verify rowcounts, vacuum analyze, and cleanup
 * 
 */

/************************************
 * 01 - Backup to backup schema
 */

drop table if exists backup.truv_hpm_std;
create table backup.truv_hpm_std as
select * from truven.hpm_std;

drop table if exists backup.truv_hpm_wc;
create table backup.truv_hpm_wc as
select * from truven.hpm_wc;

/************************************
 * 02 - create temp tables
 */

drop table if exists dw_staging.truv_hpm_std_2018_temp;

create table dw_staging.truv_hpm_std_2018_temp as
select distinct * from truven.hpm_std
where "year" = 2018;

drop table if exists dw_staging.truv_hpm_wc_2018_temp;

create table dw_staging.truv_hpm_wc_2018_temp as
select distinct * from truven.hpm_wc
where "year" = 2018;

/*************************************
 * 03 - delete year = 2018 from live tables
 */

delete from truven.hpm_std where year = 2018;

delete from truven.hpm_wc where year = 2018;

/***************************************
 * 04 - insert corrected rows into tables
 */

insert into truven.hpm_std
select * from dw_staging.truv_hpm_std_2018_temp;

insert into truven.hpm_wc
select * from dw_staging.truv_hpm_wc_2018_temp;

/****************************************
 * 05 - verify rowcounts and cleanup
 */

select count(*) from truven.hpm_std where year = 2018;

select count(*) from truven.hpm_wc where year = 2018;

vacuum analyze truven.hpm_std;

vacuum analyze truven.hpm_wc;

drop table if exists dw_staging.truv_hpm_std_2018_temp;

drop table if exists dw_staging.truv_hpm_wc_2018_temp;






