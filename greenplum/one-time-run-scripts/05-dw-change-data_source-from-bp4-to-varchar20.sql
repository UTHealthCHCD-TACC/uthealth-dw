/************************************************
 * Script purpose:
 * 	Based on conversation today 03/28/2023 with Dr. Ganduglia,
 * 	Lopita, Jeff, Xiaorui, and Isrrael we decided to split off htw and
 * 	chip_perinatal as their own data sources in data_warehouse
 * 
 * HOWEVER, data_source is currently bpchar(4), so we need to first convert
 * it to varchar(20) and then write some throwaway code to fix
 * 'CHIP Perinatal' in the plan_type table to 'CHIP PERI'
 *
************************************************/

/**************
 * Ok converting data_source to varchar(20) is NOT trivial,
 * 
 * we have to remake all tables
 * load all data in
 * then do-si-do tables around
 * GUH
 */

--code to get the column names and data_types with max character lengths
drop table if exists dev.xz_dw_column_names;

select table_schema, table_name, column_name, ordinal_position,
	data_type, character_maximum_length,
	case when data_type = 'character' then 'bpchar'
		when data_type = 'smallint' then 'int2'
		when data_type = 'integer' then 'int4'
		when data_type = 'bigint' then 'int8'
		when data_type = 'boolean' then 'bool'
		when data_type = 'character varying' then 'text'
		when data_type = 'numeric' then 'numeric(13,2)'
		when data_type = 'double precision' then 'float'
		else data_type
	end as data_type_mod,
	case when character_maximum_length is not null then '(' || character_maximum_length || ')'
		else '' end as char_len_mod,
	null::text as data_type_final
into dev.xz_dw_column_names
from information_schema.columns
where table_schema = 'data_warehouse'
and table_name not like '%_1_%' and
table_name != 'last_analyze_date' --view, not relational table
order by table_name, ordinal_position;

--concatenate bpchar with number of characters
update dev.xz_dw_column_names
set data_type_final = data_type_mod || char_len_mod;

--change all data types for years to int2 (range is +/-32767)
update dev.xz_dw_column_names
set data_type_final = 'int2'
where column_name in ('data_year', 'fiscal_year', 'year');

--most of these are fine, just some claim_ids and one uth_admission_id is numeric(13,2) for no reason
update dev.xz_dw_column_names
set data_type_final = 'int8'
where column_name in ('uth_admission_id', 'uth_claim_id');

--ok change the thing we're actually here to do
--convert data type for data_source from bpchar(4) to varchar(20)
update dev.xz_dw_column_names
set data_type_final = 'varchar(20)'
where column_name = 'data_source';

select * from dev.xz_dw_column_names;
--ok this is good enough

select version();
--PostgreSQL 9.4.24
--(Greenplum Database 6.16.3 build commit:f245f5686a407beb99f46a9ac7e3b9152223724b)
--on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 6.4.0,
--64-bit compiled on Jun 30 2021 19:36:48

select column_name || ' ' || data_type_final || ','
from dev.xz_dw_column_names
where table_name = 'member_enrollment_yearly'
order by ordinal_position;


create table sales.customers (
    customer_id serial PRIMARY KEY,
    first_name varchar(50),
    last_name varchar(50),
    email varchar(100)
);


drop table if exists dw_staging.member_enrollment_monthly;
drop table if exists dw_staging.member_enrollment_yearly;
drop table if exists dw_staging.medicaid_program_enrollment;
drop table if exists dw_staging.claim_header;
drop table if exists dw_staging.claim_detail;
drop table if exists dw_staging.claim_diag;
drop table if exists dw_staging.claim_icd_proc;
drop table if exists dw_staging.pharmacy_claims;

--enrollment yearly
create table dw_staging.member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;



