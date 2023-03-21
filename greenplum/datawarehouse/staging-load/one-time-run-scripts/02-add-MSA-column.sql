/***************************************************************
 * This script adds an MSA (Metropolitan Statistical Area) column to
 * 		data_warehouse.member_enrollment_monthly
 * 		data_warehouse.member_enrollment_yearly
 * This is because in 2023 we received data from Truven that no longer
 * had zip code and only had MSA
 * 
 * Data type INT is ok because even though MSA is a code, there are
 * no codes that begin with 0 except MSA = 0 = Non-MSA. All MSAs that
 * map to an actual area begin with a non-zero digit
****************************************************************/

--first make a copy of the enrollment tables to staging_clean
--not dw_staging b/c that's where we build tables of the same name
--this is just for safety
create table staging_clean.member_enrollment_yearly 
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
 );

create table staging_clean.member_enrollment_monthly  
(like data_warehouse.member_enrollment_monthly including defaults) 
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
 );

--insert data into empty tables
insert into staging_clean.member_enrollment_yearly
select * from data_warehouse.member_enrollment_yearly;

insert into staging_clean.member_enrollment_monthly
select * from data_warehouse.member_enrollment_monthly;

--vacuum analyze b/c otherwise the counting takes too long
vacuum analyze staging_clean.member_enrollment_yearly;
vacuum analyze staging_clean.member_enrollment_monthly;

--check to make sure things copied properly
select count(*) as dw_yearly_count from data_warehouse.member_enrollment_yearly;
select count(*) as stg_yearly_count from staging_clean.member_enrollment_yearly;

select count(*) as dw_monthly_count from data_warehouse.member_enrollment_monthly;
select count(*) as stg_monthly_count from staging_clean.member_enrollment_monthly;

--drop the data_warehouse tables... it's ok we made copies
drop table if exists data_warehouse.member_enrollment_yearly;
drop table if exists data_warehouse.member_enrollment_monthly;

--build new ones including the MSA column
--could we have just used an alter table add column? Sure, but I wanted
--MSA to live next to Zip and Zip3

select * from information_schema.columns;

create table data_warehouse.member_enrollment_yearly 
	(data_source bpchar(4),
	"year" int2,
	uth_member_id int8,
	total_enrolled_months int2,
	gender_cd bpchar(1),
	race_cd bpchar(1),
	age_derived	int4,
	dob_derived date,
	state varchar,
	zip5 bpchar(5),
	zip3 bpchar(3),
	msa int4,
	death_date date,
	plan_type text,
	bus_cd bpchar(4),
	employee_status	text,
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
	claim_created_flag bool,
	rx_coverage int2,
	fiscal_year int2,
	family_id text,
	behavioral_coverage bpchar(1),
	load_date date,
	dual int2,
	htw	int2,
	member_id_src text,
	table_id_src text) 
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
 );

insert into data_warehouse.member_enrollment_yearly (
	data_source,
	"year",
	uth_member_id,
	total_enrolled_months,
	gender_cd,
	race_cd,
	age_derived,
	dob_derived,
	state,
	zip5,
	zip3,
	death_date,
	plan_type,
	bus_cd,
	employee_status,
	enrolled_jan,
	enrolled_feb,
	enrolled_mar,
	enrolled_apr,
	enrolled_may,
	enrolled_jun,
	enrolled_jul,
	enrolled_aug,
	enrolled_sep,
	enrolled_oct,
	enrolled_nov,
	enrolled_dec,
	claim_created_flag,
	rx_coverage,
	fiscal_year,
	family_id,
	behavioral_coverage,
	load_date,
	dual,
	htw,
	member_id_src,
	table_id_src
)
select * from staging_clean.member_enrollment_yearly;

create table data_warehouse.member_enrollment_monthly
	(data_source bpchar(4),
	"year" int2,
	uth_member_id int8,
	month_year_id int4,
	consecutive_enrolled_months	int2,
	gender_cd bpchar(1),
	race_cd bpchar(1),
	age_derived int2,
	dob_derived	date,
	state text,
	zip5 bpchar(5),
	zip3 bpchar(3),
	msa int4,
	death_date date,
	plan_type text,
	bus_cd bpchar(4),
	employee_status text,
	claim_created_flag bool,
	rx_coverage int2,
	fiscal_year int2,
	family_id text,
	behavioral_coverage bpchar(1),
	load_date date,
	dual int2,
	htw int2,
	age_months int4,
	table_id_src text,
	member_id_src text) 
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
 );


insert into data_warehouse.member_enrollment_monthly (
	data_source,
	year,
	uth_member_id,
	month_year_id,
	consecutive_enrolled_months,
	gender_cd,
	race_cd,
	age_derived,
	dob_derived,
	state,
	zip5,
	zip3,
	death_date,
	plan_type,
	bus_cd,
	employee_status,
	claim_created_flag,
	rx_coverage,
	fiscal_year,
	family_id,
	behavioral_coverage,
	load_date,
	dual,
	htw,
	age_months,
	table_id_src,
	member_id_src)
select * from staging_clean.member_enrollment_monthly;

--vacuum analyze b/c otherwise the counting takes too long
vacuum analyze data_warehouse.member_enrollment_yearly;
vacuum analyze data_warehouse.member_enrollment_monthly;

--check to make sure things copied properly
select count(*) as dw_yearly_count from data_warehouse.member_enrollment_yearly;
select count(*) as stg_yearly_count from staging_clean.member_enrollment_yearly;

select count(*) as dw_monthly_count from data_warehouse.member_enrollment_monthly;
select count(*) as stg_monthly_count from staging_clean.member_enrollment_monthly;

--drop staging tables
drop table if exists staging_clean.member_enrollment_yearly;
drop table if exists staging_clean.member_enrollment_monthly;

/****************
 * code I was using to pull column names
 * 
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'staging_clean'
 and table_name = 'member_enrollment_yearly'
order by ordinal_position;

SELECT column_name || ','
FROM information_schema.columns
WHERE table_schema = 'staging_clean'
 and table_name = 'member_enrollment_yearly'
order by ordinal_position;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'staging_clean'
 and table_name = 'member_enrollment_monthly'
order by ordinal_position;

SELECT column_name || ','
FROM information_schema.columns
WHERE table_schema = 'staging_clean'
 and table_name = 'member_enrollment_monthly'
order by ordinal_position;
 * 
 * */

 */

