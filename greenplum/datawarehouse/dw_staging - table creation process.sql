/* ******************************************************************************************************
 *  this script is the first step of the dw_staging portion of the data refresh process. It will build tables in dw_staging 
 *  and prepare to load updated data into them.
 *  (1) declare which data sources are being updated
 *  (2) drop any existing staging tables in step 
 *  (3) copy data_warehouse tables down to dw_staging but exclude data from the data_sources that we specified in step 1. 
 * 	(4) member_enrollment_monthly and claim_detail must be explicitly defined with row_number because logic is used to 
 *      populate consecutive enrolled months and claim sequence number respectively. 
 *  (5) assign table ownership to uthealth_dev so anyone with that role can work on these tables 
 * 
 * */


do $$
declare
---(1) change my_data_source according to what data is being updated, use two single quotes around each data source
---example:  my_data_source text := ' (''truv'',''mcrt'',''optz'') ';  //   := ' (''truv'') ';
	my_data_source text ' (''mdcd'',''optd'',''optz'') ';
	med_return boolean;
begin

raise notice 'Creating dw_staging tables for: %', my_data_source;


---(2) drop existing tables
drop table if exists dw_staging.member_enrollment_monthly;
drop table if exists dw_staging.member_enrollment_yearly;

drop table if exists dw_staging.medicaid_program_enrollment;
drop table if exists dw_staging.medicare_mbsf_abcd_enrollment;

drop table if exists dw_staging.claim_header;
drop table if exists dw_staging.claim_detail;
drop table if exists dw_staging.claim_diag;
drop table if exists dw_staging.claim_icd_proc;

drop table if exists dw_staging.pharmacy_claims;

raise notice 'existing tables dropped from dw_staging';

--(3) create tables in dw_staging 


---these two functions build medicare and medicaid enrollment tables
perform public.medicare_enrollment();

perform public.medicaid_enrollment();

raise notice 'medicare mbsf and medicaid enrollment built';

--enrollment yearly
create table dw_staging.member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults including all) 
;

execute 'insert into dw_staging.member_enrollment_yearly 
		select * 
		from data_warehouse.member_enrollment_yearly 
		where data_source not in ' || my_data_source || ';'
;
raise notice 'enrollment yearly created';

--claim header
create table dw_staging.claim_header
(like data_warehouse.claim_header including defaults including all) 
;

execute 'insert into dw_staging.claim_header
		select * 
		from data_warehouse.claim_header
		where data_source not in ' || my_data_source || ';'
;
raise notice 'claim header created';

--claim diag
create table dw_staging.claim_diag
(like data_warehouse.claim_diag including defaults including all)  
;

execute 'insert into dw_staging.claim_diag
		select * 
		from data_warehouse.claim_diag
		where data_source not in ' || my_data_source || ';'
;
raise notice 'claim diag created';

--claim icd proc 
create table dw_staging.claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults including all) 
;

execute 'insert into dw_staging.claim_icd_proc
		select * 
		from data_warehouse.claim_icd_proc
		where data_source not in ' || my_data_source || ';'
;
raise notice 'claim icd proc created';

--pharmacy claims 
create table dw_staging.pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults including all) 
;

execute 'insert into dw_staging.pharmacy_claims
		select * 
		from data_warehouse.pharmacy_claims
		where data_source not in ' || my_data_source || ';'
;

raise notice 'pharmacy claims created';


--(4)
---enrollment monthly - adding row_number sequence
create table dw_staging.member_enrollment_monthly  (
	data_source char(4),
	year int2, 
	uth_member_id bigint,
	month_year_id int4, 
	consecutive_enrolled_months int4, 
	gender_cd char(1), 
	race_cd char(1),
	age_derived int4, 
	dob_derived date, 
	state text, 
	zip5 char(5), 
	zip3 char(3), 
	death_date date, 
	plan_type text, 
	bus_cd char(4), 
	employee_status text, 
	claim_created_flag boolean default false,
	rx_coverage int2, 
	fiscal_year int2,
	family_id text, 
	behavioral_coverage char(1),
	row_id bigserial
) 
with (appendonly=true, orientation=column, compresstype=zlib)
distributed by (row_id)
;
--cache for sequence for performance                                                                        
alter sequence dw_staging.member_enrollment_monthly_row_id_seq cache 200;


-------------insert existing records from data warehouse. except for this data source
execute 'insert into dw_staging.member_enrollment_monthly 
		select * 
		from data_warehouse.member_enrollment_monthly 
		where data_source not in ' || my_data_source || ';'
;


--(4)
---create a copy of production claim detail table - adding row_number sequence
create table dw_staging.claim_detail (
	data_source bpchar(4),
	"year" int2,
	uth_member_id int8,
	uth_claim_id numeric,
	claim_sequence_number int4,
	from_date_of_service date,
	to_date_of_service date,
	month_year_id int4,
	place_of_service text,
	network_ind bool,
	network_paid_ind bool,
	admit_date date,
	discharge_date date,
	discharge_status bpchar(2),
	cpt_hcpcs_cd text,
	procedure_type text,
	proc_mod_1 bpchar(2),
	proc_mod_2 bpchar(2),
	drg_cd text,
	revenue_cd bpchar(4),
	charge_amount numeric(13,2),
	allowed_amount numeric(13,2),
	paid_amount numeric(13,2),
	copay numeric(13,2),
	deductible numeric(13,2),
	coins numeric(13,2),
	cob numeric(13,2),
	bill_type_inst bpchar(1),
	bill_type_class bpchar(1),
	bill_type_freq bpchar(1),
	units int4,
	fiscal_year int2,
	cost_factor_year int2,
	table_id_src text,
	claim_sequence_number_src text,
	row_id bigserial
	) 
with(appendonly=true,orientation=column, compresstype=zlib)
distributed by (row_id)
;

alter sequence dw_staging.claim_detail_row_id_seq cache 200;


-------------insert existing records from data warehouse. except for this data source
execute 'insert into dw_staging.claim_detail 
		select * 
		from data_warehouse.claim_detail
		where data_source not in ' || my_data_source || ';'
;

raise notice 'claim_detail done';

---(5) 
alter table dw_staging.member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.medicaid_program_enrollment owner to uthealth_dev;
alter table dw_staging.medicare_mbsf_abcd_enrollment owner to uthealth_dev;
alter table dw_staging.claim_header owner to uthealth_dev;
alter table dw_staging.claim_detail owner to uthealth_dev;
alter table dw_staging.claim_diag owner to uthealth_dev;
alter table dw_staging.claim_icd_proc owner to uthealth_dev;
alter table dw_staging.pharmacy_claims owner to uthealth_dev;
---/(5)

---(6) 
analyze dw_staging.member_enrollment_monthly;
analyze dw_staging.member_enrollment_yearly;
analyze dw_staging.medicaid_program_enrollment;
analyze dw_staging.medicare_mbsf_abcd_enrollment;
analyze dw_staging.claim_header;
analyze dw_staging.claim_detail;
analyze dw_staging.claim_diag;
analyze dw_staging.claim_icd_proc;
analyze dw_staging.pharmacy_claims;
---/(6)

raise notice 'dw_staging table creation complete for: %', my_data_source;

end $$;

-------------***

--validate
select distinct data_source from dw_staging.member_enrollment_yearly;

select distinct data_source from dw_staging.claim_header;

