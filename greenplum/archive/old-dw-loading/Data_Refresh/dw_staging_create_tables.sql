/*
 * Create empty staging tables 
 * 
 */

--enrollment yearly
create table dw_staging.member_enrollment_yearly 
(like data_warehouse.member_enrollment_yearly including defaults) 
with (
		appendonly=true, 
		orientation=column, 
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

--claim header
create table dw_staging.claim_header
(like data_warehouse.claim_header including defaults) 
with (
		appendonly=true, 
		orientation=column, 
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


--claim diag
create table dw_staging.claim_diag
(like data_warehouse.claim_diag including defaults) 
with (
		appendonly=true, 
		orientation=column, 
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


--claim icd proc 
create table dw_staging.claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (
		appendonly=true, 
		orientation=column, 
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



--pharmacy claims 
create table dw_staging.pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults) 
with (
		appendonly=true, 
		orientation=column, 
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



---enrollment monthly 
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



---claim detail - adding row_number sequence
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
	bill_provider text, 
	ref_provider text, 
	other_provider text,
	perf_rn_provider text, 
	perf_at_provider text, 
	perf_op_provider text,
	row_id bigserial
	) 
with(appendonly=true,orientation=column, compresstype=zlib)
distributed by (row_id)
;

alter sequence dw_staging.claim_detail_row_id_seq cache 200;
