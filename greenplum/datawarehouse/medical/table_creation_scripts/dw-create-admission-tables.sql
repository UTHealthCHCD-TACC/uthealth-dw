drop table if exists dw_qa.admission_icd_proc;

create table dw_qa.admission_header (
	data_source char(4),
	year int2,
	uth_admission_id bigint,
	uth_member_id bigint,
	admit_date date,
	discharge_date date,
	admit_type text, 
	discharge_status text,
	primary_diagnosis_cd text,
	primary_icd_proc_cd text,
	bill_type text,
	total_charge_amount numeric(13,2),
	total_allowed_amount numeric(13,2),
	total_paid_amount numeric(13,2),
	admission_id_src text,
	member_id_src text
) with (appendonly=true, orientation = column)
distributed by (uth_member_id);

vacuum analyze dw_qa.admission_header;


create table dw_qa.admission_diag ( 
	data_source char(4),
	year int2,
	uth_admission_id bigint,
	uth_member_id bigint,
	admit_date date,
	diag_cd text, 
	diag_position int4, 
	icd_type text 
) with (appendonly=true, orientation = column)
distributed by (uth_member_id);


vacuum analyze dw_qa.admission_diag;


create table dw_qa.admission_icd_proc ( 
	data_source char(4),
	year int2,
	uth_admission_id bigint,
	uth_member_id bigint,
	admit_date date,
	proc_cd text, 
	proc_position int4, 
	icd_type text 
) with (appendonly=true, orientation = column)
distributed by (uth_member_id);

vacuum analyze dw_qa.admission_icd_proc;


