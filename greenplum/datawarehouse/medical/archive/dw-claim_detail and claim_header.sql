/* ******************************************************************************************************
 * Original code for creating claim_detail and claim_header.
 * Ran once.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wallingTACC  || 8/23/2021 || archived
 * ******************************************************************************************************
 */

drop table if exists data_warehouse.claim_detail;

create table data_warehouse.claim_detail ( 
		data_source char(4),
		year int2,
		uth_claim_id numeric, 
		claim_sequence_number int4,
		uth_member_id bigint, 
		from_date_of_service date,
		to_date_of_service date,
		month_year_id int4, 
		perf_provider_id text,
		bill_provider_id text,
		ref_provider_id text,
		place_of_service text, 
		network_ind bool,
		network_paid_ind bool,
		admit_date date,
		discharge_date date,
		procedure_cd text,
		procedure_type text,
		proc_mod_1 char(1), 
		proc_mod_2 char(1), 
		revenue_cd char (4),
		charge_amount numeric(13,2),
		allowed_amount numeric(13,2),
		paid_amount numeric(13,2),
		copay numeric(13,2),
		deductible numeric(13,2),
		coins numeric(13,2),
		cob numeric(13,2),	
		bill_type_inst char(1),
		bill_type_class char(1),
		bill_type_freq char(1),
		units int4,
		drg_cd text,
		claim_id_src text,
		member_id_src text,
		table_id_src text,
		claim_sequence_number_src text,
		cob_type text
) with (appendonly=true, orientation = column)
distributed by (uth_member_id);


analyze data_warehouse.claim_detail;


-----------------------------------------------------------------------------------------------

drop table if exists data_warehouse.claim_header;

create table data_warehouse.claim_header (
		data_source char(4),
		year int2,
		uth_claim_id numeric, 
		uth_member_id bigint, 
		from_date_of_service date,
		claim_type text,
		place_of_service char(2),
		uth_admission_id numeric,
		admission_id_src text,
		total_charge_amount numeric(13,2),
		total_allowed_amount numeric(13,2),
		total_paid_amount numeric(13,2),
		claim_id_src text,
		member_id_src text,
		table_id_src text
) with (appendonly=true, orientation = column)
distributed by (uth_member_id);


analyze data_warehouse.claim_header;

-----------------------------------------------------------------------------------------------
