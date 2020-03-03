drop table if exists dw_qa.pharmacy_claims;


create table dw_qa.pharmacy_claims ( 
		data_source char(4),
		uth_rx_claim_id int8,
		uth_member_id int8,
		fill_date date,
		month_year_id int4,
		script_id int8,
		ndc text,
		brand_name text,
		generic_name text,
		generic_ind char(1),
		quantity int4, 
		provider_npi text,
		pharmacy_id text,
		total_charge_amount numeric(13,2),
		total_allowed_amount numeric(13,2),
		total_paid_amount numeric(13,2),
		deductible numeric(13,2),
		copay numeric(13,2),
		coins numeric(13,2),
		cob numeric(13,2),
		rx_claim_id_src text
)
with (appendonly=true, orientation = column)
distributed by (uth_member_id);



