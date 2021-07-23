drop table if exists data_warehouse.pharmacy_claims;


create table data_warehouse.pharmacy_claims ( 
		data_source char(4),
		year int2, 
		uth_rx_claim_id int8,
		uth_member_id int8,
		fill_date date,
		ndc char(11) check (length(ndc)=11),
		days_supply int2,
		script_id text, 
		refill_count int2,
		month_year_id int4,
		generic_ind char(1),
		generic_name text,
		brand_name text,
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
		fiscal_year int2,
		cost_factor_year int2,
		therapeutic_class text,
		ahfs_class text,
		first_fill char(1),
		rx_claim_id_src text, 
		member_id_src text, 
		table_id_src text
)
with (appendonly=true, orientation = column)
distributed by (uth_member_id);


vacuum analyze data_warehouse.pharmacy_claims;



