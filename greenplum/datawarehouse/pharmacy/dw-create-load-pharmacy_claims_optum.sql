-- RX imports

drop table if exists data_warehouse.pharmacy_claims;


create table data_warehouse.pharmacy_claims ( 
		data_source char(4),
		year int2, 
		-- NEW
		data_year int2,
		year_adj int2,
		-- END NEW
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
		-- NEW
		total_charge_amount_adj numeric(13,2),
		total_allowed_amount_adj numeric(13,2),
		total_paid_amount_adj numeric(13,2),
		-- END NEW
		deductible numeric(13,2),
		copay numeric(13,2),
		coins numeric(13,2),
		cob numeric(13,2),
		rx_claim_id_src text,
		member_id_src text
)
with (appendonly=true, orientation = column)
distributed by (uth_member_id);


delete from data_warehouse.pharmacy_claims where data_source = 'optz' or data_source = 'optd';

vacuum analyze data_warehouse.pharmacy_claims;

---optum zip
insert into data_warehouse.pharmacy_claims (
		data_source, year, 
		-- NEW
		year_adj,
		-- END NEW
		uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, 
		total_charge_amount, total_allowed_amount, total_paid_amount,
		-- NEW
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj,
		-- END NEW
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src,
		-- NEW
		data_year, ahfs_class 
		-- END NEW
		)			
select 'optz', extract(year from a.fill_dt), a.std_cost_yr, b.uth_rx_claim_id, b.uth_member_id, patid::text || lpad(ndc, 11,'0') || a.fill_dt,
       lpad(ndc, 11,'0'), a.days_sup, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, 
       a.charge, a.std_cost, null, 
       (a.charge * cf.cost_factor), (a.std_cost * cf.cost_factor), null,
       a.deduct, a.copay, null, null, a.clmid, a.patid::text, a.year, a.ahfsclss 
from optum_zip.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optz' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
	-- NEW
	join reference_tables.ref_optum_cost_factor cf on cf.service_type = 'PHARM' and cf.standard_price_year = a.std_cost_yr::int
	-- END NEW    
 ;

select data_year, count(*)
from data_warehouse.pharmacy_claims pc 
where data_source = 'optz'
group by data_year 
order by data_year;



--optum dod
insert into data_warehouse.pharmacy_claims (
		data_source, year, 
		-- NEW
		year_adj,
		-- END NEW
		uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, 
		total_charge_amount, total_allowed_amount, total_paid_amount,
		-- NEW
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj,
		-- END NEW
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src,
		-- NEW
		data_year, ahfs_class 
		-- END NEW
		)			
select 'optd', extract(year from a.fill_dt), a.std_cost_yr, b.uth_rx_claim_id, b.uth_member_id, patid::text || lpad(ndc, 11,'0') || a.fill_dt,
       lpad(ndc, 11,'0'), a.days_sup, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, 
       a.charge, a.std_cost, null, 
       (a.charge * cf.cost_factor), (a.std_cost * cf.cost_factor), null,
       a.deduct, a.copay, null, null, a.clmid, a.patid::text, a.year, a.ahfsclss 
from optum_dod.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optd' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
	-- NEW
	join reference_tables.ref_optum_cost_factor cf on cf.service_type = 'PHARM' and cf.standard_price_year = a.std_cost_yr::int
	-- END NEW    
 ;
 
select data_year, count(*)
from data_warehouse.pharmacy_claims pc 
where data_source = 'optd'
group by data_year 
order by data_year;