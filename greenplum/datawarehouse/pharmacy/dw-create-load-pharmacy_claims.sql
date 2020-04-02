drop table if exists dw_qa.pharmacy_claims;


create table dw_qa.pharmacy_claims ( 
		data_source char(4),
		year int2, 
		uth_rx_claim_id int8,
		uth_member_id int8,
		script_id int8, 
		ndc text,
		refill_count int2,
		fill_date date,
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
		rx_claim_id_src text,
		member_id_src text
)
with (appendonly=true, orientation = column)
distributed by (uth_member_id);

---add Chau on SAS side


insert into dw_qa.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, ndc, refill_count,
		fill_date, month_year_id, generic_ind, generic_name, brand_name,
		quantity, provider_npi, pharmacy_id, total_charge_amount,
		total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob,
		rx_claim_id_src, member_id_src
		)		
		
select 'mdcr',
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   srvc_dt::date,
	   c.month_year_id,
	   --script_id,
	   prod_srvc_id,
	   fill_num,
       brnd_gnrc_cd,
       gnn,
       bn,
       qty_dspnsd_num,
--	   provider_npi, pharmacy_id   
       rx_srvc_rfrnc_num,  
       tot_rx_cst_amt, --, total_allowed_amount, 
       ptnt_pay_amt,
--	   deductible, copay, coins, cob,
	   pde_id, bene_id	   
from medicare.pde_file a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'mdcr' 
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)