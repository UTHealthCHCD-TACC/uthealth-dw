/*
new variables to be added to table
retail_or_mail_indicator  bpchar(1) null,
dispensed_as_written  bpchar(2) null,
dose bpchar(50)  null,
strength bpchar(30)  null,
formulary_ind  bpchar(1) null,
special_drug_ind bpchar(1) null
*/



---Medicare Texas
insert into data_warehouse.pharmacy_claims_new (
		data_source,
		year,
		uth_rx_claim_id,
		uth_member_id,
		fill_date,
		ndc,
		days_supply,
		script_id,
		refill_count,
		month_year_id,
		generic_ind,
		generic_name,
		brand_name,
		quantity,
		provider_npi,
		pharmacy_id,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob,
		fiscal_year,
		cost_factor_year,
		therapeutic_class,
		ahfs_class,
		first_fill,
		rx_claim_id_src,
		member_id_src,
		table_id_src,
		retail_or_mail_indicator,--new
		dispensed_as_written, --new
		dose, --new
		strength, --new
		formulary_ind, --new
		special_drug_ind --new
		)
select 'mcrt',
       extract(year from srvc_dt::date),
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   srvc_dt::date,
	   prod_srvc_id, --ndc
	   trunc(a.days_suply_num::numeric,0)::int,
	   null as script_id,
	   fill_num::numeric,
	   c.month_year_id,
       brnd_gnrc_cd,
       gnn,
       bn,
       qty_dspnsd_num::numeric,
       srvc_prvdr_id,
       rx_srvc_rfrnc_num,
       tot_rx_cst_amt::numeric as charge, null as total_allowed_amount,  ptnt_pay_amt::numeric as paid,
       null, null, null, null, --	   deductible, copay, coins, cob,
       a.year::int2,
       null as cost_factor_year,
       a.formulary_id as thera_class,
       a.frmlry_rx_id as ahfs_class,
       null as first_fill,
	   pde_id,
	   bene_id,
	   'pde_file' as table_id_src,
		 null, --new
	 	lpad(a.daw_prod_slctn_cd,2,'0'), --new
		a.gcdf_desc, --new
		a.str, --new
		null, --new
		null --new
from uthealth/medicare_national.pde_file a
  join data_warehouse.dim_uth_rx_claim_id b
     on b.data_source = 'mcrt'
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)
 ;


---*********************************************************************************
---Medicare National
insert into data_warehouse.pharmacy_claims_new (
		data_source,
		year,
		uth_rx_claim_id,
		uth_member_id,
		fill_date,
		ndc,
		days_supply,
		script_id,
		refill_count,
		month_year_id,
		generic_ind,
		generic_name,
		brand_name,
		quantity,
		provider_npi,
		pharmacy_id,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob,
		fiscal_year,
		cost_factor_year,
		therapeutic_class,
		ahfs_class,
		first_fill,
		rx_claim_id_src,
		member_id_src,
		table_id_src,
		retail_or_mail_indicator,--new
		dispensed_as_written, --new
		dose, --new
		strength, --new
		formulary_ind, --new
		special_drug_ind--new
		)
select 'mcrn',
       extract(year from srvc_dt::date),
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   srvc_dt::date,
	   prod_srvc_id, --ndc
	   trunc(a.days_suply_num::numeric,0)::int,
	   null as script_id,
	   fill_num::numeric,
	   c.month_year_id,
       brnd_gnrc_cd,
       gnn,
       bn,
       qty_dspnsd_num::numeric,
       srvc_prvdr_id,
       rx_srvc_rfrnc_num,
       tot_rx_cst_amt::numeric as charge, null as total_allowed_amount,  ptnt_pay_amt::numeric as paid,
       null, null, null, null, --	   deductible, copay, coins, cob,
       a.year::int2,
       null as cost_factor_year,
       a.formulary_id as thera_class,
       a.frmlry_rx_id as ahfs_class,
       null as first_fill,
		   pde_id,
		   bene_id,
		   'pde_file' as table_id_src,
				null,--new
				lpad(a.daw_prod_slctn_cd,2,'0'), --new
				a.gcdf_desc, --new
				a.str, --new
				null, --new
				null --new
from medicare_national.pde_file a
  join data_warehouse.dim_uth_rx_claim_id b
     on b.data_source = 'mcrn'
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)
 ;


vacuum analyze data_warehouse.pharmacy_claims_new;

---validate
select count(*), data_source, year
from data_warehouse.pharmacy_claims_new pc
group by data_source , year
order by data_source , year
;
