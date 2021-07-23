
---Medicare Texas 
insert into data_warehouse.pharmacy_claims (
		data_source, 
		year, 
		uth_rx_claim_id, 
		uth_member_id, 
		script_id, 
		ndc, 
		days_supply,
		refill_count,
		fill_date, 
		month_year_id, 
		generic_ind, 
		generic_name, 
		brand_name,
		quantity, 
		provider_npi, 
		pharmacy_id, 
		total_charge_amount,
		total_allowed_amount, 
		total_paid_amount,
		deductible, copay, coins, cob,
		rx_claim_id_src, 
		member_id_src,
		data_year
		)		
		
		
select 'mcrt',
       a.year::int,
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   bene_id || prod_srvc_id || srvc_dt, --script_id
	   extract(year from srvc_dt::date)
	   prod_srvc_id, --ndc
	   trunc(a.days_suply_num::numeric,0)::int,
	   fill_num::numeric,
	   srvc_dt::date,
	   c.month_year_id,
       brnd_gnrc_cd,
       gnn,
       bn,
       qty_dspnsd_num::numeric,
       srvc_prvdr_id,
       rx_srvc_rfrnc_num,  
       tot_rx_cst_amt::numeric, 
       null, --total_allowed_amount,
       ptnt_pay_amt::numeric,
       null, null, null, null, --	   deductible, copay, coins, cob,
	   pde_id, 
	   bene_id,
	   a.year::int2
from medicare_texas.pde_file a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'mcrt' 
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)
 ;
 


---Medicare National
insert into data_warehouse.pharmacy_claims (
		data_source, 
		year, 
		uth_rx_claim_id, 
		uth_member_id, 
		script_id, 
		ndc, 
		days_supply,
		refill_count,
		fill_date, 
		month_year_id, 
		generic_ind, 
		generic_name, 
		brand_name,
		quantity, 
		provider_npi, 
		pharmacy_id, 
		total_charge_amount,
		total_allowed_amount, 
		total_paid_amount,
		deductible, copay, coins, cob,
		rx_claim_id_src, 
		member_id_src
		)		
select 'mcrn',
       extract (year from srvc_dt::date),
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   bene_id || prod_srvc_id || srvc_dt, --script_id
	   prod_srvc_id, --ndc
	   trunc(a.days_suply_num::numeric,0)::int,
	   fill_num::numeric,
	   srvc_dt::date,
	   c.month_year_id,
       brnd_gnrc_cd,
       gnn,
       bn,
       qty_dspnsd_num::numeric,
       srvc_prvdr_id,
       rx_srvc_rfrnc_num,  
       tot_rx_cst_amt::numeric, 
       null, --total_allowed_amount,
       ptnt_pay_amt::numeric,
       null, null, null, null, --	   deductible, copay, coins, cob,
	   pde_id, 
	   bene_id	   
from medicare_national.pde_file a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'mcrn' 
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)
 ;


--prescript id
with updmcrn as
(
	select b.rx_srvc_rfrnc_num , b.bene_id , b.pde_id , b."year", 
	       row_number () over (partition by b.bene_id , b.pde_id , b.year order by srvc_dt ) as rn 
	from medicare_national.pde_file b 
)
update data_warehouse.pharmacy_claims a set script_id_src = updmcrn.rx_srvc_rfrnc_num
   from updmcrn
   where a.member_id_src = updmcrn.bene_id
     and a.rx_claim_id_src = updmcrn.pde_id
     and a.data_year = updmcrn."year"::int2 
     and updmcrn.rn = 1 
;



