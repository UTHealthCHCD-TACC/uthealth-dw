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
		data_source, 
		year, 
		uth_rx_claim_id, 
		uth_member_id, 
		script_id, 
		ndc, 
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
select 'mdcr',
       extract(year from srvc_dt::date),
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   null, --script_id
	   prod_srvc_id,
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
from medicare.pde_file a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'mdcr' 
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)
 ;
 


---optum zip
insert into dw_qa.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src
		)			
select 'optz', extract(year from a.fill_dt), b.uth_rx_claim_id, b.uth_member_id, null,
       a.ndc, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, a.charge, a.std_cost, null, 
       a.deduct, a.copay, null, null, a.clmid, a.patid::text
from optum_zip.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optz' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
 ;
 

--optum dod
insert into dw_qa.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src
		)			
select 'optd', extract(year from a.fill_dt), b.uth_rx_claim_id, b.uth_member_id, null,
       a.ndc, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, a.charge, a.std_cost, null, 
       a.deduct, a.copay, null, null, a.clmid, a.patid::text 
from optum_dod.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optd' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
 ;
 

--truven medicare adv
insert into dw_qa.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src
		)
select 'trvm', extract(year from a.svcdate), b.uth_rx_claim_id, b.uth_member_id, null,
       a.ndcnum, a.refill, a.svcdate, c.month_year_id, a.genind, a.generid::text, null, 
       a.qty, a.ntwkprov, a.pharmid, null, a.pay, a.netpay, 
       a.deduct, a.copay, a.coins, a.cob, a.enrolid || ndcnum::text || svcdate::text, a.enrolid::text
from truven.mdcrd a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'trvm' 
    and b.member_id_src = a.enrolid::text
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
 ;
 
--truven commercial
insert into dw_qa.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src
		)
select 'trvc', extract(year from a.svcdate), b.uth_rx_claim_id, b.uth_member_id, null,
       a.ndcnum, a.refill, a.svcdate, c.month_year_id, a.genind, a.generid::text, null, 
       a.qty, a.ntwkprov, a.pharmid, null, a.pay, a.netpay, 
       a.deduct, a.copay, a.coins, a.cob, a.enrolid || ndcnum::text || svcdate::text, a.enrolid::text
from truven.ccaed a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'trvc' 
    and b.member_id_src = a.enrolid::text
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
 ;
 

vacuum analyze dw_qa.pharmacy_claims;


select data_source, substring(month_year_id::text,1,4), count(*) 
from data_warehouse.member_enrollment_monthly
group by data_source, substring(month_year_id::text,1,4)
order by data_source,substring(month_year_id::text,1,4);


alter table dw_qa.pharmacy_claims set schema data_warehouse;

