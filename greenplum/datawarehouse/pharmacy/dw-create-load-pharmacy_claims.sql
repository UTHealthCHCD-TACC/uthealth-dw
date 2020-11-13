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
		rx_claim_id_src text,
		member_id_src text,
		data_year int2
)
with (appendonly=true, orientation = column)
distributed by (uth_member_id);



vacuum analyze data_warehouse.pharmacy_claims;

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
		member_id_src
		)		
select 'mdcr',
       a.year::int,
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
from medicare_texas.pde_file a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'mdcr' 
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
from medicare_texas.pde_file a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'mcrn' 
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)
 ;



---optum zip
insert into data_warehouse.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src
		)			
select 'optz', extract(year from a.fill_dt), b.uth_rx_claim_id, b.uth_member_id, patid::text || lpad(ndc, 11,'0') || a.fill_dt,
       lpad(ndc, 11,'0'), a.days_sup, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, a.charge, a.std_cost, null, 
       a.deduct, a.copay, null, null, a.clmid, a.patid::text
from optum_dod.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optz' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
 ;


vacuum analyze data_warehouse.pharmacy_claims;
 

--optum dod
insert into data_warehouse.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src
		)			
select 'optd', extract(year from a.fill_dt), b.uth_rx_claim_id, b.uth_member_id, patid::text || lpad(ndc, 11,'0') || a.fill_dt,
       lpad(ndc, 11,'0'), a.days_sup, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
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
 

select distinct data_source from data_warehouse.pharmacy_claims;


select count(*) from dev.truven_ccaeo


delete from data_warehouse.pharmacy_claims where data_source = 'truv' and data_year = 2019;

drop table dev.dim_uth_rx_truv;

create table dev.dim_uth_rx_truv
with(appendonly=true, orientation=column)
as select * 
from data_warehouse.dim_uth_rx_claim_id 
where data_source = 'truv' 
distributed by (member_id_src)
;




--truven medicare adv
insert into data_warehouse.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src, data_year 
		)
select 'truv', extract(year from a.svcdate), b.uth_rx_claim_id, b.uth_member_id, a.enrolid || lpad(ndcnum::text,11,'0') || svcdate::text,
       lpad(ndcnum::text,11,'0'), a.daysupp, a.refill, a.svcdate, c.month_year_id, a.genind, a.generid::text, null, 
       a.qty, a.ntwkprov, a.pharmid, null, a.pay, a.netpay, 
       a.deduct, a.copay, a.coins, a.cob, a.enrolid || ndcnum::text || svcdate::text, a.enrolid::text, a.year 
from truven.mdcrd a 
  --join data_warehouse.dim_uth_rx_claim_id b
  join dev.dim_uth_rx_truv b
     on b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
    and b.member_id_src = a.enrolid::text
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
 where a.year = 2019;
 

--truven commercial
insert into data_warehouse.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src, data_year 
		)
select 'truv', extract(year from a.svcdate), b.uth_rx_claim_id, b.uth_member_id, a.enrolid || lpad(ndcnum::text,11,'0') || svcdate::text,
       lpad(ndcnum::text,11,'0'), a.daysupp, a.refill, a.svcdate, c.month_year_id, a.genind, a.generid::text, null, 
       a.qty, a.ntwkprov, a.pharmid, null, a.pay, a.netpay, 
       a.deduct, a.copay, a.coins, a.cob, a.enrolid || ndcnum::text || svcdate::text, a.enrolid::text, a.year 
from truven.ccaed a 
  --join data_warehouse.dim_uth_rx_claim_id b
  join dev.dim_uth_rx_truv b 
     on b.member_id_src = a.enrolid::text
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text 
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
where a.year = 2019
;
 
---cleanup
vacuum analyze data_warehouse.pharmacy_claims;

drop table dev.dim_uth_rx_truv; 



---quarantine dupes so 1 row per claim
select uth_rx_claim_id 
into quarantine.rx_duplicate_claims 
from ( 
    select uth_rx_claim_id , count(uth_rx_claim_id ) as cnt 
    from data_warehouse.pharmacy_claims 
    group by uth_rx_claim_id 
    ) x where cnt >  1 
 
    
delete from data_warehouse.pharmacy_claims 
where uth_rx_claim_id in ( select uth_rx_claim_id from quarantine.rx_duplicate_claims);


---- validate

select count(*), data_source, year 
from data_warehouse.pharmacy_claims
group by data_source, year 
order by data_source, year 

  

