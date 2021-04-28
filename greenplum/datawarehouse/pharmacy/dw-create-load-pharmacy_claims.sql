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
		data_year int2,
		total_charge_amount_adj numeric(13,2),
		total_charge_amount_adj numeric(13,2),
		total_charge_amount_adj numeric(13,2),
		year_adj int2,
		therapeutic_class text,
		ahfs_class text,
		first_fill char(1),
		script_id_src text
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
 


--prescript id
with updmcrt as
(
	select b.rx_srvc_rfrnc_num , b.bene_id , b.pde_id , b."year", 
	       row_number () over (partition by b.bene_id , b.pde_id , b.year order by srvc_dt ) as rn 
	from medicare_texas.pde_file b 
)
update data_warehouse.pharmacy_claims a set script_id_src = updmcrt.rx_srvc_rfrnc_num
   from updmcrt
   where a.member_id_src = updmcrt.bene_id
     and a.rx_claim_id_src = updmcrt.pde_id
     and a.data_year = updmcrt."year"::int2 
     and updmcrt.rn = 1 
;


select count(*), data_source, data_year 
from data_warehouse.pharmacy_claims pc 
group by  data_source, data_year 
order by  data_source, data_year 


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



from data_warehouse.pharmacy_claims 
where data_source = 'mcrt';

---optum zip
insert into data_warehouse.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src, first_fill
		)			
select 'optz', extract(year from a.fill_dt), b.uth_rx_claim_id, b.uth_member_id, patid::text || lpad(ndc, 11,'0') || a.fill_dt,
       lpad(ndc, 11,'0'), a.days_sup, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, a.charge, a.std_cost, null, 
       a.deduct, a.copay, null, null, a.clmid, a.patid::text ,,xxxx
from optum_dod.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optz' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
 ;


update data_warehouse.pharmacy_claims a set first_fill = b.fst_fill 

--first fill 
with upd2 as 
(  select b.fst_fill , b.patid, b.clmid , b."year", 
          row_number () over (partition by b.patid, b.clmid , b.year order by fill_dt) as rn 
   from optum_dod.rx b 
) 
update data_warehouse.pharmacy_claims a set first_fill = upd.fst_fill 
   from upd 
   where a.member_id_src = upd.patid::text 
     and a.rx_claim_id_src = upd.clmid::text 
     and a.data_year = upd."year" 
     and upd.rn = 1 
;


alter table data_warehouse.pharmacy_claims add column script_id_src text;

--prescript id
with upd2 as
(
	select b.prescript_id , b.patid, b.clmid , b."year", 
	       row_number () over (partition by b.patid, b.clmid , b.year order by fill_dt) as rn 
	from optum_dod.rx b
)
update data_warehouse.pharmacy_claims a set script_id_src = upd2.prescript_id
   from upd2 
   where a.member_id_src = upd2.patid::text 
     and a.rx_claim_id_src = upd2.clmid::text 
     and a.data_year = upd2."year" 
     and upd2.rn = 1 
;

select * from data_warehouse.pharmacy_claims pc where data_source = 'optd';

vacuum analyze data_warehouse.pharmacy_claims;


select * from optum_dod.rx where prescript_id is null;
 

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


drop table dev.dim_uth_rx_truv;

create table dev.dim_uth_rx_truv
with(appendonly=true, orientation=column)
as select * 
from data_warehouse.dim_uth_rx_claim_id 
where data_source = 'truv' 
distributed by (member_id_src)
;


create table dev.truv_mdcrd
with(appendonly=true, orientation=column)
as select * 
from truven.mdcrd 
distributed by (enrolid)
;


create table dev.truv_ccaed
with(appendonly=true, orientation=column)
as select * 
from truven.ccaed 
distributed by (enrolid)
;

vacuum analyze dev.truv_ccaed;


delete from data_warehouse.pharmacy_claims where data_source = 'truv';


--truven medicare adv
insert into data_warehouse.pharmacy_claims (
		data_source, year, uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src, data_year, therapeutic_class 
		)
select 'truv', extract(year from a.svcdate), b.uth_rx_claim_id, b.uth_member_id, null,
       lpad(ndcnum::text,11,'0'), a.daysupp, a.refill, a.svcdate, c.month_year_id, a.genind, a.generid::text, null, 
       a.qty, a.ntwkprov, a.pharmid, null, a.pay, a.netpay, 
       a.deduct, a.copay, a.coins, a.cob, a.enrolid || ndcnum::text || svcdate::text, a.enrolid::text, a.year , a.thercls 
from dev.truv_mdcrd a 
--from truven.mdcrd a 
  join dev.dim_uth_rx_truv b 
--join data_warehouse.dim_uth_rx_claim_id b
     on b.member_id_src = a.enrolid::text
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
;

select * from truven.mdcrd m 

drop table dev.truv_mdcrd;
 

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
from dev.truv_ccaed a 
--from truven.ccaed a 
  --join data_warehouse.dim_uth_rx_claim_id b
  join dev.dim_uth_rx_truv b 
     on b.member_id_src = a.enrolid::text
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text 
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
;
 
---cleanup
vacuum analyze data_warehouse.pharmacy_claims;

drop table dev.dim_uth_rx_truv; 



---quarantine dupes so 1 row per claim
select uth_rx_claim_id 
into quarantine.rx_duplicate_claims_truv 
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

  

