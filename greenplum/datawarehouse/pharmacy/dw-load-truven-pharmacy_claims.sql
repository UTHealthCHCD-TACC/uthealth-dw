

--****************************
--   Truven Commercial 
--****************************

drop table if exists dev.dim_uth_rx_truv;

create table dev.dim_uth_rx_truv
with(appendonly=true, orientation=column)
as select * 
from data_warehouse.dim_uth_rx_claim_id 
where data_source = 'truv' 
distributed by (member_id_src)
;

vacuum analyze dev.dim_uth_rx_truv;


create table dev.truv_mdcrd
with(appendonly=true, orientation=column)
as select enrolid::text as member_id_src, * 
from truven.mdcrd 
distributed by (member_id_src)
;

vacuum analyze dev.truv_mdcrd;

create table dev.truv_ccaed
with(appendonly=true, orientation=column)
as select enrolid::text as member_id_src, *  
from truven.ccaed 
distributed by (member_id_src)
;

vacuum analyze dev.truv_ccaed;



---work table to load
drop table if exists dev.wc_truv_mdcd_rx_load;

create table dev.wc_truv_mdcd_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);


--truven medicare adv
insert into dev.wc_truv_mdcd_rx_load (
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
		therapeutic_class, 
		ahfs_class, 
		first_fill,
		rx_claim_id_src, 
		member_id_src, 
		table_id_src
		)
select 'truv', 
	   extract(year from a.svcdate), 
	   b.uth_rx_claim_id, 
	   b.uth_member_id,
	   a.svcdate,
       lpad(ndcnum::text,11,'0'), 
       a.daysupp, 
       null as script_id,
       a.refill,
	   c.month_year_id, 
	   a.genind, 
	   a.generid::text, 
	   null,
       a.qty, 
       a.ntwkprov, 
       a.pharmid, 
       null, a.pay, a.netpay, 
       a.deduct, a.copay, a.coins, a.cob, 
       a.year as fy,
       a.thercls, 
       null as ahfs,
       null as first_fill,
       a.enrolid || ndcnum::text || svcdate::text, 
       a.enrolid::text, 
       'mdcrd' as table_id_src
from dev.truv_mdcrd a -- truven.mdcrd a 
  join dev.dim_uth_rx_truv b --data_warehouse.dim_uth_rx_claim_id b
     on b.member_id_src = a.member_id_src
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
;


vacuum analyze dev.wc_truv_mdcd_rx_load;


select count(*), year from truven.mdcrd group by year order by year;

select count(*), year from dev.wc_truv_mdcd_rx_load group by year order by year;

---delete old recs
delete from data_warehouse.pharmacy_claims where data_source ='truv' and table_id_src = 'mdcrd';

---insert new optz recs
insert into data_warehouse.pharmacy_claims 
select * from dev.wc_truv_mdcd_rx_load ;

 
------********************************************************************
--truven commercial

---work table to load
drop table if exists dev.wc_truv_com_rx_load;

create table dev.wc_truv_com_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);

---truv commercial
insert into dev.wc_truv_com_rx_load (
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
		therapeutic_class, 
		ahfs_class, 
		first_fill,
		rx_claim_id_src, 
		member_id_src, 
		table_id_src
		)
select 'truv', 
	   extract(year from a.svcdate), 
	   b.uth_rx_claim_id, 
	   b.uth_member_id,
	   a.svcdate,
       lpad(ndcnum::text,11,'0'), 
       a.daysupp, 
       null as script_id,
       a.refill,
	   c.month_year_id, 
	   a.genind, 
	   a.generid::text, 
	   null,
       a.qty, 
       a.ntwkprov, 
       a.pharmid, 
       null, a.pay, a.netpay, 
       a.deduct, a.copay, a.coins, a.cob, 
       a.year as fy,
       a.thercls, 
       null as ahfs,
       null as first_fill,
       a.enrolid || ndcnum::text || svcdate::text, 
       a.enrolid::text, 
       'ccaed' as table_id_src
from dev.truv_ccaed a    --truven.ccaed a 
  join dev.dim_uth_rx_truv b   --data_warehouse.dim_uth_rx_claim_id b
     on b.member_id_src = a.member_id_src
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text 
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
;
 

vacuum analyze dev.wc_truv_com_rx_load;



select count(*), year from truven.ccaed group by year order by year;

select count(*), year from dev.wc_truv_com_rx_load group by year order by year;

---delete old recs
delete from data_warehouse.pharmacy_claims where data_source ='truv' and table_id_src = 'ccaed';

---insert new optz recs
insert into data_warehouse.pharmacy_claims 
select * from dev.wc_truv_com_rx_load ;



vacuum analyze data_warehouse.pharmacy_claims;

---- validate

select count(*), data_source, year 
from data_warehouse.pharmacy_claims
group by data_source, year 
order by data_source, year 

  
----cleanup 

drop table if exists dev.wc_truv_com_rx_load;

drop table if exists dev.wc_truv_mdcd_rx_load;

drop table if exists dev.truv_mdcrd;

drop table if exists dev.truv_ccaed;

drop table if exists dev.dim_uth_rx_truv;
