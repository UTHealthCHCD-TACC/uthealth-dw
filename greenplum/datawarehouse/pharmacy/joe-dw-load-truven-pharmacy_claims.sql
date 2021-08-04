/*
new variables to be added to table
retail_or_mail_indicator  bpchar(1) null,
dispensed_as_written  bpchar(2) null,
dose bpchar(50)  null,
strength bpchar(30)  null,
formulary_ind  bpchar(1) null,
special_drug_ind bpchar(1) null
*/

--******************************************************************************************************************
------ Truven Medicare mdrcd
---******************************************************************************************************************

---create copy uth claims with truven and distribute on member id src
drop table if exists dev.dim_uth_rx_truv;

create table dev.dim_uth_rx_truv
with(appendonly=true, orientation=column)
as select *
from data_warehouse.dim_uth_rx_claim_id
where data_source = 'truv'
distributed by (member_id_src)
;

vacuum analyze dev.dim_uth_rx_truv;


--create copy of mdcrd pharm table
drop table if exists dev.truv_mdcrd;

create table dev.truv_mdcrd
with(appendonly=true, orientation=column)
as select *
from truven.mdcrd
distributed by (enrolid)
;

vacuum analyze dev.truv_mdcrd;


---work table to load
drop table if exists dev.truv_mdcrd_rx_load;

create table dev.truv_mdcrd_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);

vacuum analyze dev.truv_mdcrd_rx_load;



--mdcrd
insert into dev.truv_mdcrd_rx_load (
		data_source, year, uth_rx_claim_id, uth_member_id, fill_date, ndc, days_supply, script_id,
		refill_count,  month_year_id, generic_ind, generic_name, brand_name,
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src, fiscal_year, total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, year_adj,
		therapeutic_class, ahfs_class, first_fill, script_id_src, retail_or_mail_indicator,
		dispensed_as_written, dose, strength, formulary_ind, special_drug_ind
		)
select 'truv', extract(year from a.svcdate), b.uth_rx_claim_id, b.uth_member_id, a.svcdate,  lpad(a.ndcnum::text,11,'0'), a.daysupp, null,
      a.refill, c.month_year_id, a.genind, a.generid::text, null,
       a.qty, a.ntwkprov, a.pharmid, null, a.pay, a.netpay,
       a.deduct, a.copay, a.coins, a.cob, a.enrolid || a.ndcnum::text || svcdate::text, a.enrolid::text, a.year , null, null, null, null,
			 a.thercls, null, null, null, a.rxmr,
			 coalesce(a.dawind,'00'), r.mstfmds, r.strngth, null, null
from dev.truv_mdcrd a
--from truven.mdcrd a
  join dev.dim_uth_rx_truv b
--join data_warehouse.dim_uth_rx_claim_id b
     on b.member_id_src = a.enrolid::text
    and b.rx_claim_id_src = a.enrolid || a.ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
	left join reference_tables.redbook r
	  on r.ndcnum = lpad(a.ndcnum::text,11,'0')
;

---********************************
--- validate and load to production
----********************************

vacuum analyze dev.truv_mdcrd_rx_load;

select count(*), year
from dev.truv_mdcrd_rx_load group by year order by year;

select count(*), year from truven.mdcrd group by year order by year;

---delete old recs
delete from data_warehouse.pharmacy_claims where data_source = 'truv';

---insert new recs
insert into data_warehouse.pharmacy_claims
select * from dev.truv_mdcrd_rx_load;

vacuum analyze data_warehouse.pharmacy_claims;



--******************************************************************************************************************
------ Truven Commerical ccaed
---******************************************************************************************************************

--create copy of ccaed pharm table
drop table if exists dev.truv_ccaed;

create table dev.truv_ccaed
with(appendonly=true, orientation=column)
as select *
from truven.ccaed
distributed by (enrolid)
;

vacuum analyze dev.truv_ccaed;


---work table to load
drop table if exists dev.truv_ccaed_rx_load;

create table dev.truv_ccaed_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);

vacuum analyze dev.truv_ccaed_rx_load;

--ccaed
insert into dev.truv_ccaed_rx_load (
		data_source, year, uth_rx_claim_id, uth_member_id, fill_date, ndc, days_supply, script_id,
		refill_count,  month_year_id, generic_ind, generic_name, brand_name,
		quantity, provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src, fiscal_year, total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, year_adj,
		therapeutic_class, ahfs_class, first_fill, script_id_src, retail_or_mail_indicator,
		dispensed_as_written, dose, strength, formulary_ind, special_drug_ind
		)
select 'truv', extract(year from a.svcdate), b.uth_rx_claim_id, b.uth_member_id, a.svcdate,  lpad(a.ndcnum::text,11,'0'), a.daysupp, null,
      a.refill, c.month_year_id, a.genind, a.generid::text, null,
       a.qty, a.ntwkprov, a.pharmid, null, a.pay, a.netpay,
       a.deduct, a.copay, a.coins, a.cob, a.enrolid || a.ndcnum::text || svcdate::text, a.enrolid::text, a.year , null, null, null, null,
			 a.thercls, null, null, null, a.rxmr,
			 coalesce(a.dawind,'00'), r.mstfmds, r.strngth, null, null
from dev.truv_ccaed a
--from truven.ccaed a
  --join data_warehouse.dim_uth_rx_claim_id b
  join dev.dim_uth_rx_truv b
     on b.member_id_src = a.enrolid::text
    and b.rx_claim_id_src = a.enrolid || a.ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
		left join reference_tables.redbook r
		  on r.ndcnum = lpad(a.ndcnum::text,11,'0')
;







---********************************
--- validate and load to production
----********************************

vacuum analyze dev.truv_ccaed_rx_load;

select count(*), year
from dev.truv_ccaed_rx_load group by year order by year;

select count(*), year from truven.ccaed group by year order by year;

---delete old recs
delete from data_warehouse.pharmacy_claims where data_source = 'truv';

---insert new recs
insert into data_warehouse.pharmacy_claims
select * from dev.truv_ccaed_rx_load;

vacuum analyze data_warehouse.pharmacy_claims;







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



---cleanup
drop table if exists dev.dim_uth_rx_truv; drop table dev.truv_mdcrd; drop table dev.truv_mdcrd_rx_load;
drop table if exists dev.truv_ccaed; drop table if exists dev.truv_ccaed_rx_load;


/*
vacuum analyze data_warehouse.pharmacy_claims;
---cleanup

drop table dev.dim_uth_rx_truv;
