


---redistribute dim table for faster join 
drop table if exists dw_staging.truven_rx_claim_id;

create table dw_staging.truven_rx_claim_id
with(appendonly=true, orientation=column, compresstype=zlib)
as select *
from data_warehouse.dim_uth_rx_claim_id
where data_source = 'truv'
distributed by (member_id_src)
;

analyze dw_staging.truven_rx_claim_id;






--truven medicare adv
insert into dw_staging.pharmacy_claims (
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
		table_id_src,
		retail_or_mail_indicator, --new
		dispensed_as_written, --new
		dose, --new
		strength, --new
		formulary_ind, --new
		special_drug_ind --new
		)
select 'truv',
	   extract(year from a.svcdate),
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   a.svcdate,
       lpad(a.ndcnum::text,11,'0'),
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
       dev.fiscal_year_func(a.svcdate) as fiscal_year,
       a.thercls,
       null as ahfs,
       null as first_fill,
       a.enrolid || a.ndcnum::text || svcdate::text,
       a.enrolid::text,
       'mdcrd' as table_id_src,
			 a.rxmr, --new
			 coalesce(a.dawind,'00'), --new
			 r.mstfmds, --new
			 r.strngth, --new
			 null, --new
			 null --new
from truven.mdcrd a 
  join dw_staging.truven_rx_claim_id
     on b.member_id_src = a.member_id_src
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
	left join reference_tables.redbook r --new
	  on r.ndcnum = lpad(a.ndcnum::text,11,'0')
;



------********************************************************************
--truven commercial


---truv commercial
insert into dw_staging.pharmacy_claims (
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
		table_id_src,
		retail_or_mail_indicator,--new
		dispensed_as_written,--new
		dose,--new
		strength,--new
		formulary_ind,--new
		special_drug_ind--new
		)
select 'truv',
	   extract(year from a.svcdate),
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   a.svcdate,
       lpad(a.ndcnum::text,11,'0'),
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
       dev.fiscal_year_func(a.svcdate) as fiscal_year,
       a.thercls,
       null as ahfs,
       null as first_fill,
       a.enrolid || a.ndcnum::text || svcdate::text,
       a.enrolid::text,
       'ccaed' as table_id_src,
			 a.rxmr,--new
			 coalesce(a.dawind,'00'),--new
			 r.mstfmds,--new
			 r.strngth,--new
			 null,--new
			 null --new
from truven.ccaed a
  join dw_staging.truven_rx_claim_id b
     on b.member_id_src = a.member_id_src
    and b.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate)
    and c.year_int = extract(year from a.svcdate)
	left join reference_tables.redbook r -- new
		 on r.ndcnum = lpad(a.ndcnum::text,11,'0')
;


analyze dw_staging.pharmacy_claims;



select count(*), year from truven.ccaed group by year order by year;

select count(*), year from dev.wc_truv_com_rx_load group by year order by year;

---delete old recs
delete from data_warehouse.pharmacy_claims where data_source ='truv' and table_id_src = 'ccaed';

---insert new optz recs
insert into data_warehouse.pharmacy_claims_new
select * from dev.wc_truv_rx_load ;



vacuum analyze data_warehouse.pharmacy_claims_new;

---- validate

select count(*), data_source, year
from data_warehouse.pharmacy_claims_new
group by data_source, year
order by data_source, year


----cleanup

drop table if exists dev.wc_truv_com_rx_load;

drop table if exists dev.wc_truv_mdcd_rx_load;

drop table if exists dev.truv_mdcrd;

drop table if exists dev.truv_ccaed;

drop table if exists dev.dim_uth_rx_truv;
