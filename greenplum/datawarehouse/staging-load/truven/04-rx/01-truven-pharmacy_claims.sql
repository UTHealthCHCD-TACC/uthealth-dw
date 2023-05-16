/* ******************************************************************************************************
 *  Truven RX tables : make claims table
 * ******************************************************************************************************
 *  Author  || Date       || Notes
 * ******************************************************************************************************
 *  various || <Apr 2023  ||  Created script
 * ****************************************************************************************************** 
 *  xzhang  || 04/19/2023 || Added flags
 * ******************************************************************************************************
 *  xzhang  || 05/15/2023 || Added drop table/fresh table creation
 * */

select 'Truven RX claims script started at ' || current_timestamp as message;

drop table if exists dw_staging.truv_pharmacy_claims;

create table dw_staging.truv_pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);

--truven medicare adv
insert into dw_staging.truv_pharmacy_claims (
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
		special_drug_ind, --new,
		load_date 
		)
select 'truv',
	   a."year" ,
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   a.svcdate,
       lpad(a.ndcnum::text,11,'0'),
       a.daysupp,
       null as script_id,
       a.refill,
	   get_my_from_date(a.svcdate) as month_year_id,
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
       a.rx_id_src,
       a.enrolid::text,
       'mdcrd' as table_id_src,
			 a.rxmr, --new
			 coalesce(a.dawind,'00'), --new
			 r.mstfmds, --new
			 r.strngth, --new
			 null, --new
			 null, --new
			 current_date
from staging_clean.mdcrd_etl a 
  join staging_clean.truven_rx_claim_id b
     on b.member_id_src = a.enrolid
    and b.rx_claim_id_src = a.rx_id_src 
   left outer join reference_tables.redbook r --new
	 on r.ndcnum = lpad(a.ndcnum::text,11,'0')
;

analyze dw_staging.truv_pharmacy_claims;


---truv commercial
insert into dw_staging.truv_pharmacy_claims (
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
		special_drug_ind,--new
		load_date 
		)
select 'truv',
	   a."year" ,
	   b.uth_rx_claim_id,
	   b.uth_member_id,
	   a.svcdate,
       lpad(a.ndcnum::text,11,'0'),
       a.daysupp,
       null as script_id,
       a.refill,
	   get_my_from_date(a.svcdate) as month_year_id,
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
       a.rx_id_src,
       a.enrolid::text,
       'ccaed' as table_id_src,
			 a.rxmr,--new
			 coalesce(a.dawind,'00'),--new
			 r.mstfmds,--new
			 r.strngth,--new
			 null,--new
			 null, --new
			 current_date
from staging_clean.ccaed_etl a
  join staging_clean.truven_rx_claim_id b
     on b.member_id_src = a.enrolid 
    and b.rx_claim_id_src = a.rx_id_src 
   left outer join reference_tables.redbook r -- new
     on r.ndcnum = lpad(a.ndcnum::text,11,'0')
;

analyze dw_staging.truv_pharmacy_claims;

select 'Truven RX claims script completed at ' || current_timestamp as message;
