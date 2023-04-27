/**********************************************************************
 * Truven claim_detail
 * 
 * Code originally by Will/David, updated in 2022 by J. Wozny and
 * version control added March 2023 by Xiaorui
 * ********************************************************************
 * Author  || Date       || Notes
 * ********************************************************************
 * Xiaorui || 03/23/2023 || Changed mapping of pay to match what's on
 * 							the ERD column map (verified by Lopita)
 * --------------------------------------------------------------------
 * Xiaorui || 04/18/2023 || Changed msclmid to claim_id_derv
 ***********************************************************************/

select 'Truven Claim Detail script started at ' || current_timestamp as message;

drop table if exists dw_staging.claim_detail;

--create empty table
create table dw_staging.claim_detail
(like data_warehouse.claim_detail including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;

alter table dw_staging.claim_header owner to uthealth_dev;
vacuum analyze dw_staging.claim_detail;

/*
 * Insert commercial inpatient claim details (ccaes)
 * Joined to header 
 */

insert into dw_staging.claim_detail
select  'truv' as data_source,
		a.year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		null,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        a.admdate, 
        a.disdate, 
        lpad(trim(a.dstatus::text),2,'0'),
        a.proc1 as cpt_hcpcs, 
        null,
        a.procmod, 
        null as proc_mod_2, 
        lpad(substring(a.drg::text from '[0-9]*(?=\.*)'),3,'0') as drg,
        lpad(a.revcode::text,4,'0'),
        null as charge_amount,
        a.pay as allowed_amt,
        a.netpay as paid_amt,
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        substring(f.billtyp,1,1) as billtypeinst,
        substring(f.billtyp,2,1) as billtypeclass, 
        substring(f.billtyp,3,1) as billtypefreq, 
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'ccaes', 
        null, null, null, null, null, null,
		a.claim_id_derv,
		a.enrolid::text,
		current_date,
		a.stdprov
  from staging_clean.ccaes_etl a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.claim_id_derv 
  left outer join  staging_clean.truv_ccaef_etl f
    on f.enrolid = a.enrolid 
   and f.claim_id_derv  = a.claim_id_derv 
 ;

analyze dw_staging.claim_detail_1_prt_truv;

/*
 * Medicare Inpatient Claims (mdcrs)
 */

insert into dw_staging.claim_detail
select  'truv',
		a.year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		null,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        a.admdate, 
        a.disdate, 
        lpad(trim(a.dstatus::text),2,'0'),
        a.proc1 as cpt_hcpcs, 
        null,
        a.procmod, 
        null as proc_mod_2, 
        lpad(substring(a.drg::text from '[0-9]*(?=\.*)'),3,'0') as drg,
        lpad(a.revcode::text,4,'0'), 
        null as charge_amount,
        a.pay as allowed_amt,
        a.netpay as paid_amt,
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        substring(f.billtyp,1,1) as billtypeinst,
        substring(f.billtyp,2,1) as billtypeclass, 
        substring(f.billtyp,3,1) as billtypefreq, 
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'mdcrs', 
        null, null, null, null, null, null,
		a.claim_id_derv,
		a.enrolid::text,
		current_date ,
		a.stdprov
  from staging_clean.mdcrs_etl  a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.claim_id_derv 
  left outer join  staging_clean.truv_mdcrf_etl f
    on f.enrolid = a.enrolid 
   and f.claim_id_derv = a.claim_id_derv 
 ;

analyze dw_staging.claim_detail_1_prt_truv;




----------------------------------------------
-------------------MEDICARE-------------------
----------------------------------------------




/*
 * Medicare Outpatient (mdcro)
 */
insert into dw_staging.claim_detail
select  'truv',
		a.year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		null,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        null as admit, 
        null as discharge_dt,
        null as discharge_status,
        a.proc1 as cpt_hcpcs, 
        null,
        a.procmod, 
        null as proc_mod_2, 
        null as drg,
        lpad(a.revcode::text,4,'0'), 
        null as charge_amount,
        a.pay as allowed_amt,
        a.netpay as paid_amt,
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        substring(f.billtyp,1,1) as billtypeinst,
        substring(f.billtyp,2,1) as billtypeclass, 
        substring(f.billtyp,3,1) as billtypefreq, 
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'mdcro', 
        null, null, null, null, null, null,
		a.claim_id_derv,
		a.enrolid::text,
		current_date ,
		a.stdprov
  from staging_clean.mdcro_etl a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.claim_id_derv 
  left outer join  staging_clean.truv_mdcrf_etl f 
    on f.enrolid = a.enrolid 
   and f.claim_id_derv  = a.claim_id_derv  
   ;
 
analyze dw_staging.claim_detail_1_prt_truv;


---------------------------------------------
-------- Commercial Outpatient (ccaeo)
---------------------------------------------
insert into dw_staging.claim_detail
select  'truv',
		a.year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		null,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        null as admit, 
        null as discharge_dt,
        null as discharge_status,
        a.proc1 as cpt_hcpcs, 
        null,
        a.procmod, 
        null as proc_mod_2, 
        null as drg,
        lpad(a.revcode::text,4,'0'), 
        null as charge_amount,
        a.pay as allowed_amt,
        a.netpay as paid_amt,
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        substring(f.billtyp,1,1) as billtypeinst,
        substring(f.billtyp,2,1) as billtypeclass, 
        substring(f.billtyp,3,1) as billtypefreq, 
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'ccaeo', 
        null, null, null, null, null, null,
		a.claim_id_derv,
		a.enrolid::text,
		current_date , 
		a.stdprov
  from staging_clean.ccaeo_etl a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.claim_id_derv 
  left outer join  staging_clean.truv_ccaef_etl f 
    on f.enrolid = a.enrolid 
   and f.claim_id_derv = a.claim_id_derv 
   ;

analyze dw_staging.claim_detail_1_prt_truv;

select 'Truven Claim Detail script completed at ' || current_timestamp as message;



