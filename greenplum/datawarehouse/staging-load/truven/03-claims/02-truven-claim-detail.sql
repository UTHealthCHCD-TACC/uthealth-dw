
/*
 * 1) Build smaller version of dim_claim
 */

analyze dw_staging.claim_detail;
delete from dw_staging.claim_detail where data_source = 'truv';
vacuum analyze dw_staging.claim_detail;

/*
 * Insert commercial inpatient claim details
 * Joined to header 
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
        a.netpay as charge_amount,
	  	a.pay as allowed_amt,
	  	null as paid_amt, 
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
		a.msclmid::text,
		a.enrolid::text
  from staging_clean.ccaes_etl a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  staging_clean.truv_ccaef_etl f
    on f.enrolid = a.enrolid 
   and f.msclmid  = a.msclmid 
 ;

vacuum analyze dw_staging.claim_detail_1_prt_truv;

/*
 * Medicare Inpatient Claims
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
        a.netpay as charge_amount,
	  	a.pay as allowed_amt,
	  	null as paid_amt, 
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
		a.msclmid::text,
		a.enrolid::text
  from staging_clean.mdcrs_etl  a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  staging_clean.truv_mdcrf_etl f
    on f.enrolid = a.enrolid 
   and f.msclmid = a.msclmid 
 ;

vacuum analyze dw_staging.claim_detail_1_prt_truv;




----------------------------------------------
-------------------MEDICARE-------------------
----------------------------------------------




/*
 * Medicare Outpatient
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
        a.netpay as charge_amount,
	  	a.pay as allowed_amt,
	  	null as paid_amt, 
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
		a.msclmid::text,
		a.enrolid::text
  from staging_clean.mdcro_etl a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  staging_clean.truv_mdcrf_etl f 
    on f.enrolid = a.enrolid 
   and f.msclmid  = a.msclmid  
   ;
 
vacuum analyze dw_staging.claim_detail_1_prt_truv;


---------------------------------------------
-------- Commercial Outpatient 
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
        a.netpay as charge_amount,
	  	a.pay as allowed_amt,
	  	null as paid_amt, 
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
		a.msclmid::text,
		a.enrolid::text
  from staging_clean.ccaeo_etl a 
  join staging_clean.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  staging_clean.truv_ccaef_etl f 
    on f.enrolid = a.enrolid 
   and f.msclmid = a.msclmid 
   ;

vacuum analyze dw_staging.claim_detail_1_prt_truv;

----cleanup