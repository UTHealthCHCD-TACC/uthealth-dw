
/*
 * 1) Build smaller version of dim_claim
 */

drop table if exists dev.claim_detail_truv;

create table dev.claim_detail_truv
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

/*
 * Insert inpatient claim details
 * Joined to header 
 */
insert into dev.claim_detail_truv
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
  from dev.ccaes_etl a 
  join dev.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  dev.truv_ccaef_etl f
    on f.enrolid = a.enrolid 
   and f.fachdid = a.fachdid 
 ;

analyze dev.claim_detail_truv;

---------------------------------------------
insert into dev.claim_detail_truv
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
  from dev.ccaeo_etl a 
  join dev.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  dev.truv_ccaef_etl f 
    on f.enrolid = a.enrolid 
   and f.fachdid = a.fachdid 
   ;

vacuum analyze dev.claim_detail_truv;


----------------------------------------------
-------------------MEDICARE-------------------
----------------------------------------------

/*
 * Medicare Inpatient Claims
 */

insert into dev.claim_detail_truv
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
  from dev.mdcrs_etl  a 
  join dev.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  dev.truv_mdcrf_etl f
    on f.enrolid = a.enrolid 
   and f.fachdid = a.fachdid 
 ;

vacuum analyze dev.claim_detail_truv;


/*
 * Medicare Outpatient
 */

insert into dev.claim_detail_truv
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
  from dev.mdcro_etl a 
  join dev.truv_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.msclmid 
  left outer join  dev.truv_mdcrf_etl f 
    on f.enrolid = a.enrolid 
   and f.fachdid = a.fachdid 
   ;
 
vacuum analyze dev.claim_detail_truv;

----cleanup
drop table if exists dev.truv_mdcrf_etl;
drop table if exists dev.truv_mdcrf_etl;
--drop table if exists dev.claim_detail_truv;