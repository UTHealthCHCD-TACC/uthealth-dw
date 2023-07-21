
select 'Inserting from mdcro: ' || current_timestamp as message;

insert into dw_staging.trum_claim_detail
select  'trum',
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
		a.stdprov,
		f.billtyp as bill
  from staging_clean.mdcro_etl a 
  join staging_clean.trum_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.claim_id_derv 
  left outer join  staging_clean.truv_mdcrf_etl f 
    on f.enrolid = a.enrolid 
   and f.claim_id_derv  = a.claim_id_derv  
   ;

select 'Analyze dw_staging.trum_claim_detail: ' || current_timestamp as message;
  
analyze dw_staging.trum_claim_detail;

select 'Truven MDCRO Claim Detail script completed at ' || current_timestamp as message;
