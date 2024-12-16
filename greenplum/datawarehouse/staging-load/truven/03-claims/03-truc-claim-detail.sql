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
 * --------------------------------------------------------------------
 * Xiaorui || 05/03/2023 || Fixed the way that stdprov converts to text
 * 							stdprov is stored as int and converts like 40 -> 40.0
							to_char(stdprov, 'FM999D') gets rid of decimals and crops to 4 digits
							note that stdprov only goes up to 3 digits (I think) but I just
							didn't want issues
 * --------------------------------------------------------------------
 * Xiaorui || 07/20/2023 || Split into trum and truc, modified stdprov again b/c we fixed problem in script 02
 ***********************************************************************/

select 'Truven CCAE Claim Detail script started at ' || current_timestamp as message;

drop table if exists dw_staging.truc_claim_detail;

--create empty table
create table dw_staging.truc_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

vacuum analyze dw_staging.truc_claim_detail;

/*
 * Insert commercial inpatient claim details (ccaes)
 * Joined to header 
 */

select 'Inserting from ccaes: ' || current_timestamp as message;

insert into dw_staging.truc_claim_detail
select  'truc' as data_source,
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
		a.stdprov,
		f.billtyp
  from staging_clean.ccaes_etl a 
  join staging_clean.truc_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.claim_id_derv 
  left outer join  staging_clean.truv_ccaef_etl f
    on f.enrolid = a.enrolid 
   and f.claim_id_derv  = a.claim_id_derv 
 ;
select 'Analyze dw_staging.truc_claim_detail: ' || current_timestamp as message;

analyze dw_staging.truc_claim_detail;

select 'Inserting from ccaeo: ' || current_timestamp as message;
---------------------------------------------
-------- Commercial Outpatient (ccaeo)
---------------------------------------------
insert into dw_staging.truc_claim_detail
select  'truc',
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
		a.stdprov,
		f.billtyp
  from staging_clean.ccaeo_etl a 
  join staging_clean.truc_dim_id  b 
    on b.member_id_src = a.enrolid 
   and b.claim_id_src = a.claim_id_derv 
  left outer join staging_clean.truv_ccaef_etl f 
    on f.enrolid = a.enrolid 
   and f.claim_id_derv = a.claim_id_derv 
   ;

select 'Analyze dw_staging.truc_claim_detail: ' || current_timestamp as message;

analyze dw_staging.truc_claim_detail;

select 'Truven CCAE Claim Detail script completed at ' || current_timestamp as message;
