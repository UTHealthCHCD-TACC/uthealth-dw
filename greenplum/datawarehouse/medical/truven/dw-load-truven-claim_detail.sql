
/* ******************************************************************************************************
 *  load claim detail for truven
 * ******************************************************************************************************
 *  Author || Date       || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 10/07/2021 || add discharge status and pad rev code, add cpt_hcpcs_cd, 
 * 													remove procmod substrings, added substring to place of service 
 * 													remove old provider and src variables
 * 												--- inpatient tables use extract year from service date for year, outpatient reference dim table, not sure about that - will knows best
 * 												--- need to change data_year to fiscal year, but it shows up multiple places in script referencing other temp tables so i left it in some places
 * ****************************************************************************************************** 
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ****************************************************************************************************** 
   *  jwozny  || 11/05/2021 || added provider variables - note: need to add columns 
 * ****************************************************************************************************** 
 *    jwozny  || 12/17/2021 || added drg code for i tables
 * ****************************************************************************************************** 
 *    jwozny  || 12/23/2021 || added cte to get rid of duplicates in bottom bill type update script
 * ****************************************************************************************************** 
 * */

do $$ 

begin 

-- Need to use temp tables to optimize load and avoid 'broadcast motion' which can use up all disk space



-------------------------------- truven commercial outpatient--------------------------------------				       
insert into dw_staging.claim_detail (  
	data_source,
	year,
	uth_member_id,
	uth_claim_id,
	claim_sequence_number,
	from_date_of_service,
	to_date_of_service,
	month_year_id,
	place_of_service,
	network_ind,
	network_paid_ind,
	admit_date,
	discharge_date,
	discharge_status,
	cpt_hcpcs_cd,
	procedure_type,
	proc_mod_1,
	proc_mod_2,
	drg_cd,
	revenue_cd,
	charge_amount,
	allowed_amount,
	paid_amount,
	copay,
	deductible,
	coins,
	cob,
	bill_type_inst,
	bill_type_class,
	bill_type_freq,
	units,
	fiscal_year,
	cost_factor_year,
	table_id_src,
	claim_sequence_number_src							   
								)								   								   
select  'truv',
		b.data_year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		c.month_year_id,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        null, 
        null, 
        null, 
        a.proc1 as cpt_hcpcs, 
        a.proctyp, 
        a.procmod, 
        null as proc_mod_2, 
        null as drg,
        lpad(a.revcode::text,4,'0'), 
        null, 
        a.pay, 
        a.netpay, 
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        null as bill_type_inst, 
        null as bt_class, 
        null as bt_freq,  
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'ccaeo', 
        a.seqnum 
from truven.ccaeo a
  join dev.truven_dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
  ;
 
raise notice 'ccaeo loaded';

-------------------------------- truven medicare outpatient ---------------------------------------
insert into dw_staging.claim_detail (  
	data_source,
	year,
	uth_member_id,
	uth_claim_id,
	claim_sequence_number,
	from_date_of_service,
	to_date_of_service,
	month_year_id,
	place_of_service,
	network_ind,
	network_paid_ind,
	admit_date,
	discharge_date,
	discharge_status,
	cpt_hcpcs_cd,
	procedure_type,
	proc_mod_1,
	proc_mod_2,
	drg_cd,
	revenue_cd,
	charge_amount,
	allowed_amount,
	paid_amount,
	copay,
	deductible,
	coins,
	cob,
	bill_type_inst,
	bill_type_class,
	bill_type_freq,
	units,
	fiscal_year,
	cost_factor_year,
	table_id_src,
	claim_sequence_number_src							   
								)								   								   
select  'truv',
		b.data_year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		c.month_year_id,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        null, 
        null, 
        null, 
        a.proc1 as cpt_hcpcs, 
        case when substring(proc1,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(proc1,1,1) ~ '[a-zA-Z]' then 'HCPCS' 
	   									 else null end as procedure_type,
        a.procmod, 
        null as proc_mod_2, 
        null as drg,
        lpad(a.revcode::text,4,'0'), 
        null, 
        a.pay, 
        a.netpay, 
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        null as bill_type_inst, 
        null, 
        null,  
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'mdcro', 
        a.seqnum 
 from truven.mdcro a 
  join dev.truven_dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
;


raise notice 'mdcro loaded';

-------- Claim Detail 
-------------------------------- truven commercial inpatient--------------------------------------
insert into dw_staging.claim_detail (  
	data_source,
	year,
	uth_member_id,
	uth_claim_id,
	claim_sequence_number,
	from_date_of_service,
	to_date_of_service,
	month_year_id,
	place_of_service,
	network_ind,
	network_paid_ind,
	admit_date,
	discharge_date,
	discharge_status,
	cpt_hcpcs_cd,
	procedure_type,
	proc_mod_1,
	proc_mod_2,
	drg_cd,
	revenue_cd,
	charge_amount,
	allowed_amount,
	paid_amount,
	copay,
	deductible,
	coins,
	cob,
	bill_type_inst,
	bill_type_class,
	bill_type_freq,
	units,
	fiscal_year,
	cost_factor_year,
	table_id_src,
	claim_sequence_number_src							   
								)								   								   
select  'truv',
		b.data_year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		c.month_year_id,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        a.admdate, 
        a.disdate, 
        lpad(trim(a.dstatus::text),2,'0'),
        a.proc1 as cpt_hcpcs, 
        case when substring(proc1,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(proc1,1,1) ~ '[a-zA-Z]' then 'HCPCS' 
	   									 else null end as procedure_type,
        a.procmod, 
        null as proc_mod_2, 
        a.drg as drg,
        lpad(a.revcode::text,4,'0'), 
        null, 
        a.pay, 
        a.netpay, 
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        null, --substring(f.billtyp,1,1) as billtypeinst,
        null, --substring(f.billtyp,2,1) as billtypeclass, 
        null, --substring(f.billtyp,3,1) as billtypefreq, 
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'ccaes', 
        a.seqnum 
  from truven.ccaes a 
  join dev.truven_dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
  --left outer join truven.ccaef f 
  --  on f.enrolid = a.enrolid 
  -- and f.msclmid = a.msclmid 
  -- and f.year = a.year
where a.msclmid is not null
 ;

raise notice 'ccaes loaded';

   

-------------------------------- truven medicare adv inpatient--------------------------------------
insert into dw_staging.claim_detail (  
	data_source,
	year,
	uth_member_id,
	uth_claim_id,
	claim_sequence_number,
	from_date_of_service,
	to_date_of_service,
	month_year_id,
	place_of_service,
	network_ind,
	network_paid_ind,
	admit_date,
	discharge_date,
	discharge_status,
	cpt_hcpcs_cd,
	procedure_type,
	proc_mod_1,
	proc_mod_2,
	drg_cd,
	revenue_cd,
	charge_amount,
	allowed_amount,
	paid_amount,
	copay,
	deductible,
	coins,
	cob,
	bill_type_inst,
	bill_type_class,
	bill_type_freq,
	units,
	fiscal_year,
	cost_factor_year,
	table_id_src,
	claim_sequence_number_src							   
								)								   								   
select  'truv',
		b.data_year, 
		b.uth_member_id, 
		b.uth_claim_id,
		null as claim_seq,
		a.svcdate,
		a.tsvcdat,
		c.month_year_id,
		substr(a.stdplac::text,1,2), 
		a.ntwkprov::bool, 
		a.paidntwk::bool, 
        a.admdate, 
        a.disdate, 
        lpad(trim(a.dstatus::text),2,'0'),
        a.proc1 as cpt_hcpcs, 
        case when substring(proc1,1,1) ~ '[0-9]' then 'CPT'
	   									 when substring(proc1,1,1) ~ '[a-zA-Z]' then 'HCPCS' 
	   									 else null end as procedure_type,
        a.procmod, 
        null as proc_mod_2, 
        a.drg as drg,
        lpad(a.revcode::text,4,'0'), 
        null, 
        a.pay, 
        a.netpay, 
        a.copay,
        a.deduct,
        a.coins, 
        a.cob,
        null, --substring(f.billtyp,1,1) as billtypeinst,
        null, --substring(f.billtyp,2,1) as billtypeclass, 
        null, --substring(f.billtyp,3,1) as billtypefreq, 
        trunc(a.qty,0) as units,  
        dev.fiscal_year_func(a.svcdate),
        null as cfy,
        'mdcrs', 
        a.seqnum 
from truven.mdcrs a 
  join dev.truven_dim_uth_claim_id  b
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
  --left outer join truven.mdcrf f 
  --  on f.enrolid = a.enrolid 
  -- and f.msclmid = a.msclmid 
  -- and f.year = a.year  
where a.msclmid is not null
 ;

raise notice 'mdcrs loaded';

analyze dw_staging.claim_detail;

end $$;


drop table dev.wc_truv_billtype

create table dev.wc_truv_billtype 
with (appendonly = true, orientation=column, compresstype = zlib) as 
select distinct * from (
	select substring(f.billtyp,1,1) as billtypeinst,
	       substring(f.billtyp,2,1) as billtypeclass, 
	       substring(f.billtyp,3,1) as billtypefreq, 
	       c.uth_member_id,
	       c.uth_claim_id, 
	       rank() over ( partition by uth_claim_id
	                           order by seqnum, svcdate
	                      ) as seq_num
	from truven.mdcrf f 
	   join dev.truven_dim_uth_claim_id c
	     on c.member_id_src = f.member_id_src 
	    and c.claim_id_src = f.msclmid::text
	    and c.data_year = f."year" 
	union 
	select substring(f.billtyp,1,1) as billtypeinst,
	       substring(f.billtyp,2,1) as billtypeclass, 
	       substring(f.billtyp,3,1) as billtypefreq, 
	       c.uth_member_id,
	       c.uth_claim_id, 
	       rank() over ( partition by uth_claim_id
	                           order by seqnum, svcdate 
	                      ) as seq_num
	from truven.ccaef f 
	   join dev.truven_dim_uth_claim_id c
	     on c.member_id_src = f.member_id_src 
	    and c.claim_id_src = f.msclmid::text
	    and c.data_year = f."year"
) inr 
distributed by (uth_member_id)
;


analyze  dev.wc_truv_billtype;

with wc_truv_billtype as ( ---- joe: added cte to get rid of duplicates
		select max(billtypeinst) as billtypeinst,
		 max(billtypeclass) as billtypeclass,
		 max(billtypefreq) as billtypefreq,
		uth_member_id, uth_claim_id, seq_num
		from dev.wc_truv_billtype a
		group by uth_member_id, uth_claim_id, seq_num
)
update dw_staging.claim_detail a 
set bill_type_inst = billtypeinst, bill_type_class = billtypeclass, bill_type_freq = billtypefreq 
from wc_truv_billtype b 
where a.uth_member_id = b.uth_member_id 
  and a.uth_claim_id = b.uth_claim_id 
  and a.claim_sequence_number = b.seq_num
  and a.data_source = 'truv'
;



drop table  dev.wc_truv_billtype;


