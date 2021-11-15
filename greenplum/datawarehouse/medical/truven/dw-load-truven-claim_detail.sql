
/* ******************************************************************************************************
 *  load claim detail for optum zip and optum dod 
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
        null, 
        null,  
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
        null, 
        null,  
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
where a.msclmid is not null
 ;

raise notice 'mdcrs loaded';

----- Add billtype from facility tables ----- 
select enrolid, msclmid, year, max(billtyp) as billtyp 
into dev.wc_truv_detail_billtyp
from (
	select enrolid::text, msclmid::text, year, billtyp 
	from truven.ccaef 
	union 
	select enrolid::text, msclmid::text, year, billtyp 
	from truven.mdcrf 
) x
group by enrolid, msclmid, year
;


update dw_staging.claim_detail 
set bill_type_inst = substring(billtyp,1,1), bill_type_class = substring(billtyp,2,1), bill_type_freq = substring(billtyp,3,1)
from dev.wc_truv_detail_billtyp b 
     where member_id_src = b.enrolid 
     and claim_id_src = b.msclmid
     and data_year = b.year 
;


drop table dev.wc_truv_detail_billtyp

raise notice 'billtype updated';
  
 ---validate
analyze data_warehouse.claim_detail;

end $$;
