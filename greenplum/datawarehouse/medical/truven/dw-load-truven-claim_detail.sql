
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
 * */

vacuum analyze data_warehouse.claim_detail;

-- Need to use temp tables to optimize load and avoid 'broadcast motion' which can use up all disk space


alter table data_warehouse.claim_detail add column data_year int2;

delete from data_warehouse.claim_detail where data_source = 'truv';

-------------------------------- truven commercial outpatient--------------------------------------				       
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id,  place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   table_id_src, fiscal_year )		-- data_year ----> fiscal year ~ jw								   								 								   
select 'truv',b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, substr(a.stdplac::text,1,2), a.ntwkprov::bool, a.paidntwk::bool, 
       null, null, a.proc1, a.proctyp, a.procmod, null as proc_mod_2, lpad(a.revcode::text,4,'0'), 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, null,  
       'ccaeo', 
       dev.fiscal_year_func(a.svcdate)
from truven.ccaeo a
  join dev.truven_dim_uth_claim_id b -- jw: i'm not sure where this table is or gets made, but curious if it has data_year, which is being inserted into year for DW table
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
 and a.year between 2015 and 2020 -- jw: not sure which years to include, some of these had different betweens 
  ;
 

-------------------------------- truven medicare outpatient ---------------------------------------
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id,  place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   table_id_src, fiscal_year )								   								 								   
select 'truv',b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, substr(a.stdplac::text,1,2), a.ntwkprov::bool, a.paidntwk::bool, 
       null, null, a.proc1, a.proctyp, a.procmod, null as proc_mod_2, lpad(a.revcode::text,4,'0'), 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob, 
       null, null, null,  trunc(a.qty,0) as units, null,  
       'mdcro', 
       dev.fiscal_year_func(a.svcdate)
from truven.mdcro a 
  join dev.truven_dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
and a.year between 2015 and 2020
;


delete from data_warehouse.claim_detail where table_id_src in ('mdcrs','ccaes');

-------- Claim Detail 
-------------------------------- truven commercial inpatient--------------------------------------
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id,  place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, discharge_status, cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   table_id_src, fiscal_year )										   								   								   
select 'truv', extract(year from a.svcdate), b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, substr(a.stdplac::text,1,2), a.ntwkprov::bool, a.paidntwk::bool, 
       a.admdate, a.disdate, lpad(trim(a.dstatus::text),2,'0'), a.proc1, a.proctyp, a.procmod, null as proc_mod_2, lpad(a.revcode::text,4,'0'), 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, lpad(drg::int::text,3,'0'), 
      'ccaes', 
      dev.fiscal_year_func(a.svcdate)
from truven.ccaes a 
  join dev.truven_dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
and a.year between 2011 and 2014 
 ;



   

-------------------------------- truven medicare adv inpatient--------------------------------------
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, discharge_status, cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								  table_id_src, fiscal_year )										   								   								   
select 'truv', extract(year from a.svcdate), b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, substr(a.stdplac::text,1,2), a.ntwkprov::bool, a.paidntwk::bool, 
       a.admdate, a.disdate, lpad(trim(a.dstatus::text),2,'0'), a.proc1, a.proctyp, a.procmod, null as proc_mod_2, lpad(a.revcode::text,4,'0'), 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units,  lpad(drg::int::text,3,'0'), 
      'mdcrs', 
      dev.fiscal_year_func(a.svcdate)
from truven.mdcrs a 
  join dev.truven_dim_uth_claim_id b
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
 ;

----- Add billtype from facility tables ----- 
select enrolid, msclmid, year, max(billtyp) as billtyp 
into dev.wc_truv_detail_billtyp
from (
--select enrolid::text, msclmid::text, year, billtyp 
--from truven.ccaef 
--union 
select enrolid::text, msclmid::text, year, billtyp 
from truven.mdcrf 
) x
group by enrolid, msclmid, year
;


update data_warehouse.claim_detail 
set bill_type_inst = substring(billtyp,1,1), bill_type_class = substring(billtyp,2,1), bill_type_freq = substring(billtyp,3,1)
from dev.wc_truv_detail_billtyp b 
     where member_id_src = b.enrolid 
     and claim_id_src = b.msclmid
     and data_year = b.year 
;


drop table dev.wc_truv_detail_billtyp

  
 ---validate
vacuum analyze data_warehouse.claim_detail;



select count(*), year, table_id_src 
from data_warehouse.claim_detail 
where data_source = 'truv'
group by "year" , table_id_src 
order by year, table_id_src 
;