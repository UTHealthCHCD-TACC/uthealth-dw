
vacuum analyze data_warehouse.claim_detail;

--Need to use temp tables to optimize load and avoid 'broadcast motion' which can use up all disk space

create table dev.truven_ccaeo
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from truven.ccaeo
where year = 2019
distributed by(msclmid);

create table dev.dim_uth_claim_id
with(appendonly=true,orientation=column)
as select *
from data_warehouse.dim_uth_claim_id
where data_source = 'truv'
distributed by(claim_id_src);




-------- Claim Detail 
-------------------------------- truven commercial inpatient--------------------------------------
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								   								   
select 'truv', extract(year from a.svcdate), b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       a.admdate, a.disdate, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, a.drg,  
       a.msclmid, a.enrolid, 'ccaes'
from truven.ccaes a 
  join dev.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
  and a.year = 2019
 ;
   

-------------------------------- truven medicare adv inpatient--------------------------------------
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								   								   
select 'truv', extract(year from a.svcdate), b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       a.admdate, a.disdate, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, a.drg,  
       a.msclmid, a.enrolid, 'mdcrs'
from truven.mdcrs a 
  join dev.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
  and a.year = 2019
 ;
  
 

-------------------------------- truven commercial outpatient--------------------------------------				       
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								 								   
select 'truv',b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       null, null, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, null,  
       a.msclmid, a.enrolid, 'ccaeo'
from dev.truven_ccaeo a
  join dev.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
  and a.year = 2019 
  ;
 

-------------------------------- truven medicare outpatient ---------------------------------------
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								 								   
select 'truv',b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       null, null, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob, 
       null, null, null,  trunc(a.qty,0) as units, null,  
       a.msclmid, a.enrolid, 'mdcro'
from truven.mdcro a 
  join dev.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source  = 'truv'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
and a.year = 2019;


vacuum analyze data_warehouse.claim_detail;