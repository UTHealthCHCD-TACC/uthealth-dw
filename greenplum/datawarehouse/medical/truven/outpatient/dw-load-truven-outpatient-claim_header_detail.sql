
-- Claim Header
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 16min

insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						              
select distinct on (uth_claim_id) 
	   'trvc', b.data_year, b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'ccaeo'
from truven.ccaeo a/
/

  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'trvc'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
 where a.year = 2018
;

---------------------------------------------------------------------------------------------------
-------------------------------- truven medicare outpatient ---------------------------------------
---------------------------------------------------------------------------------------------------		        
		        
insert into data_warVaehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						            
select distinct on (uth_claim_id) 
	   'trvm',b.data_year, b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcro'   
from truven.mdcro a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'trvm'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
 where a.year = 2018
;

vacuum analyze reference_tables.ref_month_year;



--- Claim Detail

---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------							       
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								 								   
select 'trvc',b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       null, null, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, null,  
       a.msclmid, a.enrolid, 'ccaeo'
from truven.ccaeo a 
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'trvc'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
  and a.year = 2018
  ;

---------------------------------------------------------------------------------------------------
-------------------------------- truven medicare outpatient ---------------------------------------
---------------------------------------------------------------------------------------------------	
explain
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								 								   
select 'trvm',b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       null, null, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob, 
       null, null, null,  trunc(a.qty,0) as units, null,  
       a.msclmid, a.enrolid, 'mdcro'
from truven.mdcro a 
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'trvm'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
and a.year = 2018
  ;


vacuum analyze data_warehouse.claim_detail;

		       
		       
		       
	select count(*), year, data_source from data_warehouse.claim_detail group by year, data_source;	 

	select count(*), year, data_source from data_warehouse.claim_header group by year, data_source;	 
		       
		       
		       
		       
