
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
from truven.ccaeo a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'trvc'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
;

---------------------------------------------------------------------------------------------------
-------------------------------- truven medicare outpatient ---------------------------------------
---------------------------------------------------------------------------------------------------		        
		        
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						            
select distinct on (uth_claim_id) 
	   'truv',b.data_year, b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcro'   
from truven.mdcro a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
;

vacuum analyze data_warehouse.claim_header;


delete from data_warehouse.claim_header ch where table_id_src = 'ccaeo'

select count(*) from truven.ccaeo where enrolid is not null 

--- Claim Detail

--Need to use temp tables to optimize load and avoid 'broadcast motion' which can use up all disk space
create table dev.truven_ccaeo
with(appendonly=true,orientation=column)
as select *
from truven.ccaeo
distributed by(msclmid);

create table dev.dim_uth_claim_id
with(appendonly=true,orientation=column)
as select *
from data_warehouse.dim_uth_claim_id
distributed by(claim_id_src);

select count(*) from dev.truven_ccaeo;

---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------	
explain						       
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
from truven.ccaeo a 
  join dev.dim_uth_claim_id b 
    on b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
  ;

table dev.truven_ccaeo;
table dev.dim_uth_claim_id;
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
where a.msclmid is not null;


vacuum analyze data_warehouse.claim_detail;
		  	       
select count(*), year, data_source from data_warehouse.claim_detail group by year, data_source;	 

select count(*), data_source, table_id_src, year from data_warehouse.claim_detail group by year, data_source, table_id_src order by 1, 2, 3;	 

select count(*), year, data_source from data_warehouse.claim_detail group by year, data_source order by 1, 2;	 

select count(*), year, data_source from data_warehouse.claim_header group by year, data_source order by 1, 2;	 

select count(*), year, data_source from data_warehouse.claim_header group by year, data_source;	 

		       
		       
		       
		       
