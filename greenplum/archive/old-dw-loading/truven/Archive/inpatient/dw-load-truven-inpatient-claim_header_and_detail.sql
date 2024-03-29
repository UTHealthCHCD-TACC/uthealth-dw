--- Claim Header------------------------------------------------------------------------------
-------------------------------- truven commercial inpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 8m
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)
select distinct on (uth_claim_id) 
'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, a.caseid,
null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
a.msclmid, a.enrolid, 'ccaes'
from truven.ccaes a
join data_warehouse.dim_uth_claim_id b 
on b.data_source = 'truv'
and b.claim_id_src = a.msclmid::text
and b.member_id_src = a.enrolid::text
;


-------------------------------- truven medicare advantage inpatient------------------------------
---------------------------------------------------------------------------------------------------	
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  								        						              
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, a.caseid,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcrs'
from truven.mdcrs a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source ='truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
;


vacuum analyze data_warehouse.claim_header;


-------- Claim Detail 
-------------------------------- truven commercial inpatient--------------------------------------
--------------------------------------------------------------------------------------------------
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								   								   
select 'truv', b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       a.admdate, a.disdate, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, a.drg,  
       a.msclmid, a.enrolid, 'ccaes'
from truven.ccaes a 
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
 ;
   

-------------------------------- truven medicare adv inpatient--------------------------------------
----------------------------------------------------*-----------------------------------------------	
---
insert into data_warehouse.claim_detail (  data_source, year, uth_claim_id, claim_sequence_number_src, uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, bill_provider_id, ref_provider_id, place_of_service, network_ind, network_paid_ind,
								   admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   charge_amount, allowed_amount, paid_amount, deductible, copay, coins, cob,
								   bill_type_inst, bill_type_class, bill_type_freq, units, drg_cd,
								   claim_id_src, member_id_src, table_id_src )										   								   								   
select 'truv', b.data_year, b.uth_claim_id, a.seqnum, b.uth_member_id, a.svcdate, a.tsvcdat,
       c.month_year_id, a.provid, null, null, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       a.admdate, a.disdate, a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       null, a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob,
       null, null, null,  trunc(a.qty,0) as units, a.drg,  
       a.msclmid, a.enrolid, 'mdcrs'
from truven.mdcrs a 
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.svcdate) 
   and c.year_int = a.year
where a.msclmid is not null
 ;
  
 
vacuum analyze data_warehouse.claim_detail;

vacuum analyze data_warehouse.claim_header;

----- clean up duplicate records--------------------------------------------
--------------------------------------------------------------------------
drop table quarantine.duplicate_claim_headers ;


select uth_claim_id, data_source 
into quarantine.duplicate_claim_headers
from (
	select count(*) as rc, uth_claim_id, data_source
	from data_warehouse.claim_header 
	group by uth_claim_id, data_source 
	) a 
where rc > 1


vacuum analyze quarantine.duplicate_claim_headers; 

select distinct data_source from quarantine.duplicate_claim_headers;

delete from data_warehouse.claim_header where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers);

delete from data_warehouse.claim_detail where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers);

delete from data_warehouse.claim_diag where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers);

delete from data_warehouse.claim_icd_proc where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers);



----------------- Populate bill type and caseid 

select * from data_warehouse.claim_header where data_source  = 'trvc' and admission_id_src  is not null; 

update data_warehouse.claim_detail a 
set bill_type_inst = substring(uth_admission_id::text,1,1),  bill_type_class = substring(uth_admission_id::text,2,1)
from data_warehouse.claim_header b 
where a.uth_member_id  = b.uth_member_id 
  and a.uth_claim_id  = b.uth_claim_id 
  and b.data_source  = 'trvc'

 
 
 drop table dev.wc_trvc_billtyp  
 
 select min(billtyp) as billtype, 
 enrolid, msclmid, caseid 
 into dev.wc_trvc_billtyp 
 from truven.ccaef  a
 where (enrolid::text || msclmid::text || caseid::text) is not null 
 and msclmid <> 1
 group by enrolid, msclmid, caseid
 ;



select count(*) , count(distinct enrolid::text || msclmid::text || caseid::text) from dev.wc_trvc_billtyp


update data_warehouse.claim_detail a 
set bill_type_inst = substring(billtype,1,1),  bill_type_class = substring(billtype,2,1), bill_type_freq = substring(billtype,3,1)
from data_warehouse.claim_header b
  join dev.wc_trvc_billtyp c 
    on c.enrolid::text = b.member_id_src 
   and c.msclmid::text = b.claim_id_src 
   and c.caseid::text = b.admission_id_src 
where a.uth_member_id  = b.uth_member_id 
  and a.uth_claim_id  = b.uth_claim_id 
  and b.data_source  = 'truv'


