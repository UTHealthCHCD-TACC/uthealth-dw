select pay, netpay, ntwkprov, ntwkprov::bool 
from truven.ccaeo; 


-- 10min, 764,063,397
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						        
select 'trvc', b.uth_claim_id, b.uth_member_id, min(a.svcdate), max(a.facprof), max(trunc(stdplac,0)::text), null, null,
       null, sum(a.pay), sum(a.netpay), a.msclmid, a.enrolid, 'ccaeo'
       
       
select distinct on (uth_claim_id) 
	   'trvc', b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay), sum(a.netpay), a.msclmid, a.enrolid, 'ccaeo'
from truven.ccaeo a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'trvc'
   and b.data_year = trunc(a.year,0)
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
where trunc(year,0) between 2015 and 2017
;



						        

insert into dw_qa.claim_detail (  data_source, uth_claim_id, claim_sequence_number,  uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, place_of_service, network_ind, network_paid_ind,
								   procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd,
								   allowed_amount, paid_amount, deductible, copay, coins, cob, units,
								   bill_type_inst, bill_type_class, bill_type_freq, claim_id_src, member_id_src, table_id_src )										 								   
select 'trvc', c.uth_claim_id, a.seqnum, c.uth_member_id, a.svcdate, a.tsvcdat,
       b.month_year_id, a.provid, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), a.revcode, 
       a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob, trunc(a.qty,0), 
       substring(d.billtyp,1,1), substring(d.billtyp,2,1), substring(d.billtyp,3,1), a.msclmid, a.enrolid, 'ccaeo'
     --  d.caseid, d.fachdid, d.billtyp, facprof
     --  dx1, dx2, dx3, dx4, dxver,
from truven.ccaeo a 
  join reference_tables.ref_month_year b
    on month_int = extract(month from svcdate) 
   and year_int = year
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'trvc'
   and c.data_year = trunc(a.year,0)
   and c.claim_id_src = a.msclmid::text
   and c.member_id_src = a.enrolid::text
  left outer join truven.ccaef d 
    on d.msclmid = a.msclmid 
   and d.year = a.year 
   and d.enrolid = a.enrolid 
where a.msclmid is not null
  and a.year between 2015 and 2017
  ;


 
 select * from dev.claim_detail_v1 where uth_claim_id = 152255296912;
 
 
select caseid, msclmid, year, enrolid, *
from truven.ccaes 
where caseid = 852789 and year = 2011;
 



select *, 	
		row_number() over (
		partition by uth_claim_id
		order by claim_sequence_number) rownum 
from dev.claim_detail_v2 
where data_source = 'trvc' 
order by uth_claim_id, claim_sequence_number; 
