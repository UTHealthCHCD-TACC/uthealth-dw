--- Claim Detail Diag
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 2min 33,622,914
insert into dw_qa.claim_detail_diag (data_source, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  								        						              
select distinct on (uth_claim_id) 
	   'trvc', b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, a.caseid,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'ccaes'
from truven.ccaes a
  join data_warehouse.dim_uth_claim_id b on b.data_source = 'trvc' and b.data_year = trunc(a.year,0) and b.claim_id_src = a.msclmid::text and b.member_id_src = a.enrolid::text
  join dw_qa.claim_detail d on d.uth_claim_id=b.uth_claim_id and 
where trunc(year,0) between 2015 and 2017;




-- SCRATCH
select *
from dw_qa.claim_detail
where data_source='trvc'
limit 10;

select distinct data_source
from dw_qa.claim_detail;