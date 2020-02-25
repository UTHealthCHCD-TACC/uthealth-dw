--- Claim Detail Diag
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 
insert into dw_qa.claim_detail_diag (uth_claim_id, claim_sequence_number, date, diag_cd, diag_position, icd_type, poa_src)  								        						              
select distinct d.uth_claim_id, d.claim_sequence_number , d.from_date_of_service , a.dx1, 1, a.dxver, null 
from dw_qa.claim_detail  d
join truven.ccaes a on d.data_source ='trvc' 
and d.claim_id_src = a.msclmid::text 
and d.member_id_src = a.enrolid::text 
and d.from_date_of_service = a.svcdate 
and d.claim_sequence_number = a.seqnum;


update dw_qa.claim_detail set claim_sequence_number_src =claim_sequence_number::text 
where data_source in ('trvc', 'trvm');

alter table dw_qa.claim_detail_diag alter column claim_sequence_number type int4;


-- SCRATCH
select *
from dw_qa.claim_detail
where data_source='trvc'
limit 10;

select data_source, count(*)
from dw_qa.claim_detail
group by 1;

select data_source, count(*), count(distinct d.uth_claim_id)
from dw_qa.claim_detail_diag d
join dw_qa.claim_detail l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id
group by 1;