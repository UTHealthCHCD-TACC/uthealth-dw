--- Claim Detail Diag
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 
insert into data_warehouse.claim_detail_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, date, diag_cd, diag_position, icd_type, poa_src)  								        						              
select distinct d.data_source, d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number, d.from_date_of_service , a.dx1, 1, a.dxver, null 
from data_warehouse.claim_detail  d
join truven.ccaes a on d.data_source ='trvc' 
and d.claim_id_src = a.msclmid::text 
and d.member_id_src = a.enrolid::text 
and d.claim_sequence_number_src = a.seqnum::text
where a.year<2018;

--delete from dw_qa.claim_detail_diag where uth_claim_id in (select uth_claim_id from dw_qa.claim_detail where data_source='trvm')

alter table dw_qa.claim_detail_diag alter column claim_sequence_number type int4;


-- SCRATCH

vacuum full data_warehouse.claim_detail;
analyze data_warehouse.claim_detail_diag;

select *
from dw_qa.claim_detail
where data_source='trvm'
limit 10;

select data_source, year, count(*)
from data_warehouse.claim_detail_diag
group by 1, 2
order by 1, 2;


select data_source, diag_position , count(*) --, count(distinct d.uth_claim_id)
from dw_qa.claim_detail_diag d
join dw_qa.claim_detail l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id
group by 1, 2 
order by 1, 2;