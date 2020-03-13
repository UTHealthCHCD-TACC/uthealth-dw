-- 102 seconds
explain analyze
select h.data_source, count(distinct d.uth_member_id) as member_cnt, count(distinct d.uth_claim_id) as claim_cnt 
from dw_qa.claim_header_by_claim h 
join dw_qa.claim_detail_by_claim d on h.uth_claim_id = d.uth_claim_id 
join dw_qa.claim_detail_diag_by_claim diag on diag.uth_claim_id = d.uth_claim_id and diag.claim_sequence_number = d.claim_sequence_number 
where diag.diag_position = 1
and diag.diag_cd  like 'O%'
group by 1;
-- 118 seconds
explain analyze
select h.data_source, count(distinct d.uth_member_id) as member_cnt, count(distinct d.uth_claim_id) as claim_cnt 
from dw_qa.claim_header_by_member h 
join dw_qa.claim_detail_by_member d on h.uth_claim_id = d.uth_claim_id 
join dw_qa.claim_detail_diag_by_member diag on diag.uth_claim_id = d.uth_claim_id and diag.claim_sequence_number = d.claim_sequence_number 
where diag.diag_position = 1
and diag.diag_cd  like 'O%'
group by 1;