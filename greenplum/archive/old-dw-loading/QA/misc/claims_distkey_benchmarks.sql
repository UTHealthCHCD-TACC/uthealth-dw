analyze dw_qa.claim_detail_diag;
analyze dw_qa.claim_detail;



select data_source, l.table_id_src, diag_position , count(*), count(distinct d.uth_claim_id)
from dw_qa.claim_detail_diag d
join dw_qa.claim_detail l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id and d.uth_member_id=l.uth_member_id 
group by 1, 2, 3
order by 1, 2, 3;

create table 
analyze dev.claim_detail_diag_by_claim;
analyze dev.claim_detail_by_claim;

select data_source, l.table_id_src, diag_position , count(*), count(distinct d.uth_claim_id)
from dev.claim_detail_diag_by_claim d
join dev.claim_detail_by_claim l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id
group by 1, 2, 3
order by 1, 2, 3;


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