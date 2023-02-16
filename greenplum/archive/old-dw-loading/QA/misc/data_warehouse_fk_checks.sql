create view qa_reporting.missing_uth_claim_ids
as
select ch.*
from data_warehouse.claim_header ch
left outer join data_warehouse.dim_uth_claim_id uth on ch.uth_member_id = uth.uth_member_id and ch.uth_claim_id = uth.uth_claim_id 
where uth.uth_claim_id is null;

create view qa_reporting.missing_claim_header
as
select ch.*
from data_warehouse.claim_detail cd
left outer join data_warehouse.claim_header ch on cd.uth_member_id = ch.uth_member_id and cd.uth_claim_id = ch.uth_claim_id 
where ch.uth_claim_id is null;

