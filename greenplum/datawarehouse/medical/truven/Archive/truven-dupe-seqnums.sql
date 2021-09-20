select * from truven.mdcro where msclmid = 565918666.0 and enrolid = 467763702.0
order by seqnum;


select t.* 
from truven.mdcro t
join data_warehouse.dim_uth_claim_id d on t.msclmid::text=d.claim_id_src and t.enrolid::text=d.member_id_src
where d.uth_claim_id = 3269193078 
order by t.seqnum;
