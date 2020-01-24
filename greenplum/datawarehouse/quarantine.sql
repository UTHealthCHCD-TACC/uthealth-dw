--UTH Claim Ids
--Note: can come from any source\
create table quarantine.uth_claim_ids (
data_source text,
uth_claim_id int8,
note text
);


select data_source, note, count(*)
from quarantine.uth_claim_ids
group by 1, 2;