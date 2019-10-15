
drop table dev.uth_member_id_hash;
create table dev.uth_member_id_hash
WITH (appendonly=true, orientation=column)
as
select source, mbr_id,  md5(source || cast(mbr_id as varchar)) as uth_mbr_id
from data_warehouse.patient;

-- Check counts
select count(*), count(distinct mbr_id), count(distinct uth_mbr_id)
from dev.uth_member_id_hash;
