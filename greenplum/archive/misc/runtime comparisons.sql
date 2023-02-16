

drop table dev.wc_claim_detail;

create table dev.wc_claim_header 
with (appendoptimized=true, orientation=column)
as
select * 
from data_warehouse.claim_header 
distributed by (uth_member_id)
;


create table dev.wc_claim_detail
with (appendoptimized=true, orientation=column)
as
select * 
from data_warehouse.claim_detail 
distributed by (uth_member_id)
;

vacuum analyze dev.wc_claim_header;

vacuum analyze dev.wc_claim_detail;


create index hdr_idx on dev.wc_claim_header ( uth_claim_id);

create index dtl_idx on dev.wc_claim_detail ( uth_claim_id);

--aggregate column D
select count(*) , a.claim_type 
from data_warehouse.claim_header a
where a.data_source = 'optz'
  and a.year = 2016
group by claim_type 
;

--aggregate column D
select count(*) , a.claim_type 
from dev.wc_claim_header a
where a.data_source = 'optz'
  and a.year = 2016
group by claim_type 
;


---find one claim, column E
select a.uth_claim_id, a.uth_member_id, a.claim_type 
from data_warehouse.claim_header a 
where a.data_source = 'optz'
  and a.year = 2016
  and uth_claim_id = 6771259959
  ;
  
 
 ---find one claim, column E
select a.uth_claim_id, a.uth_member_id, a.claim_type
from dev.wc_claim_header a 
where a.data_source = 'optz'
  and a.year = 2016
  and a.uth_claim_id = 6771259959
  ;
  
 
---join with aggregate col F
select count(*), a.claim_type 
from data_warehouse.claim_header a 
  join data_warehouse.claim_detail b 
    on a.uth_member_id = b.uth_member_id 
   and a.uth_claim_id = b.uth_claim_id 
where a.year = 2016
 and a.data_source = 'optz'
group by a.claim_type 


---join with aggregate col F
select count(a.uth_claim_id), a.claim_type 
from dev.wc_claim_header a 
  join dev.wc_claim_detail b 
    on  a.uth_member_id = b.uth_member_id 
    and a.uth_claim_id = b.uth_claim_id 
where a.year = 2016
 and a.data_source = 'optz'
group by a.claim_type 

---join with aggregate col F
select a.uth_claim_id , a.claim_type, b.uth_member_id , b.claim_sequence_number 
from data_warehouse.claim_header a 
  join data_warehouse.claim_detail b 
    on a.uth_member_id = b.uth_member_id 
   and a.uth_claim_id = b.uth_claim_id 
where a.year = 2016
 and a.data_source = 'optz'
 and a.uth_claim_id =  6771259959
;

---join one claim, col G
select a.uth_claim_id , a.claim_type, b.uth_member_id , b.claim_sequence_number 
from dev.wc_claim_header a 
  join dev.wc_claim_detail b 
    on a.uth_member_id = b.uth_member_id 
   and a.uth_claim_id = b.uth_claim_id 
where a.year = 2016
 and a.data_source = 'optz'
 and a.uth_claim_id =  6771259959
 ;