----------------claim ids ----------------------------------
vacuum analyze data_warehouse.dim_uth_claim_id;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.icn, a.pcn, b.uth_member_id, a.year_fy 
from medicaid.clm_proc a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = a.pcn  
   and b.data_source = 'mdcd'
  left outer join data_warehouse.dim_uth_claim_id c
    on c.member_id_src = a.pcn 
   and c.claim_id_src = a.icn
 where c.uth_member_id is null 
; 


---medicaid
--encounter

vacuum analyze data_warehouse.dim_uth_member_id;
vacuum analyze data_warehouse.dim_uth_claim_id;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.derv_enc, a.mem_id, b.uth_member_id, a.year_fy 
from medicaid.enc_proc a   
   join data_warehouse.dim_uth_member_id b 
     on b.member_id_src = a.mem_id    
    and b.data_source = 'mdcd'
   left outer join data_warehouse.dim_uth_claim_id c
     on c.member_id_src = a.mem_id
    and c.claim_id_src = a.derv_enc 
where c.uth_claim_id is null 
;

---medicaid
--htw

vacuum analyze data_warehouse.dim_uth_member_id;
vacuum analyze data_warehouse.dim_uth_claim_id;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.icn, a.pcn, b.uth_member_id, a.year_fy 
from medicaid.htw_clm_proc a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = a.pcn  
   and b.data_source = 'mdcd'
  left outer join data_warehouse.dim_uth_claim_id c
    on c.member_id_src = a.pcn 
   and c.claim_id_src = a.icn
 where c.uth_member_id is null 
; 

vacuum analyze data_warehouse.dim_uth_claim_id;

