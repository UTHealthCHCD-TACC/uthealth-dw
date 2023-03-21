
---truven commercial
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truv'
      ,a.year 
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.enrolid::text || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.enrolid::text
from truven.ccaed  a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src =  a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'truv'
 and c.member_id_src = a.enrolid::text
 and c.rx_claim_id_src = a.enrolid::text || ndcnum::text || svcdate::text
where c.uth_rx_claim_id is null 
  and  a.enrolid::text is not null 
;


--truven medicare
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truv'
      ,a.year 
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.enrolid::text || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.enrolid::text
from truven.mdcrd a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src =  a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'truv'
 and c.member_id_src = a.enrolid::text
 and c.rx_claim_id_src = a.enrolid::text || ndcnum::text || svcdate::text
where c.uth_rx_claim_id is null 
  and  a.enrolid::text is not null 
;

--Run date 03-20-23 XRZ after truven_pay was loaded
--Updated Rows 52087965



