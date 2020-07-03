 ---update claim sequence
select uth_claim_id,
       uth_member_id,
       claim_sequence_number_src,
       from_date_of_service,
       row_number() over ( partition by uth_claim_id
                           order by claim_sequence_number_src, from_date_of_service
                          ) as rownum 
     into dev.wc_trvc_sequence_logic
       from data_warehouse.claim_detail
       where data_source in ('trvc')	      
       		     
	
vacuum analyze dev.wc_trvc_sequence_logic;
		       
		       
update data_warehouse.claim_detail a set claim_sequence_number = rownum       
from dev.wc_trvc_sequence_logic b 
where a.uth_member_id = b.uth_member_id 
  and a.uth_claim_id = b.uth_claim_id
  and a.claim_sequence_number_src = b.claim_sequence_number_src
  and a.from_date_of_service = b.from_date_of_service;
;
		        

vacuum analyze data_warehouse.claim_detail;

select * from data_warehouse.claim_detail where data_source = 'trvc';


---trvm 
update data_warehouse.claim_detail a set claim_sequence_number = rownum 
        from (    
        select uth_claim_id,
               uth_member_id,
               claim_sequence_number_src,
               from_date_of_service,
               row_number() over ( partition by uth_claim_id
                                   order by claim_sequence_number_src, from_date_of_service
                                  ) as rownum 
		       from data_warehouse.claim_detail
		       where data_source in ('trvm')	      
) b		       
where a.uth_member_id = b.uth_member_id 
  and a.uth_claim_id = b.uth_claim_id
  and a.claim_sequence_number_src = b.claim_sequence_number_src
  and a.from_date_of_service = b.from_date_of_service;
  
 
select * from data_warehouse.claim_detail where data_source = 'trvm';