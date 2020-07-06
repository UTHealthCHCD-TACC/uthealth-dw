 ---update claim sequence
select uth_claim_id,
       uth_member_id,
       claim_sequence_number_src,
       from_date_of_service,
       row_number() over ( partition by uth_claim_id
                           order by claim_sequence_number_src, from_date_of_service
                          ) as rownum 
     into dev.wc_truv_sequence_logic
       from data_warehouse.claim_detail
       where data_source in ('truv')	      
       		     
	
vacuum analyze dev.wc_truv_sequence_logic;
		
create table dev.wc_truv_sequences
with(appendonly=true,orientation=column)
as select *
from dev.wc_trvc_sequence_logic
distributed by(uth_member_id );


drop table dev.wc_truv_sequence_logic ;

		       
update data_warehouse.claim_detail a set claim_sequence_number = rownum       
from dev.wc_truv_sequences b 
where a.uth_member_id = b.uth_member_id 
  and a.uth_claim_id = b.uth_claim_id
  and a.claim_sequence_number_src = b.claim_sequence_number_src
  and a.from_date_of_service = b.from_date_of_service;
;
		        

vacuum analyze data_warehouse.claim_detail;

select * from data_warehouse.claim_detail where data_source = 'truv' and claim_sequence_number is null ;

drop table dev.wc_truv_sequences;

