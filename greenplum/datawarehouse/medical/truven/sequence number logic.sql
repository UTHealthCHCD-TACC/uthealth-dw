 ---update claim sequence
 update data_warehouse.claim_detail a set claim_sequence_number = rownum 
        from (                         
        select uth_claim_id,
               claim_sequence_number_src,
               row_number() over ( partition by uth_claim_id
                                   order by claim_sequence_number_src
                                  ) as rownum 
		       from data_warehouse.claim_detail
		       where data_source in ('trvm','trvc')
		       order by uth_claim_id, claim_sequence_number_src 		      
		       		     ) b		       
where a.uth_claim_id = b.uth_claim_id
  and a.claim_sequence_number_src = b.claim_sequence_number_src;

		        
vacuum analyze dw_qa.claim_detail;


select * from dw_qa.claim_detail
		       where data_source in ('trvm','trvc')



		       