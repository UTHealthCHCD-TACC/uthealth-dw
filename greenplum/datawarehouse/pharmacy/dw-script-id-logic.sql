select * from optum_zip.mbr_enroll

select uth_rx_claim_id, uth_member_id, ndc, fill_date,
       right(ndc,5) || month_year_id::text || lpad(extract(day from fill_date)::text,2,'0') || right(uth_member_id::text,5) as script_id
   into dev.wc_script_first_id
from data_warehouse.pharmacy_claims
where refill_count = 0

where refill_count is null --58,021,586



select uth_rx_claim_id, uth_member_id, ndc, fill_date, refill_count
  into dev.wc_script_secondary_fills
from data_warehouse.pharmacy_claims a
where refill_count <> 0 
  and exists (  select 1 
  				from data_warehouse.pharmacy_claims b 
  				where b.ndc = a.ndc 
  				  and b.uth_member_id = a.uth_member_id 
  				  and b.refill_count = 0 
  				  and b.fill_date < a.fill_date)

  				  
  				  
select a.*, b.script_id, b.fill_date as script_date 
into dev.wc_script_secondary_multiples
from dev.wc_script_secondary_fills a 
  join dev.wc_script_first_id b 
    on b.uth_member_id = a.uth_member_id 
   and b.ndc = a.ndc 
   and b.fill_date < a.fill_date 
  				  
  				  
  				  

select * from dev.wc_script_first_id



select * 
into dev.wc_script_temp_trvm
from data_warehouse.pharmacy_claims
where data_source = 'trvm'


vacuum analyze dev.wc_script_temp_trvm

update dev.wc_script_temp_trvm set script_id = null; 

select uth_rx_claim_id 
into dev.wc_script_quarantine_trvm
from ( 
select count(*), uth_rx_claim_id 
from dev.wc_script_temp_trvm
group by uth_rx_claim_id 
having count(*) > 1
) a


delete from dev.wc_script_temp_trvm
where uth_rx_claim_id in ( select uth_rx_claim_id from dev.wc_script_quarantine_trvm)

select *
from dev.wc_script_temp_trvm


select count(*), count(distinct uth_rx_claim_id) from dev.wc_script_temp_trvm;



update dev.wc_script_temp_trvm a set script_id = b.script_id 
from dev.wc_script_first_id b 
  where b.uth_member_id = a.uth_member_id
    and b.uth_rx_claim_id = a.uth_rx_claim_id
    and a.refill_count = 0
 ;


select count(*), count(distinct uth_rx_claim_id), data_source 
from data_warehouse.pharmacy_claims 
group by data_source;




update dev.wc_script_temp_trvm a set script_id = b.script_id 
from dev.wc_script_temp_trvm b 
 where b.ndc = a.ndc 
   and b.uth_member_id = b.uth_member_id
   and b.refill_count = 0 
   and a.refill_count <> 0 
   and b.fill_date = ( select max(c.fill_date) 
   					   from dev.wc_script_temp_trvm c 	
   					   where c.uth_member_id = a.uth_member_id
   					     and c.ndc = a.ndc 
   					     and c.fill_date < a.fill_date
   					     and c.refill_count = 0 ) 
   
   
   					     
   select * 
   from dev.wc_script_temp_trvm a 
   where ndc = '00074372790'
   and uth_member_id = 220346940
   order by uth_member_id, fill_date
   
   




select  row_number () over ( partition by uth_member_id, ndc, refill_count 
                               order by refill_count
                              ) as rownum,
         fill_date, ndc, refill_count 
from dev.wc_script_temp
--where ndc = '61314064175'
order by fill_date, refill_count




      row_number() over ( partition by uth_claim_id
                           order by claim_sequence_number_src, from_date_of_service
                          ) as rownum 