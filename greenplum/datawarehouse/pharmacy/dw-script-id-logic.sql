/*
 * David Code
 */

drop table dev.temp_script_id;
create table dev.temp_script_id(like data_warehouse.pharmacy_claims)
with(appendonly=true, orientation=column, compresstype=zlib);


insert into dev.temp_script_id
select *
from data_warehouse.pharmacy_claims
where uth_member_id=100000000;


select count(*) from dev.temp_script_id;

select ndc, fill_date, refill_count from dev.temp_script_id order by fill_date;

select distinct on (a.ndc, a.fill_date) a.ndc, a.fill_date, a.refill_count, b.ndc, b.fill_date, b.refill_count 
from dev.temp_script_id a
left outer join dev.temp_script_id b on a.uth_member_id = b.uth_member_id and a.ndc = b.ndc and a.fill_date <= b.fill_date and a.refill_count = 0 and b.refill_count > 0
order by a.ndc, a.fill_date, b.fill_date;

/*
 * Will Code
 */
select * from optum_zip.mbr_enroll

drop table dev.wc_script_first_id 


select uth_rx_claim_id, uth_member_id, ndc, fill_date,
       right(ndc,5) || right(uth_member_id::text,5) || month_year_id::text || lpad(extract(day from fill_date)::text,2,'0') as script_id
   into dev.wc_script_first_id
from data_warehouse.pharmacy_claims
where refill_count = 0



---15min
update data_warehouse.pharmacy_claims a set script_id = b.script_id 
from dev.wc_script_first_id b 
where a.uth_member_id = b.uth_member_id 
  and a.uth_rx_claim_id = b.uth_rx_claim_id 
;


----

drop table dev.wc_temp_rx


  select * 
  into dev.wc_temp_rx 
  from data_warehouse.pharmacy_claims pc where uth_member_id = 358436946 

  
  select * 
  into dev.wc_temp_rx2
  from dev.wc_temp_rx
  
  select ndc, fill_date, refill_count, script_id
  from dev.wc_temp_rx
 order by ndc, fill_date;
  

 
 update data_warehouse.pharmacy_claims a set script_id = b.script_id 
 from data_warehouse.pharmacy_claims b 
 where b.uth_member_id = a.uth_member_id 
    and b.ndc = a.ndc 
    and b.refill_count = 0 
    and a.refill_count <> 0 
    and b.fill_date = ( select max(c.fill_date) 
                       from data_warehouse.pharmacy_claims c 
                       where c.uth_member_id = a.uth_member_id
                         and c.ndc = a.ndc 
                         and c.refill_count = 0 
                         and c.fill_date < a.fill_date ) 
   
 
 
  
  

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