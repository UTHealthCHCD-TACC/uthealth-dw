
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
select * from optum_dod.mbr_enroll

drop table dev.wc_script_first_id 

select uth_rx_claim_id, uth_member_id, ndc, fill_date,
       right(ndc,5) || right(uth_member_id::text,5) || month_year_id::text || lpad(extract(day from fill_date)::text,2,'0') as script_id
   into dev.wc_script_first_id
from data_warehouse.pharmacy_claims
where refill_count = 0


--update initial script_id where refill=0
---15min
update data_warehouse.pharmacy_claims a set script_id = b.script_id 
from dev.wc_script_first_id b 
where a.uth_member_id = b.uth_member_id 
  and a.uth_rx_claim_id = b.uth_rx_claim_id 
;


select * from data_warehouse.pharmacy_claims pc where refill_count = 0 and script_id is null and ndc is not null;

drop table dev.wc_script_first_id 

----update script id for refills beyond initial
 
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
;



update data_warehouse.pharmacy_claims a set script_id = b.script_id 
from data_warehouse.pharmacy_claims b 
where a.uth_member_id in ( 361583100, 130463626, 502108669) 
    and b.uth_member_id = a.uth_member_id 
    and b.ndc = a.ndc 
    and b.refill_count = 0 
    and a.refill_count <> 0 
    and b.fill_date = ( select max(c.fill_date) 
                       from data_warehouse.pharmacy_claims c 
                       where c.uth_member_id = a.uth_member_id 
                         and c.ndc = a.ndc 
                         and c.refill_count = 0 
                         and c.fill_date < a.fill_date 
                         and uth_member_id in (361583100, 130463626, 502108669 ) 
                        )                     
; 
                         
  
---Scratch work

drop table dev.wc_temp_rx


select * 
into dev.wc_temp_rx 
from data_warehouse.pharmacy_claims pc where uth_member_id in (361583100, 130463626, 502108669 ) ;

  
select * 
into dev.wc_temp_rx2
from dev.wc_temp_rx
;


 update dev.wc_temp_rx  a set script_id = b.script_id 
 from dev.wc_temp_rx  b 
 where b.uth_member_id = a.uth_member_id 
    and b.ndc = a.ndc 
    and b.refill_count = 0 
    and a.refill_count <> 0 
    and b.fill_date = ( select max(c.fill_date) 
                       from dev.wc_temp_rx  c 
                       where c.uth_member_id = a.uth_member_id
                         and c.ndc = a.ndc 
                         and c.refill_count = 0 
                         and c.fill_date < a.fill_date ) 
;



select *
from dev.wc_temp_rx
order by uth_member_id, fill_date
;
  
