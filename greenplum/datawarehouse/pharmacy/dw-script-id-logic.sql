select count(), count(distinct member_id_src)
from dev.temp_script_id;

select distinct refill_count from dev.temp_script_id tsi
where refill_count=0;

update dev.temp_script_id set uth_script_id = null;

alter table dev.temp_script_id drop column uth_script_id;

alter table dev.temp_script_id add column uth_script_id int8;
drop sequence dev.temp_script_id_uth_script_id_seq;
create sequence dev.temp_script_id_uth_script_id_seq;

update dev.temp_script_id set uth_script_id=nextval('dev.temp_script_id_uth_script_id_seq')
where uth_script_id is null and refill_count = 0;

update dev.temp_script_id b set uth_script_id=a.uth_script_id
from dev.temp_script_id a

select b.uth_rx_claim_id, count(*)	
select a.*
from dev.temp_script_id b
join dev.temp_script_id a
on a.uth_member_id=b.uth_member_id and a.ndc=b.ndc and a.refill_count=0 and b.refill_count>0
and a.fill_date = (select max(c.fill_date) from  dev.temp_script_id c
					where c.uth_member_id = a.uth_member_id
					                         and c.ndc = a.ndc 
					                         and c.refill_count = 0 
					                         and c.fill_date < b.fill_date ) 
where b.uth_rx_claim_id = 7871096076;					                         
group by 1
having count(*) > 1;

					                         
select uth_member_id, member_id_src, uth_script_id, script_id, ndc, refill_count, fill_date
from dev.temp_script_id
order by uth_script_id, ndc, fill_date, refill_count

/*
 * David Code
 */
select distinct script_id from data_warehouse.pharmacy_claims pc ;

drop table dev.temp_script_id;
create table dev.temp_script_id(like data_warehouse.pharmacy_claims)
with(appendonly=true, orientation=column, compresstype=zlib);

select uth_member_id, count(distinct member_id_src), count(*)
from data_warehouse.pharmacy_claims pc 
group by 1
order by 3 desc;

select uth_member_id, count(distinct member_id_src), count(*)
from data_warehouse.dim_uth_rx_claim_id
group by 1
order by 3 desc;

select uth_member_id, count(*)
from data_warehouse.dim_uth_rx_claim_id
group by 1
order by 2 desc;

insert into dev.temp_script_id
select *
from data_warehouse.pharmacy_claims
where uth_member_id=211359520;


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
 
----12/4 work
drop table dev.wc_pharm_claims;

select * 
into dev.wc_pharm_claims
from data_warehouse.pharmacy_claims 
where data_year between 2016 and 2018 
and data_source in ('truv')
;

vacuum analyze dev.wc_pharm_claims


delete from dev.wc_pharm_claims where ndc = '00000000000'


select uth_member_id , fill_date, script_id , ndc , days_supply , refill_count , fill_date + days_supply as diff,  
       row_number() over(partition by uth_member_id , ndc , (case when refill_count = 0 then 0 else 1 end) order by fill_date ) as rn
from dev.wc_pharm_claims
where uth_member_id = 538861541
order by ndc, fill_date 


---assign script to refill 0
update dev.wc_pharm_claims set script_id = replace(fill_date::text,'-','') || right(uth_member_id::text,4) || right(ndc,4) 
where refill_count = 0 
;



----function
CREATE OR REPLACE FUNCTION public.script_id_truv ( )
RETURNS int AS $FUNC$	 
	declare
	r_uth_member_id numeric; 
	r_uth_rx_claim_id numeric; 
	r_fill_date date;
    r_script_id text; 
    r_ndc text; 
    r_days_supply int; 
    r_refill_count int; 
    r_diff date;     
   	upd_uth_member_id numeric; 
	upd_uth_rx_claim_id numeric;
	counter int := 0;
begin

	while counter < 10 loop 
	
		for r_uth_member_id, r_uth_rx_claim_id, r_fill_date, r_script_id, r_ndc, r_days_supply, r_refill_count, r_diff
		  in 
		select uth_member_id , uth_rx_claim_id , fill_date, script_id , ndc , days_supply , refill_count , fill_date + days_supply as diff
		from dev.wc_pharm_claims
		where uth_member_id = 538861541
		  and script_id is not null 
		
		loop 
	
		    for upd_uth_member_id, upd_uth_rx_claim_id
		       in 
		    select uth_member_id , uth_rx_claim_id 
		    from dev.wc_pharm_claims 
		    where uth_member_id = r_uth_member_id 
		    and ndc = r_ndc 
		    and fill_date > r_fill_date
		    and fill_date <= r_diff 
		    and script_id is null
		    and refill_count > 0 
				    
			    loop 
			    	update dev.wc_pharm_claims set script_id = r_script_id where uth_member_id = upd_uth_member_id and uth_rx_claim_id = upd_uth_rx_claim_id; 	       	
			    end loop;

	end loop;
	raise notice 'Counter %', counter;
	counter := counter+1;
end loop;

	return 0;
end $FUNC$
language 'plpgsql';
 

select public.script_id_truv ();


