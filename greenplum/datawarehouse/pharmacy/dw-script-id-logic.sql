--Main Code

--Work on subset of data
drop table dev.temp_script_id_truv;
create table dev.temp_script_id_truv(like data_warehouse.pharmacy_claims)
with(appendonly=true, orientation=column);

truncate dev.temp_script_id_optz;

--Generate random selection
drop table dev.temp_script_id_uth_member_ids;
create table dev.temp_script_id_uth_member_ids (uth_member_id int8)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--explain
insert into dev.temp_script_id_uth_member_ids
select * from 
  (select distinct uth_member_id from data_warehouse.pharmacy_claims where data_source='truv') table_alias
ORDER BY random()
limit 10000;

-- Insert random records
insert into dev.temp_script_id_truv
select p.*
from data_warehouse.pharmacy_claims p
join dev.temp_script_id_uth_member_ids m on p.uth_member_id=m.uth_member_id
where p.data_source='truv';

alter table dev.temp_script_id rename to temp_script_id_truv;
alter table dev.temp_script_id_truv rename to temp_script_id;

-- Create uth_script_ids
alter table dev.temp_script_id drop column uth_script_id;
alter table dev.temp_script_id add column uth_script_id int8;
drop sequence dev.temp_script_id_uth_script_id_seq;
create sequence dev.temp_script_id_uth_script_id_seq;
alter sequence dev.temp_script_id_uth_script_id_seq cache 500;

drop table dev.uth_script_ids;
create table dev.uth_script_ids
with(appendonly=true, orientation=column)
as
select nextval('dev.temp_script_id_uth_script_id_seq') as uth_script_id, a.*
from (select distinct uth_member_id, ndc, script_id, min(fill_date) as fill_date, min(refill_count) as min_refill_count
from dev.temp_script_id
where (script_id is null and (refill_count=0 or refill_count is null)) or script_id is not null
group by 1, 2, 3
) a
distributed by (uth_member_id);

-- Verify = 0
select count(distinct uth_script_id) from dev.uth_script_ids;

select *
from dev.uth_script_ids
where uth_member_id = 165010113

create table dev.uth_script_id_same_script_id_both_refill_count_zero
as
select *
from dev.temp_script_id
where uth_member_id = 165010113 and script_id='JJRVN83RF';

select * from (
select uth_member_id, ndc, script_id, fill_date, min_refill_count, count(*)
from dev.uth_script_ids
group by uth_member_id, ndc, script_id, fill_date, min_refill_count
having count(*)>1
) a
order by 2,3,4,5;

--Update the null and min refill_count records
update dev.temp_script_id a set uth_script_id=u.uth_script_id
from dev.uth_script_ids u
where a.uth_member_id=u.uth_member_id and a.ndc=u.ndc and a.script_id=u.script_id and a.fill_date=u.fill_date and a.refill_count=u.min_refill_count;

--Verify
select count(distinct uth_script_id) from dev.temp_script_id;

--Get just the refill_count=0 records
drop table dev.temp_script_id_0;
create table dev.temp_script_id_0
with(appendonly=true,orientation=column)
as
select distinct uth_script_id, uth_member_id, ndc, script_id, fill_date
from dev.temp_script_id a1
where uth_script_id is not null
distributed by (uth_member_id);

vacuum analyze dev.temp_script_id;
vacuum analyze dev.temp_script_id_0;

--Verify = 0
select uth_member_id, ndc, script_id, fill_date, count(distinct uth_script_id) 
from dev.temp_script_id_0 tsi 
group by 1, 2, 3, 4
having count(*) > 1;

-- Update refill_count>0 to match above
update dev.temp_script_id b set uth_script_id=a.uth_script_id
from dev.temp_script_id_0 as a
where a.uth_member_id=b.uth_member_id and a.ndc=b.ndc and a.script_id=b.script_id and b.refill_count>0 and a.fill_date = 
(select max(c.fill_date) 
from  dev.temp_script_id_0 c 
where c.uth_member_id = a.uth_member_id and c.ndc = a.ndc and c.script_id=a.script_id and c.fill_date < b.fill_date );
					                        
explain 
select *
from dev.temp_script_id b
join dev.temp_script_id_0 as a
on a.uth_member_id=b.uth_member_id and a.ndc=b.ndc and a.refill_count=0 and b.refill_count>0
and a.fill_date = (select max(c.fill_date) from  dev.temp_script_id_0 c
					where c.uth_member_id = a.uth_member_id
					                         and c.ndc = a.ndc 
					                         and c.fill_date < b.fill_date );

--Scratch
drop table dev.temp_script_id_matches;
create table dev.temp_script_id_matches
as
select a.rx_claim_id_src as a_rx_claim_id_src, a.refill_count as a_refill_count, b.rx_claim_id_src as b_rx_claim_id_src, b.refill_count as b_refill_count, a.uth_script_id
from  dev.temp_script_id b
join dev.temp_script_id a
on a.uth_member_id=b.uth_member_id and a.ndc=b.ndc and a.refill_count=0 and b.refill_count>0
and a.fill_date = (select max(c.fill_date) from  dev.temp_script_id c
					where c.uth_member_id = a.uth_member_id
					                         and c.ndc = a.ndc 
					                         and c.refill_count = 0 
					                         and c.fill_date < b.fill_date );
--Verify = 0
select a_rx_claim_id_src, a_refill_count, b_rx_claim_id_src, b_refill_count, uth_script_id, count()
from dev.temp_script_id_matches
group by 1, 2, 3, 4, 5
having count(*)>1;

--Verify
select uth_member_id, count(*)
from dev.temp_script_id
group by 1
order by 2 desc 
limit 3;

select uth_script_id, ndc, fill_date, refill_count, script_id
from dev.temp_script_id
where uth_member_id = 170265538
order by ndc, fill_date, refill_count;

create table dev.uth_script_id_diff_ndc_same_drug
as
select uth_member_id, ndc, script_id, fill_date, refill_count, uth_script_id, generic_name, brand_name
from dev.temp_script_id
where script_id='J99JV33FO';

select *
from dev.temp_script_id
where uth_member_id = 169998132
and script_id in ()
select *
from reference_tables.ndc_tier_map
where ndc_code in ('93531101', '93531110');

select *
from reference_tables.ref_ndc_product
where ndc in ('00093531101', '00093531110');

select count(*) 
from (select drug_id
from reference_tables.ndc_tier_map
group by drug_id 
having count(distinct ndc_code) > 1) a;

select *
from dev.temp_script_id
where uth_script_id is null
order by uth_member_id, ndc;

create table dev.diff_clmid_rx_same_script_id_optz
as
select data_source, uth_member_id, uth_rx_claim_id, rx_claim_id_src, script_id, uth_script_id, ndc, fill_date, refill_count
from dev.temp_script_id
where uth_script_id=234637
order by fill_date, refill_count;

select *
from dev.diff_clmid_rx_same_script_id_optz
order by fill_date;

select *
from dev.temp_script_id
where script_id='JJFOVJLLF';

where 
where script_id = 'JOJVRROVN';
where member_id_src = '560499874406181' --'30647131404';

select count(*)
from dev.temp_script_id
where uth_script_id is null;

create table dev.temp_script_id_null
with(appendonly=true,orientation=column) as
select *
from dev.temp_script_id
where uth_script_id is null
distributed by (member_id_src);


select *
from dev.temp_script_id_null
where member_id_src = '1116678402'
order by rx_claim_id_src;

select *
from dev.temp_script_id
where member_id_src = '1116678402' and ndc='00228253950'
order by rx_claim_id_src;

select refill_count, select count(*)
from dev.temp_script_id_null
order by 1;

select days_supply, count(*)
from dev.temp_script_id
group by 1
order by 1;

create table dev.temp_script_id_null_refill_count
with(appendonly=true,orientation=column) 
as
select *
from dev.temp_script_id
where refill_count is null;

select *
from dev.temp_script_id_null_refill_count
where uth_member_id = '534171766'
order by uth_member_id, ndc, fill_date;

select data_source, refill_count, count(*)
from data_warehouse.pharmacy_claims
group by 1, 2
order by 1, 2;

select count(*)
from dev.temp_script_id_dupes;

select count(*)
from dev.temp_script_id_matches;

select *
from dev.temp_script_id tsi 
where rx_claim_id_src in ('306471314044300530142012-06-20', '306471314044300530142012-07-20');

select *
from truven.mdcrd
where enrolid = 1116678402 and ndcnum=228253950
order by seqnum;

select *
from truven.mdcrd
where enrolid = 1116678402
order by svcdate;

--Scratch
drop table dev.temp_script_id_matches; 					                         
create table dev.temp_script_id_matches
( a_id text,
  a_refill_count int,
  b_id text,
  b_refill_count int,
  uth_script_id int8
)
with(appendonly=true, orientation=column)
distributed by (uth_script_id);

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

					                         
select data_source, uth_member_id, member_id_src, uth_script_id, script_id, ndc, refill_count, fill_date
from dev.temp_script_id
order by data_source, uth_script_id, ndc, fill_date, refill_count;

/*
* David Code 
* Creates a subset of data for dev
*/




-- Scratch 

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
where data_source='truv';

select distinct data_source from dev.temp_script_id;

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


