/* 
******************************************************************************************************
 *  This script creates the uth_script_id column in pharmacy_claims.  Basic logic is
 *
 * 1. create uth_script_id for all records with refill_count=0 or refill_count is null
 * 2. match those records to all other records on uth_member_id and ndc where refill_count > 0 and fill_date within 180 days after fill_date of records from step 1
 * 3. create uth_script_id for any records that dont yet have a uth_script_id, taking the min refill_count as the starting record
 * 4. Now match records still lacking uth_script_id with those from step 3 with same logic as step 2
 * 
 * for dev, we take a random sample of data to speed things up for testing.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 *  wallingTACC  ||9/7/2021 || Fixed bug with refill_count=null records.  Ignoring first_fill as it is unreliable
 * ******************************************************************************************************
 * */

--Main Code

--Work on subset of data
drop table data_warehouse.pharmacy_claims_truv;
create table data_warehouse.pharmacy_claims_truv(like data_warehouse.pharmacy_claims)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--truncate data_warehouse.pharmacy_claims_truv;

--Generate random selection
drop table data_warehouse.pharmacy_claims_uth_member_ids;
create table data_warehouse.pharmacy_claims_uth_member_ids (uth_member_id int8)
with(appendonly=true, orientation=column)
distributed by (uth_member_id);

--explain
insert into data_warehouse.pharmacy_claims_uth_member_ids
select * from 
  (select distinct uth_member_id from data_warehouse.pharmacy_claims where data_source='truv') table_alias
ORDER BY random()
limit 10000; --10k random members

-- Insert random records
insert into data_warehouse.pharmacy_claims_truv
select p.*
from data_warehouse.pharmacy_claims p
join data_warehouse.pharmacy_claims_uth_member_ids m on p.uth_member_id=m.uth_member_id
where p.data_source='truv';

--drop table data_warehouse.pharmacy_claims;
alter table data_warehouse.pharmacy_claims rename to temp_script_id_truv;
alter table data_warehouse.pharmacy_claims_truv rename to temp_script_id;

--- NON-DEV/PROD Start Here !!!!!!

update data_warehouse.pharmacy_claims
set uth_script_id = null;

-- Create uth_script_ids
--alter table data_warehouse.pharmacy_claims drop column uth_script_id;
--alter table data_warehouse.pharmacy_claims add column uth_script_id int8;
drop sequence data_warehouse.pharmacy_claims_uth_script_id_seq;
create sequence data_warehouse.pharmacy_claims_uth_script_id_seq;
alter sequence data_warehouse.pharmacy_claims_uth_script_id_seq cache 500;

--drop table dev.uth_script_ids;
create table dev.uth_script_ids
with(appendonly=true, orientation=column)
as
select nextval('data_warehouse.pharmacy_claims_uth_script_id_seq') as uth_script_id, a.*
from (select distinct uth_member_id, ndc, script_id, year, fill_date, refill_count
from data_warehouse.pharmacy_claims
where refill_count=0 or refill_count is null-- or first_fill='Y'
) a
distributed by (uth_member_id);

-- Verify
select count(distinct uth_script_id) from dev.uth_script_ids;

--Verify = no rows
select * from (
select uth_member_id, ndc, script_id, fill_date, refill_count, count(*)
from dev.uth_script_ids
group by uth_member_id, ndc, script_id, fill_date, refill_count
having count(*)>1
) a
order by 2,3,4,5;

--Update the null and min refill_count records

/*
select *
from data_warehouse.pharmacy_claims a
join dev.uth_script_ids u
on a.uth_member_id=u.uth_member_id 
and a.ndc=u.ndc 
and ((a.script_id is null and u.script_id is null) or (a.script_id=u.script_id)) 
and a.fill_date=u.fill_date and a.refill_count is not distinct from u.refill_count
where a.uth_member_id=594282329 and a.ndc='59762374401';
*/

update data_warehouse.pharmacy_claims as a 
set uth_script_id=u.uth_script_id
from dev.uth_script_ids u
where a.uth_member_id=u.uth_member_id and a.ndc=u.ndc 
and ((a.script_id is null and u.script_id is null) or (a.script_id=u.script_id)) 
and a.fill_date=u.fill_date and a.refill_count is not distinct from u.refill_count;

--Get just the first/min records
--drop table dev.pharmacy_claims_0;
create table dev.pharmacy_claims_0
with(appendonly=true,orientation=column)
as
select distinct uth_script_id, uth_member_id, ndc, script_id, fill_date, refill_count
from data_warehouse.pharmacy_claims a1
where uth_script_id is not null
and refill_count is not null
distributed by (uth_member_id);

vacuum analyze data_warehouse.pharmacy_claims;
vacuum analyze dev.pharmacy_claims_0;

--Verify = no rows
/*
select uth_member_id, ndc, script_id, fill_date, refill_count, count(distinct uth_script_id) 
from data_warehouse.pharmacy_claims_0 tsi 
group by 1, 2, 3, 4, 5
having count(*) > 1;
*/

-- Update refill_count>0 to match above
update data_warehouse.pharmacy_claims b set uth_script_id=a.uth_script_id
from dev.pharmacy_claims_0 as a
where b.refill_count is not null
and a.uth_member_id=b.uth_member_id and a.ndc=b.ndc 
and ((a.script_id is null and b.script_id is null) or (a.script_id=b.script_id)) 
and b.refill_count>0 
and a.fill_date = 
(select max(c.fill_date) 
from  data_warehouse.pharmacy_claims_0 c 
where c.uth_script_id is not null
and c.uth_member_id = a.uth_member_id and c.ndc = a.ndc 
and ((c.script_id is null and a.script_id is null) or (c.script_id=a.script_id)) 
and c.fill_date <= b.fill_date 
and c.refill_count <= b.refill_count
and EXTRACT(DAY FROM age(c.fill_date, b.fill_date)) < 180
);

/*
explain 
select b.uth_member_id, b.ndc, b.fill_date
from data_warehouse.pharmacy_claims b
join data_warehouse.pharmacy_claims_0 as a
on a.uth_member_id=b.uth_member_id and a.ndc=b.ndc and b.refill_count>0
and ((a.script_id is null and b.script_id is null) or (a.script_id=b.script_id))
and a.fill_date = (select max(c.fill_date) from  data_warehouse.pharmacy_claims_0 c
					where c.uth_member_id = a.uth_member_id
					                         and c.ndc = a.ndc 
					                         and ((c.script_id is null and a.script_id is null) or (c.script_id=a.script_id))
					                         and c.fill_date < b.fill_date
					                        and c.refill_count < b.refill_count
											and EXTRACT(DAY FROM AGE(c.fill_date, b.fill_date)) < 180
											)	
group by 1, 2, 3											
having count(distinct a.uth_script_id) > 1;
*/

-- Now get those with no-0 refill_count
--drop table dev.uth_script_ids_no_zero;
create table dev.uth_script_ids_no_zero as
select nextval('data_warehouse.pharmacy_claims_uth_script_id_seq') as uth_script_id, a.*
from (select distinct uth_member_id, ndc, script_id, year, min(fill_date) as fill_date, min(refill_count) as refill_count
from data_warehouse.pharmacy_claims
where uth_script_id is null
--where uth_member_id = 537290390 and ndc='00603497528'
group by 1, 2, 3, 4
) a
distributed by (uth_member_id);

update data_warehouse.pharmacy_claims as a 
set uth_script_id=u.uth_script_id
from dev.uth_script_ids_no_zero u
where a.uth_member_id=u.uth_member_id and a.ndc=u.ndc 
and ((a.script_id is null and u.script_id is null) or (a.script_id=u.script_id)) 
and a.fill_date=u.fill_date and a.refill_count=u.refill_count
and a.uth_script_id is null;

update data_warehouse.pharmacy_claims b set uth_script_id=a.uth_script_id
from dev.uth_script_ids_no_zero as a
where b.uth_script_id is null 
and a.uth_member_id=b.uth_member_id and a.ndc=b.ndc 
and ((a.script_id is null and b.script_id is null) or (a.script_id=b.script_id)) 
and b.refill_count>0 
and a.fill_date = 
(select max(c.fill_date) 
from dev.uth_script_ids_no_zero c 
where c.uth_script_id is not null
and c.uth_member_id = a.uth_member_id and c.ndc = a.ndc 
and ((c.script_id is null and a.script_id is null) or (c.script_id=a.script_id)) 
and c.fill_date <= b.fill_date 
and c.refill_count <= b.refill_count
and EXTRACT(DAY FROM age(c.fill_date, b.fill_date)) < 180
);

--Drop temp tables
drop table dev.uth_script_ids;
drop table dev.pharmacy_claims_0;
drop table dev.uth_script_ids_no_zero;
/*
 * SCRATCH
 */

select uth_script_id, count(*)
from data_warehouse.pharmacy_claims
group by 1
order by 2 desc;

select *
from data_warehouse.pharmacy_claims
where uth_script_id=459286366;

select uth_script_id, fill_date, days_supply, refill_count, first_fill, script_id
from data_warehouse.pharmacy_claims
where uth_script_id = 251565
order by fill_date;

select uth_member_id, ndc, first_fill, fill_date, refill_count, days_supply
from data_warehouse.pharmacy_claims tsi 
where uth_script_id = 374681;

select *
from data_warehouse.pharmacy_claims
where uth_member_id = 148626256 and ndc='60505025203' and uth_script_id is null;

/* QA
 * 
 * check for refill_count > 0 but first in the uth_script_id sequence
 * check diff ndc same drug, don't look at drug_id level
 * check for cases where refill spans calendar year
 * Add a derived_refill_count per uth_script_id
*/

--********************************************************************************
--*********************************************************************************
--((((((((((((((((((((((((((((((((((()))))))))))))))))))))))))))))))))))))))))))))))
					                        
--Scratch

-- NULL uth_script_id					                        
select data_source, count(*)
from data_warehouse.pharmacy_claims where uth_script_id is null
group by 1
order by 1;

select *
from data_warehouse.pharmacy_claims
where uth_script_id is null
and ndc='00603497528'
and uth_member_id = 537290390
order by fill_date;


select *
from data_warehouse.pharmacy_claims
where uth_script_id = '101075088';


SELECT uth_member_id, ndc, fill_date, refill_count, rank() OVER (PARTITION BY uth_member_id, ndc ORDER BY fill_date asc) 
FROM data_warehouse.pharmacy_claims
where ndc='00603497528'
and uth_member_id = 537290390;

select a.uth_script_id, max(age(a.fill_date, b.fill_date)) as max_age
from data_warehouse.pharmacy_claims a
join data_warehouse.pharmacy_claims b on a.uth_script_id = b.uth_script_id 
where a.uth_script_id is not null
group by 1
order by 2 desc;

select *
from data_warehouse.pharmacy_claims
where uth_script_id = 2699977596;

----------------------------------------------------------

drop table data_warehouse.pharmacy_claims_matches;
create table data_warehouse.pharmacy_claims_matches
as
select a.uth_member_id, a.rx_claim_id_src as a_rx_claim_id_src, a.refill_count as a_refill_count, b.rx_claim_id_src as b_rx_claim_id_src, b.refill_count as b_refill_count, a.uth_script_id
from  data_warehouse.pharmacy_claims b
join data_warehouse.pharmacy_claims a
on a.uth_member_id=b.uth_member_id and a.ndc=b.ndc and b.refill_count>0
and ((a.script_id is null and b.script_id is null) or (a.script_id=b.script_id))
and a.fill_date = (select max(c.fill_date) from  data_warehouse.pharmacy_claims c
					where c.uth_member_id = a.uth_member_id
					                         and c.ndc = a.ndc 
					                         and ((c.script_id is null and a.script_id is null) or (c.script_id=a.script_id))
					                         and c.fill_date < b.fill_date );
--Verify = 0
select count(*) from (
select uth_member_id, a_rx_claim_id_src, a_refill_count, b_rx_claim_id_src, b_refill_count, uth_script_id, count() as cnt
from data_warehouse.pharmacy_claims_matches
group by 1, 2, 3, 4, 5, 6
having count(*)>1
order by cnt desc) as a;

select count(*)
from dev.uth_script_ids;

select *
from data_warehouse.pharmacy_claims
where uth_script_id = 153414;

select *
from data_warehouse.pharmacy_claims pc 
where rx_claim_id_src = '1584244202690970158072016-09-29'

select *
from truven.ccaed
where enrolid = '1584244202'
and svcdate = '2016-09-29';

select distinct data_source, first_fill
from data_warehouse.pharmacy_claims;

--Verify
select uth_member_id, count(*)
from data_warehouse.pharmacy_claims
group by 1
order by 2 desc 
limit 3;

select uth_script_id, ndc, fill_date, refill_count, script_id
from data_warehouse.pharmacy_claims
where uth_member_id = 170265538
order by ndc, fill_date, refill_count;

create table dev.uth_script_id_diff_ndc_same_drug
as
select uth_member_id, ndc, script_id, fill_date, refill_count, uth_script_id, generic_name, brand_name
from data_warehouse.pharmacy_claims
where script_id='J99JV33FO';

select *
from data_warehouse.pharmacy_claims
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
from data_warehouse.pharmacy_claims
where uth_script_id is null
order by uth_member_id, ndc;

create table dev.diff_clmid_rx_same_script_id_truv
as
select data_source, uth_member_id, uth_rx_claim_id, rx_claim_id_src, script_id, uth_script_id, ndc, fill_date, refill_count
from data_warehouse.pharmacy_claims
where uth_script_id=234637
order by fill_date, refill_count;

select *
from dev.diff_clmid_rx_same_script_id_truv
order by fill_date;

select *
from data_warehouse.pharmacy_claims
where script_id='JJFOVJLLF';

where 
where script_id = 'JOJVRROVN';
where member_id_src = '560499874406181' --'30647131404';

select count(*)
from data_warehouse.pharmacy_claims
where uth_script_id is null;

create table data_warehouse.pharmacy_claims_null
with(appendonly=true,orientation=column) as
select *
from data_warehouse.pharmacy_claims
where uth_script_id is null
distributed by (member_id_src);


select *
from data_warehouse.pharmacy_claims_null
where member_id_src = '1116678402'
order by rx_claim_id_src;

select *
from data_warehouse.pharmacy_claims
where member_id_src = '1116678402' and ndc='00228253950'
order by rx_claim_id_src;

select refill_count, select count(*)
from data_warehouse.pharmacy_claims_null
order by 1;

select days_supply, count(*)
from data_warehouse.pharmacy_claims
group by 1
order by 1;

create table data_warehouse.pharmacy_claims_null_refill_count
with(appendonly=true,orientation=column) 
as
select *
from data_warehouse.pharmacy_claims
where refill_count is null;

select *
from data_warehouse.pharmacy_claims_null_refill_count
where uth_member_id = '534171766'
order by uth_member_id, ndc, fill_date;

select data_source, refill_count, count(*)
from data_warehouse.pharmacy_claims
group by 1, 2
order by 1, 2;

select count(*)
from data_warehouse.pharmacy_claims_dupes;

select count(*)
from data_warehouse.pharmacy_claims_matches;

select *
from data_warehouse.pharmacy_claims tsi 
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
drop table data_warehouse.pharmacy_claims_matches; 					                         
create table data_warehouse.pharmacy_claims_matches
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
from data_warehouse.pharmacy_claims b
join data_warehouse.pharmacy_claims a
on a.uth_member_id=b.uth_member_id and a.ndc=b.ndc and a.refill_count=0 and b.refill_count>0
and a.fill_date = (select max(c.fill_date) from  data_warehouse.pharmacy_claims c
					where c.uth_member_id = a.uth_member_id
					                         and c.ndc = a.ndc 
					                         and c.refill_count = 0 
					                         and c.fill_date < b.fill_date ) 
where b.uth_rx_claim_id = 7871096076;					                         
group by 1
having count(*) > 1;

					                         
select data_source, uth_member_id, member_id_src, uth_script_id, script_id, ndc, refill_count, fill_date
from data_warehouse.pharmacy_claims
order by data_source, uth_script_id, ndc, fill_date, refill_count;

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

insert into data_warehouse.pharmacy_claims
select *
from data_warehouse.pharmacy_claims
where data_source='truv';

select distinct data_source from data_warehouse.pharmacy_claims;

select count(*) from data_warehouse.pharmacy_claims;

select ndc, fill_date, refill_count from data_warehouse.pharmacy_claims order by fill_date;

select distinct on (a.ndc, a.fill_date) a.ndc, a.fill_date, a.refill_count, b.ndc, b.fill_date, b.refill_count 
from data_warehouse.pharmacy_claims a
left outer join data_warehouse.pharmacy_claims b on a.uth_member_id = b.uth_member_id and a.ndc = b.ndc and a.fill_date <= b.fill_date and a.refill_count = 0 and b.refill_count > 0
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


