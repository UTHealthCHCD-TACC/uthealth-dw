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
 *  wallingTACC  ||11/29/2021 || Functionized the script and set to run in dw_staging
 * ******************************************************************************************************
 * */

--CREATE OR REPLACE FUNCTION dw_staging.reset_uth_script_ids() 	RETURNS void  LANGUAGE plpgsql VOLATILE AS $$
	
do $$ 
begin
	
raise info '%: setting uth_script_id to null ', clock_timestamp();

update dw_staging.pharmacy_claims
set uth_script_id = null;

raise info '%: re-create sequence ', clock_timestamp();


drop sequence dw_staging.pharmacy_claims_uth_script_id_seq;
create sequence dw_staging.pharmacy_claims_uth_script_id_seq;
alter sequence dw_staging.pharmacy_claims_uth_script_id_seq cache 500;

raise info '%: create dev.uth_script_ids ', clock_timestamp();

create table dev.uth_script_ids
with(appendonly=true, orientation=column)
as
select nextval('dw_staging.pharmacy_claims_uth_script_id_seq') as uth_script_id, a.*
from (select distinct uth_member_id, ndc, script_id, year, fill_date, refill_count
from dw_staging.pharmacy_claims
where refill_count=0 or refill_count is null-- or first_fill='Y'
) a
distributed by (uth_member_id);


raise info '%: update the null and min refill_count records ', clock_timestamp();

--Update the null and min refill_count records
update dw_staging.pharmacy_claims as a 
set uth_script_id=u.uth_script_id
from dev.uth_script_ids u
where a.uth_member_id=u.uth_member_id and a.ndc=u.ndc 
and ((a.script_id is null and u.script_id is null) or (a.script_id=u.script_id)) 
and a.fill_date=u.fill_date and a.refill_count is not distinct from u.refill_count;

raise info '%: create dev.pharmacy_claims_0 ', clock_timestamp();

end $$ 
;

--Get just the first/min records
--drop table aadev.pharmacy_claims_0;a
create table dev.pharmacy_claims_0
with(appendonly=true,orientation=column)
as
select distinct uth_script_id, uth_member_id, ndc, script_id, fill_date, refill_count
from dw_staging.pharmacy_claims a1
where uth_script_id is not null
and refill_count is not null
distributed by (uth_member_id);

analyze dw_staging.pharmacy_claims;
analyze dev.pharmacy_claims_0;

do $$ 
begin 
raise info '%: update refill_count>0 to match above ', clock_timestamp();

-- Update refill_count>0 to match above
update dw_staging.pharmacy_claims b set uth_script_id=a.uth_script_id
from dev.pharmacy_claims_0 as a
where a.uth_member_id=b.uth_member_id and a.ndc=b.ndc 
and b.refill_count is not null
and ((a.script_id is null and b.script_id is null) or (a.script_id=b.script_id)) 
and b.refill_count>0 
and a.fill_date = 
(select max(c.fill_date) 
from  dev.pharmacy_claims_0 c 
where c.uth_script_id is not null
and c.uth_member_id = a.uth_member_id and c.ndc = a.ndc 
and ((c.script_id is null and a.script_id is null) or (c.script_id=a.script_id)) 
and c.fill_date <= b.fill_date 
and c.refill_count <= b.refill_count
and EXTRACT(DAY FROM age(c.fill_date, b.fill_date)) < 180
);

raise info '%: create dev.uth_script_ids_no_zero ', clock_timestamp();

-- Now get those with no-0 refill_count
--drop table dev.uth_script_ids_no_zero;
create table dev.uth_script_ids_no_zero as
select nextval('dw_staging.pharmacy_claims_uth_script_id_seq') as uth_script_id, a.*
from (select distinct uth_member_id, ndc, script_id, year, min(fill_date) as fill_date, min(refill_count) as refill_count
from dw_staging.pharmacy_claims
where uth_script_id is null
group by 1, 2, 3, 4
) a
distributed by (uth_member_id);

end $$ 

raise info '%: update dw_staging.pharmacy_claims from dev.uth_script_ids_no_zero ', clock_timestamp();

update dw_staging.pharmacy_claims as a 
set uth_script_id=u.uth_script_id
from dev.uth_script_ids_no_zero u
where a.uth_member_id=u.uth_member_id and a.ndc=u.ndc 
and ((a.script_id is null and u.script_id is null) or (a.script_id=u.script_id)) 
and a.fill_date=u.fill_date and a.refill_count=u.refill_count
and a.uth_script_id is null;

raise info '%: final update with closest within 180 days ', clock_timestamp();

update dw_staging.pharmacy_claims b set uth_script_id=a.uth_script_id
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

raise info '%: drop temp tables ', clock_timestamp();

--Drop temp tables
drop table dev.uth_script_ids;
drop table dev.pharmacy_claims_0;
drop table dev.uth_script_ids_no_zero;

raise info '%: end script ', clock_timestamp();

end $$
--EXECUTE ON ANY
;

analyze dw_staging.pharmacy_claims ;

select count(*) 
from dw_staging.pharmacy_claims
where uth_script_id is null
;

