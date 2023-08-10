/********************************
 * This script populates the dim_uth_claim_id table with new uth_claim_ids if they do not exist for a particular claim
 * 
 * Author 	|| Date		|| Comment
 * ----------------------------------------------------
 * Various	  < 08/2023		Script created
 * ----------------------------------------------------
 * Xiaorui	|| 08/07/23	|| Modified to accomodate mhtw and mcpp
 * 						   Also changed the source table to header join proc tables (previously proc tables only)
 * 						   bc proc tables don't have date of service (need DOS to convert to calendar year)
 * 						   Logic: Instead of trying to figure out what data source everyone is in,
 * 						   first assign everyone to data_source mdcd, then after enrollment tables
 * 						   are done, then assign the data_source based on service date and member enrollment
 */

select 'Medicaid dim_uth_claim_id script begin' as message;


/******************
 * Question: Are there duplicates?
 * 
select member_id_src, claim_id_src, count(*) from data_warehouse.dim_uth_claim_id
where data_source in ('mdcd', 'mcpp', 'mhtw')
group by 1, 2
having count(*) > 1;

Answer: no, apparently? Interesting.
 */

/******************************
 * Grab distinct claim_id/member_ids from clm header, enc header, htw header
 * 
 * Note: hdr_from_dos is pretty clean, no 1900-01-01s in it
 */
select 'Creating temp tables: ' || current_timestamp as message;

--claim ids from clm_header
drop table if exists dw_staging.mcd_clm_header_temp;

create table dw_staging.mcd_clm_header_temp as
select distinct a.icn, b.pcn, 
	extract (year from a.hdr_frm_dos::date) as cy
from medicaid.clm_header a left join medicaid.clm_proc b
on a.icn = b.icn
where b.pcn is not null;

--claim ids from enc_header
drop table if exists dw_staging.mcd_enc_header_temp;

create table dw_staging.mcd_enc_header_temp as
select distinct a.derv_enc as icn, b.mem_id as pcn, 
	extract (year from a.frm_dos::date) as cy
from medicaid.enc_header a left join medicaid.enc_proc b
on a.derv_enc = b.derv_enc
where b.mem_id is not null;

--claim ids from htw_clm_header
drop table if exists dw_staging.mcd_htw_header_temp;

create table dw_staging.mcd_htw_header_temp as
select distinct a.icn, b.pcn, 
	extract (year from a.hdr_frm_dos::date) as cy
from medicaid.htw_clm_header a left join medicaid.htw_clm_proc b
on a.icn = b.icn
where b.pcn is not null;


/****************************
 * Insert into dim_uth_claim_id table
 */

--from clm tables
select 'Add in new claim ids from clm_proc: ' || current_timestamp as message;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year)
select 'mdcd', a.icn, a.pcn, a.cy
from dw_staging.mcd_clm_header_temp a
  left outer join data_warehouse.dim_uth_claim_id c
    on c.member_id_src = a.pcn 
   and c.claim_id_src = a.icn
   and c.data_source in ('mdcd', 'mhtw', 'mcpp')
 where c.uth_member_id is null 
;
vacuum analyze data_warehouse.dim_uth_claim_id;


--from encounter tables
select 'Add in new claim ids from enc_proc: ' || current_timestamp as message;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , data_year)
select 'mdcd', a.icn, a.pcn, a.cy 
from dw_staging.mcd_enc_header_temp a 
   left outer join data_warehouse.dim_uth_claim_id c
     on c.member_id_src = a.pcn
    and c.claim_id_src = a.icn
    and c.data_source in ('mdcd', 'mhtw', 'mcpp')
where c.uth_claim_id is null 
;

vacuum analyze data_warehouse.dim_uth_claim_id;

--from htw tables
select 'Add in new claim ids from htw_proc: ' || current_timestamp as message;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year)
select 'mdcd', a.icn, a.pcn, a.cy
from dw_staging.mcd_htw_header_temp a
  left outer join data_warehouse.dim_uth_claim_id c
    on c.member_id_src = a.pcn 
   and c.claim_id_src = a.icn
   and c.data_source in ('mdcd', 'mhtw', 'mcpp')
 where c.uth_member_id is null 
;

vacuum analyze data_warehouse.dim_uth_claim_id;

/***************
 * HOT FIX 08/07/2022
 * Previously data_year was set to the fiscal year; change it so that it's set to calendar year
 * This code should not need to be run again

--update data year for clms
update data_warehouse.dim_uth_claim_id b
set data_year = a.cy
from dw_staging.mcd_clm_header_temp a
where b.data_year != a.cy
	and a.pcn = b.member_id_src
	and a.icn = b.claim_id_src
	and b.data_source in ('mdcd', 'mhtw', 'mcpp');

--update data year for enc
update data_warehouse.dim_uth_claim_id b
set data_year = a.cy
from dw_staging.mcd_enc_header_temp a
where b.data_year != a.cy
	and a.pcn = b.member_id_src
	and a.icn = b.claim_id_src
	and b.data_source in ('mdcd', 'mhtw', 'mcpp');

--update data year for htw
update data_warehouse.dim_uth_claim_id b
set data_year = a.cy
from dw_staging.mcd_htw_header_temp a
where b.data_year != a.cy
	and a.pcn = b.member_id_src
	and a.icn = b.claim_id_src
	and b.data_source in ('mdcd', 'mhtw', 'mcpp');

 */

/******************
 * Clean-up
 */
select 'Final vacuum and cleanup: ' || current_timestamp as message;
vacuum analyze data_warehouse.dim_uth_claim_id;

drop table if exists dw_staging.mcd_clm_header_temp;
drop table if exists dw_staging.mcd_enc_header_temp;
drop table if exists dw_staging.mcd_htw_header_temp;

select 'Medicaid dim_uth_claim_id script completed at: ' || current_timestamp as message;




