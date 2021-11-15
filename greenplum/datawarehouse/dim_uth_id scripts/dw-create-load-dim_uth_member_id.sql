/* ******************************************************************************************************
 *  This script creates dim_uth_member_id values for a given dataset.  
 * It also removes dim_uth_member_id values no longer found in dataset.
 * Run the relevant section for the dataset.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc002  || 8/31/2021 || add claim created ids from medical tables
 * ******************************************************************************************************
 *  wc003  || 11/09/2021 || merge to single script
 * ******************************************************************************************************
 */

------ load dim_uth_member_id------------------------------------------------


------------ /  BEGIN SCRIPT
--- approx runtime 20min 11/09/21

do $$ 
begin

-- ***** Optum DoD ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optd' as v_raw_data
	from optum_dod.mbr_enroll_r 
	 left outer join data_warehouse.dim_uth_member_id b 
	              on b.data_source = 'optd'
	             and b.member_id_src = patid::text
	where b.member_id_src is null 	
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

---cleanup optd records that are no longer in raw data 
delete from data_warehouse.dim_uth_member_id mem 
    using( 
select a.uth_member_id
from data_warehouse.dim_uth_member_id a 
   left outer join optum_dod.mbr_enroll_r b 
     on a.member_id_src = b.patid::text 
where a.data_source = 'optd' 
  and b.patid is null 
 )  del 
where mem.uth_member_id = del.uth_member_id 
  and mem.claim_created_id is false
;


--- ***** Optum Zip  ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optz' as v_raw_data
	from optum_zip.mbr_enroll
	 left outer join data_warehouse.dim_uth_member_id b 
              on b.data_source = 'optz'
             and b.member_id_src = patid::text
	where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;


---cleanup optz records from dim table that are no longer in raw membership table
delete from data_warehouse.dim_uth_member_id mem 
    using( 
select a.uth_member_id
from data_warehouse.dim_uth_member_id a 
   left outer join optum_zip.mbr_enroll b 
     on a.member_id_src = b.patid::text 
where a.data_source = 'optz' 
  and b.patid is null 
 )  del 
where mem.uth_member_id = del.uth_member_id 
  and mem.claim_created_id is false
;



---***** Truven ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'truv' as v_raw_data
           from (
                select enrolid from truven.ccaet 
                  union 
                select enrolid from truven.mdcrt 
                ) inr 
	 left outer join data_warehouse.dim_uth_member_id b 
          on b.data_source = 'truv'
         and b.member_id_src = enrolid::text
	where b.member_id_src is null 	
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

---cleanup truven 
delete from data_warehouse.dim_uth_member_id mem 
    using(     
  with cte_truv as (   
       select distinct enrolid::text as mem_id 
             from ( select enrolid from truven.ccaet 
                      union 
                    select enrolid from truven.mdcrt 
                  ) inr 
               ) 
select a.uth_member_id
from data_warehouse.dim_uth_member_id a 
   left outer join cte_truv b 
     on a.member_id_src = b.mem_id  
where a.data_source = 'truv' 
  and b.mem_id is null 
 )  del 
where mem.uth_member_id = del.uth_member_id 
;



--- ***** Medicare Texas ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mcrt' as v_raw_data
	from medicare_texas.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'mcrt'
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;



--- ***** Medicare National ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mcrn' as v_raw_data
	from medicare_national.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source in ('mcrn','mcrt')
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--- ***** Medicaid ***** 

---medicaid enrl
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mdcd' as v_raw_data 
   from medicaid.enrl  
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcd' 
     and member_id_src = client_nbr 
    where member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


--medicaid chip
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mdcd' as v_raw_data 
   from medicaid.chip_uth
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcd' 
     and member_id_src = client_nbr 
    where member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

------   



---*******************************************************
--// wcc002 - claim created ids 
---*******************************************************


--Optum DoD claim created wcc002 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as (
	select distinct patid as v_member_id, 'optd' as v_raw_data
	from optum_dod.medical
	 left outer join data_warehouse.dim_uth_member_id b 
	              on b.data_source = 'optd'
	             and b.member_id_src = patid::text
	where b.member_id_src is null 		
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member 
;



--Optum zip claim created wcc002
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member_optz as (
	select distinct patid as v_member_id, 'optz' as v_raw_data
	from optum_zip.medical
	 left outer join data_warehouse.dim_uth_member_id b 
	              on b.data_source = 'optz'
	             and b.member_id_src = patid::text
	where b.member_id_src is null 		
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member_optz
;



---Truven Commercial claim created wcc002 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'truv' as v_raw_data
	           from (
                select enrolid from truven.ccaeo 
                  union 
                select enrolid from truven.ccaes
                ) inr 
	 left outer join data_warehouse.dim_uth_member_id b 
          on b.data_source = 'truv'
         and b.member_id_src = enrolid::text
	where b.member_id_src is null 	
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member 
;



---Truven Medicare claim created wcc002 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'truv' as v_raw_data
	           from (
                select enrolid from truven.mdcro 
                  union 
                select enrolid from truven.mdcrs
                ) inr 
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'truv'
     and b.member_id_src = enrolid::text
where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member 
;

end $$
;


