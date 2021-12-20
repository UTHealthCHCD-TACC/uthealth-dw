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
 *  jw001  || 11/12/2021 || wrap in function
 * ******************************************************************************************************
 *  wc004  || 12/12/2021 || remove claim created logic, consolidate medicaid, remove old record cleanup logic 
 * ****************************************************************************************************** 
 **/

------ load dim_uth_member_id------------------------------------------------

CREATE OR REPLACE FUNCTION dw_staging.load_dim_uth_member_id()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

begin
	
------------ /  BEGIN SCRIPT
	
raise notice 'begin script';

-- ***** Optum DoD ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct a.member_id_src as v_member_id, 'optd' as v_data_source
	from optum_dod.mbr_enroll_r a 
	 left outer join data_warehouse.dim_uth_member_id b 
              on b.data_source = 'optd'
             and b.member_id_src = a.member_id_src 
	where b.uth_member_id is null 
)
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

raise notice 'load optd finished';


--- ***** Optum Zip  ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct a.member_id_src as v_member_id, 'optz' as v_data_source
	from optum_zip.mbr_enroll a
	 left outer join data_warehouse.dim_uth_member_id b 
              on b.data_source = 'optz'
             and b.member_id_src = a.member_id_src 
	where b.uth_member_id is null 
)
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;


raise notice 'load optz finished';


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

raise notice 'load truven finished';
raise notice 'load mcrt begin';


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

raise notice 'load mcrt finished';
raise notice 'load mcrn begin';

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

raise notice 'load mcrn finished';

--- ***** Medicaid ***** 

raise notice 'load mdcd begin';

---medicaid enrl
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mdcd' as v_raw_data 
   from ( 
   			select client_nbr 
	   		from medicaid.enrl  
	   	union
	   		select client_nbr
   			from medicaid.chip_uth
         ) inr 
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcd' 
     and member_id_src = client_nbr 
    where member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;


raise notice 'load mdcd finished';

analyze data_warehouse.dim_uth_member_id;

raise notice 'load uth_member_id function complete';

end $$
;

--run when recreating function 
alter function dw_staging.load_dim_uth_member_id() owner to uthealth_dev;
grant all on function dw_staging.load_dim_uth_member_id() to uthealth_dev;


/*
---*******************************************************
--// wcc002 - claim created ids 
---*******************************************************

raise notice 'claim created begin';
raise notice 'claim created optd begin';

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

raise notice 'claim created optd finished';
raise notice 'claim created optz begin';

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

raise notice 'claim created optz finished';
raise notice 'claim created begin truv';

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
raise notice 'claim created truv finished';
raise notice 'claim created begin truv mdcr';

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

raise notice 'claim created truv mdcr finished';
raise notice 'claim created finished';
raise notice 'analyze dim_uth_member_id';

analyze data_warehouse.dim_uth_member_id;

alter function dw_staging.load_dim_uth_member_id() owner to uthealth_dev;
grant all on function dw_staging.load_dim_uth_member_id() to uthealth_dev;

raise notice 'ownership transferred to uthealth_dev';
raise notice 'end script';

end $$
;
*/

