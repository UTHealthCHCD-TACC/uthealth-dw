/* ******************************************************************************************************
 *  This script creates dim_uth_member_id values for people found in claims tables that have no 
 *  cooresponding enrollment record for that month+year 
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 3/09/2022 || script created 
 * ******************************************************************************************************
 **/


do $$

begin
	
raise notice 'claim created optd begin';

--Optum DoD claim created  
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


raise notice 'claim created optz begin';

--Optum zip claim created 
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


raise notice 'claim created begin truv';

---Truven Commercial claim created  
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

raise notice 'claim created begin truv mdcr';

---Truven Medicare claim created  
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

raise notice 'claim created begin medicare texas';

---- ***** Medicare Texas***** 
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient, and snf tables
insert into data_warehouse.dim_uth_member_id  (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as ( 
	select distinct bene_id as v_member_id, 'mcrt' as v_data_source
	from ( 
		    select bene_id
		    from medicare_texas.bcarrier_claims_k 		    
		union 
			select bene_id
			from medicare_texas.dme_claims_k 
		union 	   
			select bene_id
			from medicare_texas.hha_base_claims_k 
		union 
			select bene_id
			from medicare_texas.hospice_base_claims_k 
		union 
			select bene_id
			from medicare_texas.inpatient_base_claims_k
		union 
			select bene_id
			from medicare_texas.outpatient_base_claims_k
		union 
			select bene_id
			from medicare_texas.snf_base_claims_k 
		 ) raw_clms
   left outer join data_warehouse.dim_uth_member_id b 
   		 on b.member_id_src = raw_clms.bene_id 
  		and b.data_source = 'mcrt'
	where b.member_id_src is null 
 )
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member ;


raise notice 'claim created begin medicare national';
---- ***** Medicare National***** 
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient, and snf tables
insert into data_warehouse.dim_uth_member_id  (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as ( 
	select distinct bene_id as v_member_id, 'mcrn' as v_data_source
	from ( 
		    select bene_id
		    from medicare_national.bcarrier_claims_k 		    
		union 
			select bene_id
			from medicare_national.dme_claims_k 
		union 	   
			select bene_id
			from medicare_national.hha_base_claims_k 
		union 
			select bene_id
			from medicare_national.hospice_base_claims_k 
		union 
			select bene_id
			from medicare_national.inpatient_base_claims_k
		union 
			select bene_id
			from medicare_national.outpatient_base_claims_k
		union 
			select bene_id
			from medicare_national.snf_base_claims_k 
		 ) raw_clms
   left outer join data_warehouse.dim_uth_member_id b 
   		 on b.member_id_src = raw_clms.bene_id 
  		and b.data_source = 'mcrn'
	where b.member_id_src is null 
 )
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member ;


raise notice 'claim created begin medicaid';
------------ ***** Medicaid ***** 
--claims
insert into data_warehouse.dim_uth_member_id  (member_id_src, data_source, uth_member_id, claim_created_id)

with cte_distinct_member as ( 
	select distinct pcn as v_member_id, 'mdcd' as v_data_source
	from ( 
			select a.pcn
			from medicaid.clm_proc a
		union 
			select trim(a.mem_id) as pcn
			from medicaid.enc_proc a   
		 ) raw_clms
   left outer join data_warehouse.dim_uth_member_id b 
   		 on b.member_id_src = raw_clms.pcn 
  		and b.data_source = 'mdcd'
	where b.member_id_src is null 
 )
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member ;
			

raise notice 'analyze dim_uth_member_id';

analyze data_warehouse.dim_uth_member_id;


raise notice 'end script';

end $$
;

