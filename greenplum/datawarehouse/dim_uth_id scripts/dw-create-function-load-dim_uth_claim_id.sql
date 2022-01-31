/* ******************************************************************************************************
 *  This table is used to generate a de-identified claim id that will be used to populate claim_detail and claim_header tables
 *	The uth_claim_id column will be a sequence that is initially set to a 100,000,000
 *  This code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 8/16/2021 || script created 
 * ******************************************************************************************************
 *  wallingTACC  || 8/23/2021 || updated comments.
 * ******************************************************************************************************
 *  wc002  || 8/31/2021 || consolidate medicare script
 * ******************************************************************************************************
 *  wc003  || 11/09/2021 || run as one script
 * ****************************************************************************************************** 
 *  jw001  || 11/12/2021 || wrap in function
 * ****************************************************************************************************** 
 *  wc004  || 12/07/2021 || add data source to medicaid
 * *******************************************************************************************************/

---runtime 11/9/21 - 90minutes
select dw_staging.load_dim_uth_claim_id();

select count(*), data_source
from data_warehouse.dim_uth_claim_id 
group by data_source
order by data_source 
;

----

CREATE OR REPLACE FUNCTION dw_staging.load_dim_uth_claim_id()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

begin
	
raise notice 'begin script load_dim_uth_claim_id';

raise notice 'load optd begin';

-- ***** Optum dod ***** 
--8/31/2021 runtime 10m16s
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)          
with cte_distinct_claim as 
    (
		select  distinct 'optd' as v_data_source, a.clmid::text as v_claim_id_src, a.member_id_src as v_member_id_src, 
		                 b.uth_member_id as v_uth_member_id, a."year" as v_data_year
		from optum_dod.medical a
		  join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'optd'
		   and b.member_id_src = a.member_id_src 
     ) 
select v_data_source, v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year 
from cte_distinct_claim cdc
  left outer join data_warehouse.dim_uth_claim_id c
    on c.data_source = v_data_source
   and c.claim_id_src = v_claim_id_src
   and c.member_id_src = v_member_id_src
   and c.data_year = v_data_year 
where c.uth_claim_id is null
;

raise notice 'optd loaded';

-- ***** Optum zip ***** 
--8/31/2021 runtime 10m16s
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
with cte_distinct_claim as 
    (
		select  distinct 'optz' as v_data_source, a.clmid::text as v_claim_id_src, a.member_id_src as v_member_id_src, 
		                 b.uth_member_id as v_uth_member_id, a."year" as v_data_year
		from optum_zip.medical a
		  join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'optz'
		   and b.member_id_src = a.member_id_src 
     ) 
select v_data_source, v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year 
from cte_distinct_claim cdc
  left outer join data_warehouse.dim_uth_claim_id c
    on c.data_source = v_data_source
   and c.claim_id_src = v_claim_id_src
   and c.member_id_src = v_member_id_src
   and c.data_year = v_data_year 
where c.uth_claim_id is null
;


raise notice 'optz loaded';


raise notice 'load truven begin';

--- ***** truven commercial, outpatient  ***** 
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'truv', a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))                                            
from truven.ccaeo a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
         on  b.data_source = c.data_source
        and a.msclmid::text = c.claim_id_src 
        and a.enrolid::text = c.member_id_src 
where a.enrolid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4;


-- ***** truven commercial, inpatient ***** 
insert into data_warehouse.dim_uth_claim_id ( data_source, claim_id_src, member_id_src , uth_member_id, data_year )                                              
select  'truv', a.msclmid::text, a.enrolid::text, b.uth_member_id  , min(trunc(a.year,0))
from truven.ccaes a  
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
	    on  b.data_source = c.data_source
	      and a.msclmid::text = c.claim_id_src 
	      and a.enrolid::text = c.member_id_src
  where a.enrolid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4
;

raise notice 'truven com loaded';
raise notice 'load mcrt begin';

--- ***** truven medicare, outpatient *****  
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'truv', a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))
from truven.mdcro a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
    on  b.data_source = c.data_source
      and a.msclmid::text = c.claim_id_src 
      and a.enrolid::text = c.member_id_src 
where a.enrolid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4
;


--- ***** truven medicare inpatient ***** 
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year )     
select  'truv', a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))
from truven.mdcrs a  
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
    on  b.data_source = c.data_source
      and a.msclmid::text = c.claim_id_src 
      and a.enrolid::text = c.member_id_src
where a.enrolid is not null
and c.uth_claim_id is null
group by 1,2,3,4;

raise notice 'truven mdcr* loaded';


---- ***** Medicare Texas***** 
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient, and snf tables
---wc002
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)
select 'mcrt', clm_id, bene_id, raw_clms.uth_member_id, raw_clms.data_year 
from 
( 
    select clm_id, bene_id, uth_member_id, year::int2 as data_year
    from medicare_texas.bcarrier_claims_k a 
      join data_warehouse.dim_uth_member_id b 
        on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.dme_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 	   
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.hha_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.hospice_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.inpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.outpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.snf_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
 ) raw_clms
   left outer join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = raw_clms.bene_id 
   and c.data_source = 'mcrt'
   and c.claim_id_src = raw_clms.clm_id
where c.uth_claim_id is null 
;

raise notice 'medicare texas loaded';

---- ***** Medicare National***** 
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient, and snf tables
---wc002
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)
select 'mcrn', clm_id, bene_id, raw_clms.uth_member_id, raw_clms.data_year 
from 
( 
    select clm_id, bene_id, uth_member_id, year::int2 as data_year
    from medicare_national.bcarrier_claims_k a 
      join data_warehouse.dim_uth_member_id b 
        on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.dme_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 	   
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.hha_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.hospice_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.inpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.outpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.snf_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
 ) raw_clms
   left outer join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = raw_clms.bene_id 
   and c.data_source = 'mcrn'
   and c.claim_id_src = raw_clms.clm_id
where c.uth_claim_id is null 
;

raise notice 'medicare national loaded';

------------ ***** Medicaid ***** 
--claims
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.icn, a.pcn, b.uth_member_id, a.year_fy 
from medicaid.clm_proc a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = a.pcn  
   and b.data_source = 'mdcd'
  left outer join data_warehouse.dim_uth_claim_id c
    on c.member_id_src = a.pcn 
   and c.claim_id_src = a.icn
where c.uth_member_id is null 
; 


---medicaid
--encounter
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.derv_enc, trim(a.mem_id), b.uth_member_id, a.year_fy 
from medicaid.enc_proc a   
   join data_warehouse.dim_uth_member_id b 
     on b.member_id_src = trim(a.mem_id)    
    and b.data_source = 'mdcd'
   left outer join data_warehouse.dim_uth_claim_id c
     on c.member_id_src = a.mem_id
    and c.claim_id_src = a.derv_enc 
where c.uth_claim_id is null 
;

raise notice 'medicaid loaded';



alter function dw_staging.load_dim_uth_claim_id() owner to uthealth_dev;
grant all on function dw_staging.load_dim_uth_claim_id() to uthealth_dev;
raise notice 'ownership transferred to uthealth_dev';

raise notice 'dim_uth_claim_id updates complete';

end $$
;

--------------------------------------------------------------------------------------------------------------------------




