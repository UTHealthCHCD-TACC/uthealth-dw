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
 * 
 * ****************************************************************************************************** */




-- ***** Optum dod ***** 
--8/31/2021 runtime 10m16s
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'optd', a.clmid::text, a.patid::text, b.uth_member_id, min(trunc(a.year,0))
from optum_dod.medical a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optd'
   and b.member_id_src = a.patid::text 
  left join data_warehouse.dim_uth_claim_id c
        on  b.data_source = c.data_source
          and a.clmid::text = c.claim_id_src 
          and a.patid::text = c.member_id_src 
where a.patid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4;


---cleanup optd 
--11m59s
delete from data_warehouse.dim_uth_claim_id clm
    using( 
select a.uth_claim_id
from data_warehouse.dim_uth_claim_id a 
   left outer join optum_dod.medical b 
     on a.member_id_src = b.patid::text 
    and a.claim_id_src = b.clmid::text 
where a.data_source = 'optd' 
  and b.clmid is null 
 )  del 
where clm.uth_claim_id = del.uth_claim_id 
;


-- ***** Optum zip ***** 
--8/31/2021 runtime 10m16s
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'optz', a.clmid::text, a.patid::text, b.uth_member_id, min(trunc(a.year,0))
from optum_zip.medical a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optz'
   and b.member_id_src = a.patid::text 
  left join data_warehouse.dim_uth_claim_id c
        on  b.data_source = c.data_source
          and a.clmid::text = c.claim_id_src 
          and a.patid::text = c.member_id_src
where a.patid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4;


---cleanup optz 
--12m50s
delete from data_warehouse.dim_uth_claim_id clm
    using( 
select a.uth_claim_id
from data_warehouse.dim_uth_claim_id a 
   left outer join optum_zip.medical b 
     on a.member_id_src = b.patid::text 
    and a.claim_id_src = b.clmid::text 
where a.data_source = 'optz' 
  and b.clmid is null 
 )  del 
where clm.uth_claim_id = del.uth_claim_id 
;


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

------------ ***** Medicaid ***** 
--claims
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.icn, a.pcn, b.uth_member_id, a.year_fy 
from medicaid.clm_proc a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = a.pcn  
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
   left outer join data_warehouse.dim_uth_claim_id c
     on c.member_id_src = a.mem_id
    and c.claim_id_src = a.derv_enc 
where c.uth_claim_id is null 
;

--------------------------------------------------------------------------------------------------------------------------
vacuum analyze  data_warehouse.dim_uth_claim_id;

---- // END SCRIPT 


/*  Original Table Create
 * --!do not run unless table has to be recreated

drop table if exists data_warehouse.dim_uth_claim_id;

CREATE TABLE data_warehouse.dim_uth_claim_id (
	uth_claim_id bigserial NOT NULL,
	uth_member_id int8 null,
	data_source bpchar(4) NULL,
	claim_id_src text NOT NULL,
	member_id_src text NOT NULL,
	data_year int4 NOT NULL
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (uth_member_id);

alter sequence data_warehouse.dim_uth_claim_id_uth_claim_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_claim_id_uth_claim_id_seq cache 200;


analyze data_warehouse.dim_uth_claim_id;
*/

