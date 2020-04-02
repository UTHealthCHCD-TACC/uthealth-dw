/* code to populate dim_uth_claim_id, run dw-create-dim_uth_claim_id.sql to build table first
 * 
 * this code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 */

---truven commercial, outpatient - 31min 2,479,688,920 rows
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'trvc', a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))                                            
from truven.ccaeo a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join dw_qa.dim_uth_claim_id c
         on  b.data_source = c.data_source
        and a.msclmid::text = c.claim_id_src 
        and a.enrolid::text = c.member_id_src 
where a.enrolid is not null
and c.uth_claim_id is NULL
GROUP BY 1, 2, 3, 4;

select count(*) from dw_qa.dim_uth_claim_id;


 
--truven commercial, inpatient 3min 110,482,008
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id, data_year)                                              
select  'trvc', a.msclmid::text, a.enrolid::text, b.uth_member_id  , min(trunc(a.year,0))
from truven.ccaes a  
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join dw_qa.dim_uth_claim_id c
	    on  b.data_source = c.data_source
	      and a.msclmid::text = c.claim_id_src 
	      and a.enrolid::text = c.member_id_src
  where a.enrolid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4;
 
 select count(*) from truven.ccaes --where msclmid is null; 
 



--- truven medicare outpatient 2min, 506,266,398
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'trvm', a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))
from truven.mdcro a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'trvm'
   and b.member_id_src = a.enrolid::text 
  left join dw_qa.dim_uth_claim_id c
    on  b.data_source = c.data_source
      and a.msclmid::text = c.claim_id_src 
      and a.enrolid::text = c.member_id_src 
where a.enrolid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4;

delete from dw_qa.dim_uth_claim_id where data_source = 'trvm';
 
 
---Truven medicare inpatient  2min 50,129,682
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year )     
select  'trvm', a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))
from truven.mdcrs a  
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'trvm'
   and b.member_id_src = a.enrolid::text 
  left join dw_qa.dim_uth_claim_id c
    on  b.data_source = c.data_source
      and a.msclmid::text = c.claim_id_src 
      and a.enrolid::text = c.member_id_src
where a.enrolid is not null
and c.uth_claim_id is null
group by 1,2,3,4;


--Optum Dod 15min
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'optd', a.clmid::text, a.patid::text, b.uth_member_id, min(trunc(a.year,0))
from optum_dod.medical a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'optd'
   and b.member_id_src = a.patid::text 
  left join dw_qa.dim_uth_claim_id c
        on  b.data_source = c.data_source
          and a.clmid::text = c.claim_id_src 
          and a.patid::text = c.member_id_src 
where a.patid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4;


--Optum Zip 20m
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)                                              
select  'optz', a.clmid::text, a.patid::text, b.uth_member_id, min(trunc(a.year,0))
from optum_zip.medical a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'optz'
   and b.member_id_src = a.patid::text 
  left join dw_qa.dim_uth_claim_id c
        on  b.data_source = c.data_source
          and a.clmid::text = c.claim_id_src 
          and a.patid::text = c.member_id_src
where a.patid is not null
and c.uth_claim_id is null
group by 1, 2, 3, 4;

analyze dw_qa.dim_uth_member_id;
analyze dw_qa.dim_uth_claim_id;


---------------------------------------------------------------------------------------------------
---- Medicare :
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient,
---- and snf tables to generate uth_claim_ids.
---------------------------------------------------------------------------------------------------

--bcarrier
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)      
select  'mdcr', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare.bcarrier_claims_k a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join dw_qa.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;

--dme
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mdcr', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare.dme_claims_k a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join dw_qa.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
group by 1, 2, 3, 4;

--hha
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mdcr', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare.hha_base_claims_k a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join dw_qa.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;

--hospice
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mdcr', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare.hospice_base_claims_k a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join dw_qa.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
group by 1, 2, 3, 4;


--inpatient
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mdcr', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare.inpatient_base_claims_k a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join dw_qa.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
group by 1, 2, 3, 4;

--outpatient
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select 'mdcr', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare.outpatient_base_claims_k a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join dw_qa.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;

--snf
insert into dw_qa.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mdcr', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare.snf_base_claims_k a
  join dw_qa.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join dw_qa.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;



-- Scratch

select count(distinct uth_claim_id), count(*), data_source, data_year 
from dw_qa.dim_uth_claim_id
--where data_source = 'trvc'
group by data_source, data_year;


select count(distinct msclmid::text || enrolid::text || year::text ) from truven.ccaeo;


select count(distinct msclmid) from truven.ccaes;


select count(distinct a.clmid ), year from optum_dod.medical a group by year;


select count(uth_member_id), data_source
from dw_qa.dim_uth_member_id
group by data_source;