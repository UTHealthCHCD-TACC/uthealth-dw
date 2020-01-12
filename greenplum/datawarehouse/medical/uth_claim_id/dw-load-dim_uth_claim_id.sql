/* code to populate dim_uth_claim_id, run dw-create-dim_uth_claim_id.sql to build table first
 * 
 * this code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 */

---truven commercial, outpatient - 31min 2,479,688,920 rows
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)                                              
select distinct  'trvc', a.msclmid::text, a.enrolid::text, trunc(a.year,0), b.uth_member_id                                              
from truven.ccaeo a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
         on  b.data_source = c.data_source
        and a.msclmid::text = c.claim_id_src 
        and a.enrolid::text = c.member_id_src
        and trunc(a.year,0) = c.data_year 
where a.enrolid is not null
and c.uth_claim_id is null
;

select count(*) from data_warehouse.dim_uth_claim_id;


 
--truven commercial, inpatient ??? ~400,000,000
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)                                              
select distinct  'trvc', a.msclmid::text, a.enrolid::text, trunc(a.year,0), b.uth_member_id                                              
from truven.ccaes a  
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
	    on  b.data_source = c.data_source
	      and a.msclmid::text = c.claim_id_src 
	      and a.enrolid::text = c.member_id_src
	      and trunc(a.year,0) = c.data_year 
  where a.enrolid is not null
and c.uth_claim_id is null
;
 
 select count(*) from truven.ccaes --where msclmid is null; 
 



--- truven medicare outpatient 2min, 506,266,398
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)                                              
select distinct  'trvm', a.msclmid::text, a.enrolid::text, trunc(a.year,0), b.uth_member_id                                              
from truven.mdcro a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source in ('trvm','trvc')
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
    on  b.data_source = c.data_source
      and a.msclmid::text = c.claim_id_src 
      and a.enrolid::text = c.member_id_src
      and trunc(a.year,0) = c.data_year 
where a.enrolid is not null
and c.uth_claim_id is null
;


 
 
---Truven medicare inpatient  2min 50,129,682
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)     
select distinct  'trvm', a.msclmid::text, a.enrolid::text, trunc(a.year,0), b.uth_member_id                                              
from truven.mdcrs a  
  join data_warehouse.dim_uth_member_id b 
    on b.data_source in ('trvm','trvc')
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
    on  b.data_source = c.data_source
      and a.msclmid::text = c.claim_id_src 
      and a.enrolid::text = c.member_id_src
      and trunc(a.year,0) = c.data_year 
where a.enrolid is not null
and c.uth_claim_id is null
;


--Optum Dod 15min
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id)                                              
select distinct  'optd', a.clmid::text, a.patid::text, trunc(a.year,0), b.uth_member_id                                              
from optum_dod.medical a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optd'
   and b.member_id_src = a.patid::text 
  left join data_warehouse.dim_uth_claim_id c
        on  b.data_source = c.data_source
          and a.clmid::text = c.claim_id_src 
          and a.patid::text = c.member_id_src
          and trunc(a.year,0) = c.data_year 
where a.patid is not null
and c.uth_claim_id is null
;


--Optum Zip 20m
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id)                                              
select distinct  'optz', a.clmid::text, a.patid::text, trunc(a.year,0), b.uth_member_id                                              
from optum_zip.medical a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optz'
   and b.member_id_src = a.patid::text 
  left join data_warehouse.dim_uth_claim_id c
        on  b.data_source = c.data_source
          and a.clmid::text = c.claim_id_src 
          and a.patid::text = c.member_id_src
          and trunc(a.year,0) = c.data_year 
where a.patid is not null
and c.uth_claim_id is null
;


analyze data_warehouse.dim_uth_claim_id;


---------------------------------------------------------------------------------------------------
---- Medicare :
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient,
---- and snf tables to generate uth_claim_ids.
---------------------------------------------------------------------------------------------------


--bcarrier
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id)      
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.bcarrier_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--dme
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id) 
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.dme_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--hha
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id) 
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.hha_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--hospice
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id) 
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.hospice_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;


--inpatient
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id) 
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.inpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--outpatient
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id) 
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.outpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--snf
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year, uth_member_id) 
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.snf_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;




select count(distinct uth_claim_id), data_source, data_year 
from data_warehouse.dim_uth_claim_id
where data_source = 'optd'
group by data_source, data_year;


select count(distinct msclmid) from truven.ccaeo;


select count(distinct msclmid) from truven.ccaes;


select count(distinct a.clmid ), year from optum_dod.medical a group by year;


select count(uth_member_id), data_source
from data_warehouse.dim_uth_member_id
group by data_source;