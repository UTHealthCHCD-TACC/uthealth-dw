--This table is used to generate a de-identified claim id that will be used to populate claim_detail and claim_header tables
--The uth_claim_id column will be a sequence that is initially set to a 100,000,000


select count(*), data_source, data_year 
from data_warehouse.dim_uth_claim_id 
group by data_source, data_year
order by data_source, data_year

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


/* code to populate dim_uth_claim_id
 * 
 * this code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 */


---truven commercial, outpatient 
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


vacuum analyze truven.ccaes; 

vacuum analyze data_warehouse.dim_uth_claim_id;

--truven commercial, inpatient
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



vacuum analyze data_warehouse.dim_uth_claim_id;


---truven medicare, outpatient 
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


---truven medicare inpatient
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


vacuum analyze data_warehouse.dim_uth_claim_id


--Optum dod 
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


--Optum zip 20m
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

vacuum analyze data_warehouse.dim_uth_claim_id;

select count(*), data_source 
from data_warehouse.dim_uth_claim_id
group by data_source;
---------------------------------------------------------------------------------------------------
---- Medicare :
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient,
---- and snf tables to generate uth_claim_ids.
---------------------------------------------------------------------------------------------------


vacuum analyze data_warehouse.dim_uth_claim_id

--bcarrier
drop table dev.wc_bcarrier_tx

create table dev.wc_bcarrier_tx
with (appendonly=true, orientation=column)
as 
select * from medicare_texas.bcarrier_claims_k where year = '2018'
distributed by (bene_id);


--bcarrier
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)      
select  'mcrt', clm_id, bene_id, b.uth_member_id, a.year::int2
from dev.wc_bcarrier_tx a 
--medicare_texas.bcarrier_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id 
   and c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
where c.uth_claim_id is null 
;




--dme
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrt', clm_id, bene_id, b.uth_member_id, a.year::int2
from medicare_texas.dme_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
;



--hha
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrt', clm_id, bene_id, b.uth_member_id, a.year::int2
from medicare_texas.hha_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
;

--hospice
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrt', clm_id, bene_id, b.uth_member_id, a.year::int2
from medicare_texas.hospice_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
;


--inpatient
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrt', clm_id, bene_id, b.uth_member_id, a.year::int2
from medicare_texas.inpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
;

--outpatient
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select 'mcrt', clm_id, bene_id, b.uth_member_id, a.year::int2
from medicare_texas.outpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
;

--snf
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select 'mcrt', clm_id, bene_id, b.uth_member_id, a.year::int2
from medicare_texas.snf_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
;


-------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---- Medicare **National**:
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient,
---- and snf tables to generate uth_claim_ids.
---------------------------------------------------------------------------------------------------


--bcarrier
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)      
select  'mcrn', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare_national.bcarrier_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;

--dme
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrn', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare_national.dme_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
group by 1, 2, 3, 4;

--hha
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrn', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare_national.hha_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;

--hospice
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrn', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare_national.hospice_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null
group by 1, 2, 3, 4;


--inpatient
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrn', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare_national.inpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.member_id_src = bene_id
   and b.data_source = 'mcrn'
  left join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id 
   and c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
where c.uth_claim_id is null
group by 1, 2, 3, 4;

--outpatient
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select 'mcrn', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare_national.outpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;

--snf
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
select  'mcrn', clm_id, bene_id, b.uth_member_id, min(extract(year from clm_from_dt::date))
from medicare_national.snf_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
where c.uth_claim_id is null 
group by 1, 2, 3, 4;


------------medicaid
--claims
with cte_mdcd_clm as ( 
select distinct a.icn, b.pcn
from medicaid.clm_header a 
   join medicaid.clm_proc b  
    on a.icn = b.icn
) 
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', c.icn, c.pcn, b.uth_member_id, extract(year from a.hdr_frm_dos::date) as yr
from medicaid.clm_header a
  join cte_mdcd_clm c 
    on c.icn = a.icn 
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = c.pcn  
  left outer join data_warehouse.dim_uth_claim_id d 
    on d.member_id_src = c.pcn 
   and d.claim_id_src = c.icn
where d.uth_member_id is null 
; 


---medicaid
--encounter
with cte_mdcd_enc as ( 
select distinct a.derv_enc, b.mem_id 
from medicaid.enc_header a 
   join medicaid.enc_proc b  
     on a.derv_enc = b.derv_enc 
)
--insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.derv_enc, b.mem_id--, c.uth_member_id, extract(year from a.frm_dos::date) as yr
from medicaid.enc_header a 
   join cte_mdcd_enc b 
     on b.derv_enc = a.derv_enc   
   join data_warehouse.dim_uth_member_id c 
     on c.member_id_src = b.mem_id 
     
     
   left outer join data_warehouse.dim_uth_claim_id d 
     on d.member_id_src = b.mem_id
    and d.claim_id_src = b.derv_enc 
where d.uth_claim_id is null 
;


select count(*) from data_warehouse.dim_uth_claim_id where data_source = 'mdcd';



--------------------------------------------------------------------/End/-------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------
vacuum analyze  data_warehouse.dim_uth_claim_id;

-- Scratch

select  count(*), data_source, data_year 
from data_warehouse.dim_uth_claim_id
group by data_source, data_year
order by data_source, data_year ;


select count(distinct msclmid::text || enrolid::text || year::text ) from truven.ccaeo;


select count(distinct msclmid) from truven.ccaes;


select count(distinct a.clmid ), year from optum_dod.medical a group by year;


select count(uth_member_id), data_source
from data_warehouse.dim_uth_member_id
group by data_source;