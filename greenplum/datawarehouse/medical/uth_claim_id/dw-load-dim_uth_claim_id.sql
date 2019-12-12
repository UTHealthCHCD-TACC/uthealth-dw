/* code to populate dim_uth_claim_id, run dw-create-dim_uth_claim_id.sql to build table first
 * 
 * this code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 */

---truven commercial, outpatient
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)                                              
select distinct  'trvc', a.msclmid::text, a.enrolid::text, trunc(a.year,0)::text, b.uth_member_id                                              
from truven.ccaeo a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
                                            on  b.data_source = c.data_source
                                              and a.msclmid::text = c.claim_id_src 
                                              and a.enrolid::text = c.member_id_src
                                              and trunc(a.year,0)::text = c.data_year 
  where a.enrolid is not null
  and c.generated_value is null
;

 
--truven commercial, inpatient 
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)                                              
select distinct  'trvc', coalesce(a.msclmid::text,a.caseid::text), a.enrolid::text, trunc(a.year,0)::text, b.uth_member_id                                              
from truven.ccaes a  
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
                                            on  b.data_source = c.data_source
                                              and a.msclmid::text = c.claim_id_src 
                                              and a.enrolid::text = c.member_id_src
                                              and trunc(a.year,0)::text = c.data_year 
  where a.enrolid is not null
  and c.generated_value is null
;
 
 
--- truven medicare outpatient 
 insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)                                              
select distinct  'trvc', a.msclmid::text, a.enrolid::text, trunc(a.year,0)::text, b.uth_member_id                                              
from truven.mdcro a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
                                            on  b.data_source = c.data_source
                                              and a.msclmid::text = c.claim_id_src 
                                              and a.enrolid::text = c.member_id_src
                                              and trunc(a.year,0)::text = c.data_year 
  where a.enrolid is not null
  and c.generated_value is null
;
 
 
---Truven medicare inpatient  
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)     
select distinct  'trvc', coalesce(a.msclmid::text,a.caseid::text), a.enrolid::text, trunc(a.year,0)::text, b.uth_member_id                                              
from truven.mdcrs a  
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
  left join data_warehouse.dim_uth_claim_id c
                                            on  b.data_source = c.data_source
                                              and a.msclmid::text = c.claim_id_src 
                                              and a.enrolid::text = c.member_id_src
                                              and trunc(a.year,0)::text = c.data_year 
  where a.enrolid is not null
  and c.generated_value is null
;


 


update data_warehouse.dim_uth_claim_id set uth_claim_id = ( substring(data_year::text,3,2) || generated_value::text  )::bigint;



