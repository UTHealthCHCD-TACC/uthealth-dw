select count (distinct  'trvc'||msclmid::text||enrolid::text||trunc(year,0)::text||b.uth_member_id)
from truven.ccaes a
---from truven.ccaeo_wc a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = enrolid::text 
where a.enrolid is not null
 and not exists ( select 1 from data_warehouse.dim_uth_claim_id c
                                            where c.data_source = 'trvc'
                                              and c.claim_id_src = a.msclmid::text
                                              and c.member_id_src = a.enrolid::text
                                              and c.data_year = trunc(a.year,0)::text )
 
--Updated version
create temporary table dim_uth_claim_id_temp as select * from data_warehouse.dim_uth_claim_id;

CREATE TABLE data_warehouse.dim_uth_claim_id_temp (
	generated_value bigserial NOT NULL,
	data_source bpchar(4) NULL,
	claim_id_src text NOT NULL,
	member_id_src text NOT NULL,
	data_year bpchar(4) NOT NULL,
	uth_claim_id int8 NULL,
	uth_member_id int8 NULL
)
WITH (
	appendonly=true, orientation=column
)
DISTRIBUTED BY (generated_value);

insert into data_warehouse.dim_uth_claim_id_temp (data_source, claim_id_src, member_id_src, data_year , uth_member_id)                                              
select distinct  'trvc', a.msclmid::text, a.enrolid::text, trunc(a.year,0)::text, b.uth_member_id                                              
--select count (distinct  'trvc'||a.msclmid::text||a.enrolid::text||trunc(a.year,0)::text||b.uth_member_id)
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
  and c.generated_value is null;
                 
select count(*) from data_warehouse.dim_uth_claim_id_temp;

drop table data_warehouse.dim_uth_claim_id;
rename data_warehouse.dim_uth_claim_id_temp to data_warehouse.dim_uth_claim_id;
ALTER TABLE data_warehouse.dim_uth_claim_id SET LOGGED; 
     
--Diagnostics

SELECT gp_segment_id, count(*) FROM data_warehouse.dim_uth_member_id GROUP BY gp_segment_id;

select count(*) FROM data_warehouse.dim_uth_member_id
select count(*) from data_warehouse.dim_uth_claim_id;
select count(*) from truven.ccaeo_wc;
select * from data_warehouse.dim_uth_claim_id;