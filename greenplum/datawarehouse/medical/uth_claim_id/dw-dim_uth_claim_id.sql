

drop table data_warehouse.dim_uth_claim_id;

create table data_warehouse.dim_uth_claim_id (    
	generated_value bigserial,
	data_source char(4),
	claim_id_src text not null,
	member_id_src text not null,
	data_year int4 not null, 
	uth_claim_id bigint,
	uth_member_id bigint
) with (appendonly=true, orientation = column)
distributed by (generated_value);


alter sequence data_warehouse.dim_uth_claim_id_generated_value_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_claim_id_generated_value_seq cache 200;


alter table data_warehouse.dim_uth_claim_id alter column data_year type numeric(4,0) using(data_year::numeric(4,0));

select dbo.set_all_perms();

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
  and c.generated_value is null;
 
 
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
  and c.generated_value is null;
 
--- Medicare outpatient 
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
  and c.generated_value is null;
 
 
 
--- Medicare inpatient  
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
  and c.generated_value is null;
 


update data_warehouse.dim_uth_claim_id set uth_claim_id = ( substring(data_year::text,3,2) || generated_value::text  )::bigint;




select * , data_year::numeric(4,0) 
from data_warehouse.dim_uth_claim_id
where data_year = '';