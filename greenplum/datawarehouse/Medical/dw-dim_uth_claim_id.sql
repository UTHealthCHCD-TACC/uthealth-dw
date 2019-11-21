

drop table data_warehouse.dim_uth_claim_id;

create table data_warehouse.dim_uth_claim_id (    
	generated_value bigserial,
	data_source char(4),
	claim_id_src text not null,
	member_id_src text not null,
	data_year char(4) not null, 
	uth_claim_id bigint,
	uth_member_id bigint,
	unique (data_source, claim_id_src, member_id_src, data_year)
) distributed by (data_source, claim_id_src, member_id_src, data_year);


alter sequence data_warehouse.dim_uth_claim_id_generated_value_seq restart with 100000000;


select dbo.set_all_perms();



insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year , uth_member_id)


explain analyze 
select distinct 'trvc', msclmid::text, enrolid::text, trunc(year,0)::text--, b.uth_member_id
from truven.ccaeo a
limit 10;



  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text  
where a.enrolid is not null
 and not exists ( select 1 from data_warehouse.dim_uth_claim_id c
				            where c.data_source = 'trvc'
				              and c.claim_id_src = a.msclmid::text
				              and c.member_id_src = a.enrolid::text
				              and c.data_year = trunc(a.year,0)::text
             	)
limit 10;


 
 
 
 
 
 
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, data_year )


select distinct 'trvc', msclmid::text, enrolid::text, trunc(year,0)::text, b.uth_member_id 
from truven.ccaeo a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = enrolid::text 
   
   
   
  left outer join data_warehouse.dim_uth_claim_id c
               on c.data_source = 'trvc'
              and c.claim_id_src = msclmid::text
              and c.member_id_src = enrolid::text
              and c.data_year = trunc(year,0)::text
where c.claim_id_src is null 
and a.enrolid is not null
and a.year = 2017;
 

--trvc, 5554450.0, 33108316402.0, 2017

select * from truven.ccaes where msclmid = 5554450.0 and year = 2017

select a.msclmid,msclmid::text, a.enrolid, enrolid::text, a.year , b.* 
from truven.ccaeo a
  left outer join data_warehouse.dim_uth_claim_id b
               on b.data_source = 'trvc'
              and b.claim_id_src = msclmid::text
              and b.member_id_src = enrolid::text
              and b.data_year = trunc(year,0)::text
where msclmid = 22768661.0 and enrolid = 4462286502.0 and year = 2017

select * from data_warehouse.dim_uth_claim_id where claim_id_src = '1.0' and member_id_src = '720140604.0' and data_year = '2017'

delete
from data_warehouse.dim_uth_claim_id;


update data_warehouse.dim_uth_claim_id set uth_claim_id = ( substring(data_year::text,3,2) || generated_value::text )::bigint;

select count(*)
from data_warehouse.dim_uth_claim_id --where claim_id_src = '308900219.0'


select count(*), year from truven.ccaeo group by year;


select enrolid, msclmid::text, year::text
from truven.ccaeo_wc a 
limit 1;




