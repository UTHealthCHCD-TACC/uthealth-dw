/*
 * current dim load script was too long, testing if breaking into staging tables helps. 
 */

drop table if exists dw_staging.truven_dim_claim_load;

create table dw_staging.truven_dim_claim_load with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select msclmid::text as claim_id_src, 
	   enrolid::text as member_id_src, 
	   year as v_year 
  from truven.ccaeo
 where enrolid is not null
  distributed by (member_id_src)
  ;
			
 
select pg_size_pretty(pg_relation_size('dw_staging.truven_dim_claim_load'));
 
 
 
select * from dw_staging.truven_dim_claim_load;
			
			
vacuum analyze data_warehouse.dim_uth_member_id;
			

select * from dw_staging.truven_dim_claim_load ;

select count(distinct member_id_src || claim_id_src ) 
  from dw_staging.truven_dim_claim_load 
union all
select count(distinct member_id_src || claim_id_src || v_year::text) 
  from dw_staging.truven_dim_claim_load ;



insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)   
with cte_distinct_truven_claim as 
(      select distinct 'truv' as v_data_source, v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year
       from (                                        
				select  a.msclmid::text as v_claim_id_src, 
				        a.enrolid::text as v_member_id_src, 
				        b.uth_member_id as v_uth_member_id, 
				        min(trunc(a.year,0)) as v_data_year                                      
				from truven.ccaeo a
				  join data_warehouse.dim_uth_member_id b 
				    on b.data_source = 'truv'
				   and b.member_id_src = a.enrolid::text 
				group by 1, 2, 3
           union                                          
				select a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))
				  from truven.ccaes a  
				  join data_warehouse.dim_uth_member_id b 
				    on b.data_source = 'truv'
				   and b.member_id_src = a.enrolid::text 
				group by 1, 2, 3
           union                                
				select a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))
				from truven.mdcro a
				  join data_warehouse.dim_uth_member_id b 
				    on b.data_source = 'truv'
				   and b.member_id_src = a.enrolid::text 
				group by 1, 2, 3
			union   
				select a.msclmid::text, a.enrolid::text, b.uth_member_id, min(trunc(a.year,0))
				from truven.mdcrs a  
				  join data_warehouse.dim_uth_member_id b 
				    on b.data_source = 'truv'
				   and b.member_id_src = a.enrolid::text 
				group by 1,2,3
        ) inr 
)     
select v_data_source, v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year 
from cte_distinct_truven_claim 
  left outer join data_warehouse.dim_uth_claim_id c
    on c.data_source = v_data_source
   and c.claim_id_src = v_claim_id_src
   and c.member_id_src = v_member_id_src
   and c.data_year = v_data_year 
where c.uth_claim_id is null
;