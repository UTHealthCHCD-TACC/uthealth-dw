insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mdcd' as v_raw_data 
   from ( 
   			select client_nbr 
	   		from medicaid.enrl  
	   	union
	   		select client_nbr
   			from medicaid.chip_uth
   		union
   			select client_nbr 
   			  from medicaid.htw_enrl 
         ) inr 
    left outer join data_warehouse.dim_uth_member_id 
      on data_source = 'mdcd' 
     and member_id_src = client_nbr 
    where member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

vacuum analyze data_warehouse.dim_uth_member_id;

/*
---------member ids claim created 
vacuum analyze data_warehouse.dim_uth_member_id;

insert into data_warehouse.dim_uth_member_id  (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as ( 
	select distinct pcn as v_member_id, 'mdcd' as v_data_source
	from ( 
			select a.pcn
			from medicaid.clm_proc a
		union 
			select a.mem_id as pcn
			from medicaid.enc_proc a   
		 ) raw_clms
   left outer join data_warehouse.dim_uth_member_id b 
   		 on b.member_id_src = raw_clms.pcn 
  		and b.data_source = 'mdcd'
	where b.member_id_src is null 
 )
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member ;
*/