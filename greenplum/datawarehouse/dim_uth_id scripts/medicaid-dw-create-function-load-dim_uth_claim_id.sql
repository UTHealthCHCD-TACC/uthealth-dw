---------member ids 

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


----------------claim ids ----------------------------------
vacuum full medicaid.clm_proc;
vacuum analyze data_warehouse.dim_uth_member_id;
vacuum analyze data_warehouse.dim_uth_claim_id;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.icn, a.pcn, b.uth_member_id, a.year_fy 
from medicaid.clm_proc a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = a.pcn  
   and b.data_source = 'mdcd'
  left outer join data_warehouse.dim_uth_claim_id c
    on c.member_id_src = a.pcn 
   and c.claim_id_src = a.icn
 where c.uth_member_id is null 
; 


---medicaid
--encounter

vacuum full medicaid.enc_proc;
vacuum analyze data_warehouse.dim_uth_member_id;
vacuum analyze data_warehouse.dim_uth_claim_id;

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src , uth_member_id , data_year)
select 'mdcd', a.derv_enc, a.mem_id, b.uth_member_id, a.year_fy 
from medicaid.enc_proc a   
   join data_warehouse.dim_uth_member_id b 
     on b.member_id_src = a.mem_id    
    and b.data_source = 'mdcd'
   left outer join data_warehouse.dim_uth_claim_id c
     on c.member_id_src = a.mem_id
    and c.claim_id_src = a.derv_enc 
where c.uth_claim_id is null 
;

vacuum analyze data_warehouse.dim_uth_member_id;
vacuum analyze data_warehouse.dim_uth_claim_id;


-------------------------------RX-----------------------------------------
---chip rx 

vacuum analyze data_warehouse.dim_uth_rx_claim_id;

insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 				
select 'mdcd', 
       a.year_fy,
       nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       pcn || ndc || replace(rx_fill_dt::text, '-',''),
       b.uth_member_id, 
       a.pcn
from medicaid.chip_rx a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = pcn
   and b.data_source = 'mdcd' 
  left outer join data_warehouse.dim_uth_rx_claim_id c 
    on c.member_id_src = a.pcn 
   and c.rx_claim_id_src = pcn || ndc || replace(rx_fill_dt::text, '-','')
   and c."year" = a.year_fy 
   and c.data_source = 'mdcd' 
where c.uth_rx_claim_id is null 
;
-----

vacuum analyze data_warehouse.dim_uth_rx_claim_id;

--medicaid ffs rx   
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'mdcd', 
       a.year_fy,
       nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       pcn || ndc || replace(rx_fill_dt::text, '-',''),
       b.uth_member_id, 
       a.pcn
from medicaid.ffs_rx a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = pcn
   and b.data_source = 'mdcd' 
  left outer join data_warehouse.dim_uth_rx_claim_id c 
    on c.member_id_src = a.pcn 
   and c.rx_claim_id_src = pcn || ndc || replace(rx_fill_dt::text, '-','')
   and c."year" = a.year_fy 
   and c.data_source = 'mdcd' 
where c.uth_rx_claim_id is null 
;

vacuum analyze data_warehouse.dim_uth_rx_claim_id;

--medicaid mco rx   
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'mdcd', 
       a.year_fy,
       nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       pcn || ndc || replace(rx_fill_dt::text, '-',''),
       b.uth_member_id, 
       a.pcn
from medicaid.mco_rx a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = pcn
   and b.data_source = 'mdcd' 
  left outer join data_warehouse.dim_uth_rx_claim_id c 
    on c.member_id_src = a.pcn 
   and c.rx_claim_id_src = pcn || ndc || replace(rx_fill_dt::text, '-','')
   and c."year" = a.year_fy 
   and c.data_source = 'mdcd' 
where c.uth_rx_claim_id is null 
;

vacuum analyze data_warehouse.dim_uth_rx_claim_id;
