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

-------------------HTW------------------------------
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'mdcd', 
       dev.fiscal_year_func(a.rx_fill_dt) as fiscal_year,
       nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       pcn || ndc || replace(rx_fill_dt::text, '-',''),
       b.uth_member_id, 
       a.pcn
from medicaid.htw_ffs_rx a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = pcn
   and b.data_source = 'mdcd' 
  left outer join data_warehouse.dim_uth_rx_claim_id c 
    on c.member_id_src = a.pcn 
   and c.rx_claim_id_src = pcn || ndc || replace(rx_fill_dt::text, '-','')
   and c.data_source = 'mdcd' 
where c.uth_rx_claim_id is null 
;

vacuum analyze data_warehouse.dim_uth_rx_claim_id;