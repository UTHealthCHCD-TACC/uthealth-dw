drop table if exists data_warehouse.dim_uth_rx_claim_id; 



create table data_warehouse.dim_uth_rx_claim_id ( 
			data_source char(4),			
			uth_rx_claim_id bigserial,
			rx_claim_id_src text, 
			uth_member_id int8, 			
			member_id_src text
) 
with (appendonly=true, orientation = column)
distributed by (uth_member_id);
;

alter sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq cache 200;


analyze data_warehouse.dim_uth_rx_claim_id;



create materialized view truven.ccaed_2015 with (appendonly=true, orientation = column)
as select * from truven.ccaed a where a.year = 2015;
 
create materialized view truven.ccaed_2016 with (appendonly=true, orientation = column)
as select * from truven.ccaed a where a.year = 2016;

create materialized view truven.ccaed_2017 with (appendonly=true, orientation = column)
as select * from truven.ccaed a where a.year = 2017;


---truven commercial
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )
select 'trvc'
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.enrolid || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.enrolid 
from truven.ccaed_2015 a   -- 2016 2017
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'trvc'
 and c.member_id_src = a.enrolid::text 
 and c.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
where c.uth_rx_claim_id is null 
  and a.enrolid is not null;


--truven medicare
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )
select 'trvm'
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.enrolid || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.enrolid 
from truven.mdcrd a   -- 2016 2017
  join data_warehouse.dim_uth_member_id b 
    on b.data_source in ('trvc', 'trvm')
   and b.member_id_src = a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'trvm'
 and c.member_id_src = a.enrolid::text 
 and c.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
where c.uth_rx_claim_id is null 
  and a.enrolid is not null;

 
--medicare
select count(*) from medicare.pde_file;

select count(distinct pde_id) from medicare.pde_file;


insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 					
select 'mdcr'
       ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	   ,a.pde_id
	   ,b.uth_member_id
	   ,a.bene_id 
from medicare.pde_file a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = a.bene_id
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'mdcr'
 and c.member_id_src = a.bene_id 
 and c.rx_claim_id_src = a.pde_id
where c.uth_rx_claim_id is null 
  and a.bene_id is not null
 ;



--optum dod 
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'optd'
      ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
      ,a.clmid
      ,b.uth_member_id
      ,a.patid 
from optum_dod_refresh.rx a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optd'
   and b.member_id_src = a.patid::text
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'optd'
 and c.member_id_src = a.patid::text
 and c.rx_claim_id_src = a.clmid::text
where c.uth_rx_claim_id is null 
  and a.patid is not null
 ;

--optum zip 
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'optz'
      ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
      ,a.clmid
      ,b.uth_member_id
      ,a.patid 
from optum_zip_refresh.rx a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optz'
   and b.member_id_src = a.patid::text
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'optz'
 and c.member_id_src = a.patid::text
 and c.rx_claim_id_src = a.clmid::text
where c.uth_rx_claim_id is null 
  and a.patid is not null
 ;
