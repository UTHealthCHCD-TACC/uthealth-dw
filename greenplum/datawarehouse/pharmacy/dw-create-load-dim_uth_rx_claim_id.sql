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
from truven.ccaed_2017 a   -- 2016
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trvc'
   and b.member_id_src = a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'trvc'
 and c.member_id_src = a.enrolid::text 
 and c.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
where c.uth_rx_claim_id is null 
  and a.enrolid is not null;



 
 create materialized view truven.ccaed_2015 with (appendonly=true, orientation = column)
 as select * from truven.ccaed a where a.year = 2015;
 
 create materialized view truven.ccaed_2016 with (appendonly=true, orientation = column)
 as select * from truven.ccaed a where a.year = 2016;


 create materialized view truven.ccaed_2017 with (appendonly=true, orientation = column)
 as select * from truven.ccaed a where a.year = 2017;




 select count(distinct enrolid || ndcnum::text || svcdate::text) from truven.ccaed;


select count(*) from data_warehouse.dim_uth_rx_claim_id


select 'trvm'
	  ,a.enrolid || ndcnum::text || svcdate::text
      ,a.enrolid 
from truven.mdcrd a
where enrolid is not null;


select 'mdcr'
	   ,a.pde_id
	   ,a.bene_id 
from medicare.pde_file a
;

select 'optd'
      ,a.clmid
      ,a.patid 
from optum_dod.rx a 
--where patid is null
;


select 'optz'
      ,a.clmid
      ,a.patid 
from optum_zip.rx a
;