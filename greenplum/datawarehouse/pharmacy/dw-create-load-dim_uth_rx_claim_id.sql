drop table if exists data_warehouse.dim_uth_rx_claim_id; 

create table data_warehouse.dim_uth_rx_claim_id ( 
			data_source char(4),	
			year int2,
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


vacuum analyze data_warehouse.dim_uth_rx_claim_id;



--create materialized view truven.ccaed_2015 with (appendonly=true, orientation = column)
--as select * from truven.ccaed a where a.year = 2015;
 
vacuum analyze truven.ccaed;


---truven commercial
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truv'
      ,a.year 
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.enrolid || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.enrolid 
from truven.ccaed a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'truv'
 and c.member_id_src = a.enrolid::text 
 and c.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
 and  c.uth_rx_claim_id is null 
where a.enrolid is not null;


--truven medicare
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )				
select 'truv'
      ,a.year 
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.enrolid || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.enrolid 
from truven.mdcrd a  
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'truv'
 and c.member_id_src = a.enrolid::text 
 and c.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
 and c.uth_rx_claim_id is null 
where a.enrolid is not null;


select count(*), year 
from data_warehouse.dim_uth_rx_claim_id 
where data_source = 'truv'
group by "year" 
order by "year" 

select count(*), year 
from truven.mdcrd m 
group by year 
order by year 
;


select count(*), count(distinct uth_rx_claim_id ), data_year 
from data_warehouse.pharmacy_claims pc 
where data_source = 'truv'
group by data_year order by data_year ;


--medicare
select count(*) from medicare_texas.pde_file;

select count(distinct pde_id) from medicare_texas.pde_file;



update data_warehouse.dim_uth_rx_claim_id set data_source = 'mcrt' where data_source = 'mdcr';

insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 					
select 'mcrt'
       ,a.year::int
       ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	   ,a.pde_id
	   ,b.uth_member_id
	   ,a.bene_id 
from medicare_texas.pde_file a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = a.bene_id
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'mcrt'
 and c.member_id_src = a.bene_id 
 and c.rx_claim_id_src = a.pde_id
where c.uth_rx_claim_id is null 
  and a.bene_id is not null
 ;



---Medicare National
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 					
select 'mcrn'
       ,a.year::int
       ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	   ,a.pde_id
	   ,b.uth_member_id
	   ,a.bene_id 
from medicare_texas.pde_file a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = a.bene_id
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'mcrn'
 and c.member_id_src = a.bene_id 
 and c.rx_claim_id_src = a.pde_id
where c.uth_rx_claim_id is null 
  and a.bene_id is not null
 ;




vacuum analyze optum_dod.rx 

--optum dod 
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'optd'
      ,a.year 
      ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
      ,a.clmid
      ,b.uth_member_id
      ,a.patid 
from optum_dod.rx a
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

select count(*) from data_warehouse.dim_uth_rx_claim_id where data_source = 'optd';


select count(distinct clmid) from optum_dod.rx;

--optum zip 
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'optz'
      ,a.year 
      ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
      ,a.clmid
      ,b.uth_member_id
      ,a.patid 
from optum_zip.rx a
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


vacuum analyze data_warehouse.dim_uth_rx_claim_id;