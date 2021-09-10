/* ******************************************************************************************************
 *  This table is used to generate a de-identified pharmacy claim id that will be used to populate pharmacy_claims
 *	The uth_rx_claim_id column will be a sequence that is initially set to a 100,000,000
 *  This code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 8/31/2021 ||comments added
 * ******************************************************************************************************
 * 
 * ****************************************************************************************************** */

--//BEGIN SCRIPT

---truven commercial
with truv_cte as (  
   select distinct on ( enrolid || ndcnum::text || svcdate::text)
   enrolid, ndcnum, svcdate, year 
   from truven.ccaed 
   )
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
from truv_cte a
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
with truv_cte as (  
   select distinct on ( enrolid || ndcnum::text || svcdate::text)
   enrolid, ndcnum, svcdate, year 
   from truven.mdcrd 
   )
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
from truv_cte a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.enrolid::text 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'truv'
 and c.member_id_src = a.enrolid::text 
 and c.rx_claim_id_src = a.enrolid || ndcnum::text || svcdate::text
 and  c.uth_rx_claim_id is null 
where a.enrolid is not null;



vacuum analyze data_warehouse.dim_uth_rx_claim_id;



--medicare texas 
with medicare_texas_cte as (  
    select distinct on (pde_id) 
        year, pde_id, bene_id 
    from medicare_texas.pde_file
    )
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
from medicare_texas_cte a
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
with medicare_cte as (  
    select distinct on (pde_id) 
        year, pde_id, bene_id 
    from medicare_national.pde_file
    )
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
from medicare_cte a
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


--optum dod   4min
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
      ,a.patid::text 
from optum_dod.rx a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optd'
   and b.member_id_src = a.patid::text
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'optd'
 and c.member_id_src = a.patid::text
 and c.rx_claim_id_src = a.clmid
 and c."year" = a."year" 
where c.uth_rx_claim_id is null 
 ;


--optum zip 4min
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
      ,a.patid::text 
from optum_zip.rx a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optz'
   and b.member_id_src = a.patid::text
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'optz'
 and c.member_id_src = a.patid::text
 and c.rx_claim_id_src = a.clmid
 and c.year = a.year 
where c.uth_rx_claim_id is null 
 ;


vacuum analyze data_warehouse.dim_uth_rx_claim_id;



select data_source, year, count(*) 
from data_warehouse.dim_uth_rx_claim_id 
group by data_source , year 
order by data_source , year 
;

---//END SCRIPT 

/*  Original Table Create
 * --!do not run unless table has to be recreated

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

*/