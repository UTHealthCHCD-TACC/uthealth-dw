
/* ******************************************************************************************************
 *  Truven RX tables : make ETL
 * ******************************************************************************************************
 *  Author  || Date       || Notes
 * ******************************************************************************************************
 *  various || <Apr 2023  ||  Created script
 * ****************************************************************************************************** 
 *  xrzhang || 05/12/2023 || Fixed the issue where we were assigning > 1 uth_rx_id for 1 truven rx_id,
 * 							 added timestamps for running in psql
 * 
 * 
 * last run date: 05/12/2023 (after Truven 2022 Q2 data loaded)
 * */

/*******************
 * HOTFIX 05/12/2023
 * 
 * We gotta blow up the the whole thing and start over so
 * 
 * delete from data_warehouse.dim_uth_rx_claim_id where data_source = 'truv';
 * 
 * vacuum analyze data_warehouse.dim_uth_rx_claim_id;
 */


select 'Truven dim uth rx id script started at ' || current_timestamp as message;

--truven medicare
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truv'
      ,year
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,enrolid::text || ndcnum::text || svcdate::text as rx_claim_id_src
	  ,uth_member_id	  
      ,enrolid::text as member_id_src
from ( select distinct a.year, a.enrolid, a.ndcnum, a.svcdate, b.uth_member_id
		from truven.mdcrd a
		  join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'truv'
		   and b.member_id_src =  a.enrolid::text 
		left join data_warehouse.dim_uth_rx_claim_id c 
		  on c.data_source = 'truv'
		 and c.member_id_src = a.enrolid::text
		 and c.rx_claim_id_src = a.enrolid::text || ndcnum::text || svcdate::text
		where c.uth_rx_claim_id is null 
		  and a.enrolid::text is not null ) t;
		 
select 'Truven dim uth rx id script mdcrd completed, starting ccaed at ' || current_timestamp as message;

---truven commercial
insert into data_warehouse.dim_uth_rx_claim_id (
			data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truv'
      ,year
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,enrolid::text || ndcnum::text || svcdate::text as rx_claim_id_src
	  ,uth_member_id	  
      ,enrolid::text as member_id_src
from ( select distinct a.year, a.enrolid, a.ndcnum, a.svcdate, b.uth_member_id
		from truven.ccaed a
		  join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'truv'
		   and b.member_id_src =  a.enrolid::text 
		left join data_warehouse.dim_uth_rx_claim_id c 
		  on c.data_source = 'truv'
		 and c.member_id_src = a.enrolid::text
		 and c.rx_claim_id_src = a.enrolid::text || ndcnum::text || svcdate::text
		where c.uth_rx_claim_id is null 
		  and a.enrolid::text is not null ) t;
		 
select count(*) from data_warehouse.dim_uth_rx_claim_id where data_source = 'truv';
		 
vacuum analyze data_warehouse.dim_uth_rx_claim_id;

select 'Truven dim uth rx id script completed at ' || current_timestamp as message;


