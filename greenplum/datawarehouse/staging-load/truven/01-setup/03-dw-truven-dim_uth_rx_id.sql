
/* ******************************************************************************************************
 *  Truven RX tables : make ETL
 * ******************************************************************************************************
 *  Author  || Date       || Notes
 * ******************************************************************************************************
 *  various || <Apr 2023  ||  Created script
 * ****************************************************************************************************** 
 *  xrzhang || 05/12/2023 || Fixed the issue where we were assigning > 1 uth_rx_id for 1 truven rx_id,
 * 							 added timestamps for running in psql
 * ------------------------------------------------------------------------------------------------------
 *  xrzhang || 07/13/2023 || Split 'truv' into 'trum' and 'truc'
 * 
 * 
 * last run date: 07/13/2023 (2022 Q3 data, truv split into trum and truc)
 * */

/*******************
 * HOTFIX 05/12/2023 (also used 7/13/23)
 * 
 * We gotta blow up the the whole thing and start over so
 * 
delete from data_warehouse.dim_uth_rx_claim_id where data_source = 'truv';
vacuum analyze data_warehouse.dim_uth_rx_claim_id;
 */


select 'Truven dim uth rx id script (trum) started at ' || current_timestamp as message;

--truven medicare
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'trum'
      ,year
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,enrolid::text || ndcnum::text || svcdate::text as rx_claim_id_src
	  ,uth_member_id	  
      ,enrolid::text as member_id_src
from ( select distinct a.year, a.enrolid, a.ndcnum, a.svcdate, b.uth_member_id
		from truven.mdcrd a
		  join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'trum'
		   and b.member_id_src =  a.enrolid::text 
		left join data_warehouse.dim_uth_rx_claim_id c 
		  on c.data_source = 'trum'
		 and c.member_id_src = a.enrolid::text
		 and c.rx_claim_id_src = a.enrolid::text || ndcnum::text || svcdate::text
		where c.uth_rx_claim_id is null 
		  and a.enrolid::text is not null ) t;
		 
select 'Truven dim uth rx id script (trum) completed, starting truc at ' || current_timestamp as message;

---truven commercial
insert into data_warehouse.dim_uth_rx_claim_id (
			data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truc'
      ,year
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,enrolid::text || ndcnum::text || svcdate::text as rx_claim_id_src
	  ,uth_member_id	  
      ,enrolid::text as member_id_src
from ( select distinct a.year, a.enrolid, a.ndcnum, a.svcdate, b.uth_member_id
		from truven.ccaed a
		  join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'truc'
		   and b.member_id_src =  a.enrolid::text 
		left join data_warehouse.dim_uth_rx_claim_id c 
		  on c.data_source = 'truc'
		 and c.member_id_src = a.enrolid::text
		 and c.rx_claim_id_src = a.enrolid::text || ndcnum::text || svcdate::text
		where c.uth_rx_claim_id is null 
		  and a.enrolid::text is not null ) t;
		 
--select count(*) from data_warehouse.dim_uth_rx_claim_id where data_source = 'truc';

select 'Truven dim uth rx id script (truc) completed, vacuum analyze/backup started at ' || current_timestamp as message;
		 
vacuum analyze data_warehouse.dim_uth_rx_claim_id;

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Updated for Truven 2022 Q3, split Truven into truc and trum',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse' and table_name = 'dim_uth_rx_claim_id';

select 'Truven dim uth rx id script completed at ' || current_timestamp as message;




















