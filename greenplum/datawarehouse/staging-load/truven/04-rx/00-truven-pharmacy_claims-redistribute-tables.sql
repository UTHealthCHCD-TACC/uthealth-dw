
/* ******************************************************************************************************
 *  Truven RX tables : make ETL
 * ******************************************************************************************************
 *  Author  || Date       || Notes
 * ******************************************************************************************************
 *  various || <Apr 2023  ||  Created script
 * ****************************************************************************************************** 
 *  xzhang  || 04/19/2023 || Rearranged script order (run medicare first, it's a smaller table)
 * 							 and added flags
 * ****************************************************************************************************** 
 *  xzhang  || 07/21/2023 || Split Truven into trum and truc
 * */

select 'Truven RX ETL script started at ' || current_timestamp as message;

select 'Get dim_uth_rx_clm for trum: ' || current_timestamp as message;
--Get dim_uth_rx_clm for trum
drop table if exists staging_clean.trum_rx_claim_id;

create table staging_clean.trum_rx_claim_id with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select rx_claim_id_src, 
       member_id_src::bigint,
       uth_rx_claim_id,
       uth_member_id 
  from data_warehouse.dim_uth_rx_claim_id
 where data_source = 'trum'
distributed by (rx_claim_id_src) 
;

select 'Analyze: ' || current_timestamp as message;

analyze staging_clean.trum_rx_claim_id;

select 'Get dim_uth_rx_clm for truc: ' || current_timestamp as message;
--Get dim_uth_rx_clm for truc
drop table if exists staging_clean.truc_rx_claim_id;

create table staging_clean.truc_rx_claim_id with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select rx_claim_id_src, 
       member_id_src::bigint,
       uth_rx_claim_id,
       uth_member_id 
  from data_warehouse.dim_uth_rx_claim_id
 where data_source = 'truc'
distributed by (rx_claim_id_src) 
;

select 'Analyze: ' || current_timestamp as message;

analyze staging_clean.truc_rx_claim_id;


/*
 * Create Pharmacy Tables
 */

select 'Creating mdcrd_etl: ' || current_timestamp as message;

--- medicare pharmacy
drop table if exists staging_clean.mdcrd_etl;

create table staging_clean.mdcrd_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select *, (enrolid::text || ndcnum::text || svcdate::text) as rx_id_src
from truven.mdcrd  
distributed by (rx_id_src);

select 'Analyze: ' || current_timestamp as message;

analyze staging_clean.mdcrd_etl;

select 'Creating ccaed_etl: ' || current_timestamp as message;

--- commercial pharmacy
drop table if exists staging_clean.ccaed_etl;

create table staging_clean.ccaed_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select *, (enrolid::text || ndcnum::text || svcdate::text) as rx_id_src
from truven.ccaed 
distributed by (rx_id_src);

select 'Analyze: ' || current_timestamp as message;

analyze staging_clean.ccaed_etl;

--output completed message
select 'Truven RX ETL script completed at ' || current_timestamp as message;


--select count(*) from staging_clean.mdcrd_etl;
--665986858

--select count(*) from truven.mdcrd;
--665986858 so this part is right
