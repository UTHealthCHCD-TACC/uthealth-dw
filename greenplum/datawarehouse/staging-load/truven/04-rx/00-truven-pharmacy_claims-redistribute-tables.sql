/*
 * Create temporary tables 
 */

drop table if exists staging_clean.truven_rx_claim_id;

create table staging_clean.truven_rx_claim_id with (
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
 where data_source = 'truv'
distributed by (rx_claim_id_src) 
;

analyze staging_clean.truven_rx_claim_id;

/*
 * Create Pharmacy Tables
 */

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

analyze staging_clean.ccaed_etl;

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

analyze staging_clean.mdcrd_etl;