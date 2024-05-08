/**********************************************************************
 * Truven claim_header
 * 
 * Code originally by Will/David, updated in 2022 by J. Wozny and
 * version control added March 2023 by Xiaorui
 * ********************************************************************
 * Author  || Date       || Notes
 * ********************************************************************
 * Xiaorui || 03/23/2023 || Changed mapping of pay to match what's on
 * 							the ERD column map (verified by Lopita)
 * --------------------------------------------------------------------
 * Xiaorui || 04/18/2023 || Changed msclmid to claim_id_derv
 * --------------------------------------------------------------------
 * Xiaorui || 07/20/2023 || Split into trum and truc
 * --------------------------------------------------------------------
 * Xiaorui || 03/14/2024 || Fixed stdprov(int) -> stdprov(char) conversion
 ***********************************************************************/

select 'Truven MDCR Claim Header script started at ' || current_timestamp as message;

drop table if exists dw_staging.trum_claim_header;

--create empty table
create table dw_staging.trum_claim_header
(like data_warehouse.claim_header including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

vacuum analyze dw_staging.trum_claim_header;

select 'Insert from mdcrs: ' || current_timestamp as message;

--insert from MDCRS
with cte as (
select enrolid, claim_id_derv, 
       min(svcdate) as svcdate,
       min(year) as year,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.mdcrs_etl
  group by enrolid, claim_id_derv 
)
   insert into dw_staging.trum_claim_header
(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_type, 
	from_date_of_service,
	to_date_of_service,
	total_charge_amount, 
	total_allowed_amount, 
	total_paid_amount, 
	fiscal_year, 
	cost_factor_year,
	table_id_src,
	member_id_src,
	claim_id_src,
	load_date
)
select  'trum',
        year,
        b.uth_member_id,
        b.uth_claim_id,
        a.facprof,
        a.svcdate,
        a.tsvcdat,
        null as total_charge_amount,
        a.pay as total_allowed_amount,
        a.netpay as total_paid_amount,
        dev.fiscal_year_func(a.svcdate) as fiscal_year,
        null as cost_factor_year,
        'mdcrs',
        a.enrolid,
        a.claim_id_derv,
        current_date
  from cte a
   join staging_clean.trum_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.claim_id_derv = b.claim_id_src  
    ;

select 'Analyze: ' || current_timestamp as message;

analyze dw_staging.trum_claim_header;

select 'Insert from mdcro: ' || current_timestamp as message;

--insert from MDCRO
with cte as (
select enrolid, claim_id_derv, 
       min(svcdate) as svcdate,
       min(year) as year,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.mdcro_etl 
  group by enrolid, claim_id_derv 
)
   insert into dw_staging.trum_claim_header
(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_type, 
	from_date_of_service,
	to_date_of_service,
	total_charge_amount, 
	total_allowed_amount, 
	total_paid_amount, 
	fiscal_year, 
	cost_factor_year,
	table_id_src,
	member_id_src,
	claim_id_src,
	load_date
)
select  'trum',
        year,
        b.uth_member_id,
        b.uth_claim_id,
        a.facprof,
        a.svcdate,
        a.tsvcdat,
        null as total_charge_amount,
        a.pay as total_allowed_amount,
        a.netpay as total_paid_amount,
        dev.fiscal_year_func(a.svcdate) as fiscal_year,
        null as cost_factor_year,
        'mdcro',
        a.enrolid,
        a.claim_id_derv,
        current_date 
   from cte a
   join staging_clean.trum_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.claim_id_derv = b.claim_id_src  
    ;

select 'Analyze: ' || current_timestamp as message;

analyze dw_staging.trum_claim_header;
  

select 'Add provider type: ' || current_timestamp as message;
--- add provider type 
   
update dw_staging.trum_claim_header a 
   set provider_type = to_char(b.stdprov , 'FM9999')
  from staging_clean.truv_mdcrf_etl b
 where a.member_id_src::bigint = b.enrolid 
   and a.claim_id_src = b.claim_id_derv 
   and substring(table_id_src,1,2) = 'md';
   
vacuum analyze dw_staging.trum_claim_header;

select 'Truven MDCR Claim Header script completed at ' || current_timestamp as message;
  