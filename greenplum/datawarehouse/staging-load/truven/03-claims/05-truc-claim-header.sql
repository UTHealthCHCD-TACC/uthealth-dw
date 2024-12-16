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
 * --------------------------------------------------------------------
 * Xiaorui || 12/12/2024 || provider_type column got renamed to provider_specialty
 *                          Change was implemented due to Medicaid in March 2024
 ***********************************************************************/

select 'Truven CCAE Claim Header script started at ' || current_timestamp as message;

drop table if exists dw_staging.truc_claim_header;

--create empty table
create table dw_staging.truc_claim_header
(like data_warehouse.claim_header including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

vacuum analyze dw_staging.truc_claim_header;

select 'Insert from ccaes: ' || current_timestamp as message;

--Insert from CCAES
with cte as (
select enrolid, claim_id_derv, 
       min(svcdate) as svcdate,
       min(year) as year,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.ccaes_etl
  group by enrolid, claim_id_derv 
)
   insert into dw_staging.truc_claim_header
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
select  'truc' as data_source,
        year,
        b.uth_member_id,
        b.uth_claim_id,
        a.facprof as claim_type,
        a.svcdate as from_date_of_service,
        a.tsvcdat as to_date_of_service,
        null as total_charge_amount,
        a.pay as total_allowed_amount,
        a.netpay as total_paid_amount,
        dev.fiscal_year_func(a.svcdate) as fiscal_year,
        null as cost_factor_year,
        'ccaes',
        a.enrolid,
        a.claim_id_derv,
        current_date
  from cte a
   join staging_clean.truc_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.claim_id_derv = b.claim_id_src  
    ;

select 'Analyze: ' || current_timestamp as message;

analyze dw_staging.truc_claim_header;

select 'Insert from ccaeo: ' || current_timestamp as message;

--insert from CCAEO
with cte as (
select enrolid, claim_id_derv, 
       min(svcdate) as svcdate,
       min(year) as year,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.ccaeo_etl
  group by enrolid, claim_id_derv 
)
   insert into dw_staging.truc_claim_header
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
select  'truc',
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
        'ccaeo',
        a.enrolid,
        a.claim_id_derv,
        current_date
   from cte a
   join staging_clean.truc_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.claim_id_derv = b.claim_id_src  
    ;

select 'Analyze: ' || current_timestamp as message;

analyze dw_staging.truc_claim_header;

select 'Add provider type: ' || current_timestamp as message;
--- add provider type 

update dw_staging.truc_claim_header a 
   set provider_specialty = to_char(b.stdprov , 'FM9999')
  from staging_clean.truv_ccaef_etl b
 where a.member_id_src::bigint = b.enrolid 
   and a.claim_id_src = b.claim_id_derv 
   and substring(table_id_src,1,2) = 'cc';
  
select 'Analyze: ' || current_timestamp as message;

vacuum analyze dw_staging.truc_claim_header;
   

select 'Truven CCAE Claim Header script completed at ' || current_timestamp as message;
  