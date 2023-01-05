
analyze dw_staging.claim_header;
delete from dw_staging.claim_header where data_source = 'truv';
vacuum analyze dw_staging.claim_header;

with cte as (
select enrolid, msclmid, 
       min(svcdate) as svcdate,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.ccaes_etl
  group by enrolid, msclmid 
)
   insert into dw_staging.claim_header
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
	claim_id_src,
	member_id_src,
	load_date
)
select  'truv',
        extract(year from a.svcdate),
        b.uth_member_id,
        b.uth_claim_id,
        a.facprof,
        a.svcdate,
        a.tsvcdat,
        a.netpay,
        a.pay,
        null as paid_amt,
        dev.fiscal_year_func(a.svcdate) as fiscal_year,
        null as cost_factor_year,
        'ccaes',
        a.enrolid,
        a.msclmid,
        current_date
  from cte a
   join staging_clean.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;
   
analyze dw_staging.claim_header;

/*
 *  Medicare Inpatient
 */
with cte as (
select enrolid, msclmid, 
       min(svcdate) as svcdate,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.mdcrs_etl
  group by enrolid, msclmid 
)
   insert into dw_staging.claim_header
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
	claim_id_src,
	member_id_src,
	load_date
)
select  'truv',
        extract(year from a.svcdate),
        b.uth_member_id,
        b.uth_claim_id,
        a.facprof,
        a.svcdate,
        a.tsvcdat,
        a.netpay,
        a.pay,
        null as paid_amt,
        dev.fiscal_year_func(a.svcdate) as fiscal_year,
        null as cost_factor_year,
        'mdcrs',
        a.enrolid,
        a.msclmid,
        current_date
  from cte a
   join staging_clean.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;
   
vacuum analyze dw_staging.claim_header;
  
  /*
 *  Commercial Outpatient
 */
with cte as (
select enrolid, msclmid, 
       min(svcdate) as svcdate,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.ccaeo_etl
  group by enrolid, msclmid 
)
   insert into dw_staging.claim_header
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
	claim_id_src,
	member_id_src,
	load_date
)
select  'truv',
        extract(year from a.svcdate),
        b.uth_member_id,
        b.uth_claim_id,
        a.facprof,
        a.svcdate,
        a.tsvcdat,
        a.netpay,
        a.pay,
        null as paid_amt,
        dev.fiscal_year_func(a.svcdate) as fiscal_year,
        null as cost_factor_year,
        'ccaeo',
        a.enrolid,
        a.msclmid,
        current_date
   from cte a
   join staging_clean.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;
    
vacuum analyze dw_staging.claim_header;
  
    /*
 *  Medicare Outpatient
 */

with cte as (
select enrolid, msclmid, 
       min(svcdate) as svcdate,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from staging_clean.mdcro_etl 
  group by enrolid, msclmid 
)
   insert into dw_staging.claim_header
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
	claim_id_src,
	member_id_src,
	load_date
)
select  'truv',
        extract(year from a.svcdate),
        b.uth_member_id,
        b.uth_claim_id,
        a.facprof,
        a.svcdate,
        a.tsvcdat,
        a.netpay,
        a.pay,
        null as paid_amt,
        dev.fiscal_year_func(a.svcdate) as fiscal_year,
        null as cost_factor_year,
        'mdcro',
        a.enrolid,
        a.msclmid,
        current_date
   from cte a
   join staging_clean.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;

vacuum analyze dw_staging.claim_header;
  