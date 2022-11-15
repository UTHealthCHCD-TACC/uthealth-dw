drop table if exists dev.claim_header_truven;

create table dev.claim_header_truven
(like data_warehouse.claim_header including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
; 

/*
 *  Commercial Inpatient
 */

with cte as (
select enrolid, msclmid, 
       min(svcdate) as svcdate,
       max(tsvcdat) as tsvcdat,
       max(facprof) as facprof,
       sum(netpay) as netpay, 
       sum(pay) as pay
  from dev.ccaes_etl
  group by enrolid, msclmid 
)
   insert into dev.claim_header_truven
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
   join dev.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;
   
analyze dev.claim_header_truven;

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
  from dev.mdcrs_etl
  group by enrolid, msclmid 
)
   insert into dev.claim_header_truven
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
   join dev.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;
   
   vacuum analyze dev.claim_header_truven;
  
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
  from dev.ccaeo_etl
  group by enrolid, msclmid 
)
   insert into dev.claim_header_truven
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
   join dev.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;
    
   vacuum analyze dev.claim_header_truven;
  
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
  from dev.mdcro_etl 
  group by enrolid, msclmid 
)
   insert into dev.claim_header_truven
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
   join dev.truv_dim_id b 
     on a.enrolid = b.member_id_src 
    and a.msclmid = b.claim_id_src  
    ;

   vacuum analyze dev.claim_header_truven;
  