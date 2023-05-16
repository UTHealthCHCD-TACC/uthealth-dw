/*
 * RESIDSTRIBUTING TABLES
 * because of the raw data distribution keys, the etl scripts run slow
 * we make redistributed versions of them to help it run faster so joins work properly
 * 
 * 04/18/2023: Xiaorui changed claim_id_derv to claim_id_derv
 * 
 * 
 */

/*
 * 1) Copy the claims dimension table but distributed on source values
 */

drop table if exists staging_clean.truv_dim_id;
   
create table staging_clean.truv_dim_id as    
select member_id_src::bigint, claim_id_src, 
       uth_claim_id, uth_member_id 
  from data_warehouse.dim_uth_claim_id 
 where data_source = 'truv'
distributed by (member_id_src, claim_id_src);

analyze staging_clean.truv_dim_id;

/*
 * 2) Make temp header table for bill type
 */

---medicare 
drop table if exists staging_clean.truv_mdcrf_etl;

create table staging_clean.truv_mdcrf_etl as
select enrolid::bigint, 
	   claim_id_derv, 
	   max(billtyp) as billtyp,
	   max(stdprov) as stdprov 
  from truven.mdcrf  
group by enrolid, claim_id_derv 
distributed by (enrolid, claim_id_derv);

analyze staging_clean.truv_mdcrf_etl;

---commercial
drop table if exists staging_clean.truv_ccaef_etl;

create table staging_clean.truv_ccaef_etl as
select enrolid::bigint, 
	   claim_id_derv, 
	   max(billtyp) as billtyp,
	   max(stdprov) as stdprov 
  from truven.ccaef 
group by enrolid, claim_id_derv
distributed by (enrolid, claim_id_derv);

analyze staging_clean.truv_ccaef_etl;


/*
 * 3) Build claims tables
 */

----inpatient medicare: mdcrs
drop table if exists staging_clean.mdcrs_etl;

create table staging_clean.mdcrs_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::bigint,
	   claim_id_derv,
	   year,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   dstatus,
	   ntwkprov,
	   paidntwk,
	   admdate,
	   disdate,
	   proc1,
	   procmod,
	   drg,
	   revcode,
	   copay,
	   deduct,
	   coins,
	   cob,
	   qty,
	   dxver,
	   pdx,
	   dx1,
	   dx2,
       dx3,
       dx4,
       stdprov::text
  from truven.mdcrs  
  distributed by (enrolid, claim_id_derv);

 analyze staging_clean.mdcrs_etl;

---inpatient commercial: ccaes
drop table if exists staging_clean.ccaes_etl;

create table staging_clean.ccaes_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::bigint,
	   claim_id_derv,
	   year,
	   dstatus,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   paidntwk,
	   admdate,
	   disdate,
	   proc1,
	   procmod,
	   drg,
	   revcode,
	   copay,
	   deduct,
	   coins,
	   cob,
	   qty,
	   dxver,
	   pdx,
	   dx1,
	   dx2,
       dx3,
       dx4,
       stdprov 
  from truven.ccaes 
  distributed by (enrolid, claim_id_derv);
 
analyze staging_clean.ccaes_etl;

------------

drop table if exists staging_clean.mdcro_etl;

create table staging_clean.mdcro_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::bigint,
	   claim_id_derv,
	   svcdate,
	   year,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   paidntwk,
	   proc1,
	   procmod,
	   revcode,
	   copay,
	   deduct,
	   coins,
	   cob,
	   qty,
	   dxver,
	   dx1,
	   dx2,
       dx3,
       dx4,
       stdprov 
  from truven.mdcro 
 distributed by (enrolid, claim_id_derv);

analyze staging_clean.mdcro_etl;

------------------------------------------------


drop table if exists staging_clean.ccaeo_etl;

create table staging_clean.ccaeo_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::bigint,
	   claim_id_derv,
	   year,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   paidntwk,
	   proc1,
	   procmod,
	   revcode,
	   copay,
	   deduct,
	   coins,
	   cob,
	   qty,
	   dxver,
	   dx1,
	   dx2,
       dx3,
       dx4,
       stdprov 
  from truven.ccaeo
 distributed by (enrolid, claim_id_derv);

analyze staging_clean.ccaeo_etl;

/*check if ccaeo etl'd properly

select count(*) from truven.ccaeo;
select count(*) from staging_clean.ccaeo_etl;
 * 
 */




