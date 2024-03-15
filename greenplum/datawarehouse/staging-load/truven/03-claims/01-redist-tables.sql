/*
 * RESIDSTRIBUTING TABLES
 * because of the raw data distribution keys, the etl scripts run slow
 * we make redistributed versions of them to help it run faster so joins work properly
 * 
 * 04/18/2023: Xiaorui changed msclmid to claim_id_derv
 * 07/19/2023: Adapted for psql (added in timestamps)
 * 				Fixed the say stdprov imports
 * 				(previously added an extraneous .0 after the number if you go straight ::text instead of ::int::text
 * 10/10/2023: XZ fixed typo ('trum' was supposed to be 'truv' in a drop table statement)
 * 
 */

/*
 * 1) Copy the claims dimension table but distributed on source values
 */

select 'Truven table redistribution script started at ' || current_timestamp as message;

select 'Redistributing trum dim_id: ' || current_timestamp as message;

drop table if exists staging_clean.trum_dim_id;
   
create table staging_clean.trum_dim_id as    
select member_id_src::bigint, claim_id_src, 
       uth_claim_id, uth_member_id 
  from data_warehouse.dim_uth_claim_id 
 where data_source = 'trum'
distributed by (member_id_src, claim_id_src);

analyze staging_clean.trum_dim_id;

select 'Redistributing truc dim_id: ' || current_timestamp as message;

drop table if exists staging_clean.truc_dim_id;
   
create table staging_clean.truc_dim_id as    
select member_id_src::bigint, claim_id_src, 
       uth_claim_id, uth_member_id 
  from data_warehouse.dim_uth_claim_id 
 where data_source = 'truc'
distributed by (member_id_src, claim_id_src);

analyze staging_clean.truc_dim_id;

/*
 * 2) Make temp header table for bill type
 */

select 'Redistributing mdcrf: ' || current_timestamp as message;

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

select 'Redistributing ccaef: ' || current_timestamp as message;

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
select 'Redistributing mdcrs: ' || current_timestamp as message;

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
       (stdprov::int)::text
  from truven.mdcrs  
  distributed by (enrolid, claim_id_derv);

 analyze staging_clean.mdcrs_etl;

select 'Redistributing ccaes: ' || current_timestamp as message;

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
       (stdprov::int)::text 
  from truven.ccaes 
  distributed by (enrolid, claim_id_derv);
 
analyze staging_clean.ccaes_etl;

------------

select 'Redistributing mdcro: ' || current_timestamp as message;

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
       (stdprov::int)::text 
  from truven.mdcro 
 distributed by (enrolid, claim_id_derv);

analyze staging_clean.mdcro_etl;

------------------------------------------------
select 'Redistributing ccaeo: ' || current_timestamp as message;

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
       (stdprov::int)::text 
  from truven.ccaeo
 distributed by (enrolid, claim_id_derv);

analyze staging_clean.ccaeo_etl;

select 'Truven table redistribution script completed at ' || current_timestamp as message;

/*check if ccaeo etl'd properly

select count(*) from truven.ccaeo;
select count(*) from staging_clean.ccaeo_etl;
 * 
 */