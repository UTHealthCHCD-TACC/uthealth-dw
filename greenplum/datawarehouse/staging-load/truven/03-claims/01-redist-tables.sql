/*
 * RESIDSTRIBUTING TABLES
 * because of the raw data distribution keys, the etl scripts run slow
 * we make redistributed versions of them to help it run faster so joins work properly
 * 
 */


/*
 * 1) Copy the claims dimension table but distributed on source values
 */

drop table if exists dev.truv_dim_id;
   
create table dev.truv_dim_id as    
select member_id_src, claim_id_src, uth_claim_id, uth_member_id 
  from data_warehouse.dim_uth_claim_id 
 where data_source = 'truv'
distributed by (member_id_src, claim_id_src);

analyze dev.truv_dim_id;

/*
 * 2) Make temp header table for bill type
 */

---commercial
drop table if exists dev.truv_ccaef_etl;

create table dev.truv_ccaef_etl as
select enrolid::text, 
	   fachdid::text, 
	   max(billtyp) as billtyp
  from truven.ccaef 
group by enrolid, fachdid
distributed by (enrolid, fachdid);

analyze dev.truv_ccaef_etl;

---medicare 
drop table if exists dev.truv_mdcrf_etl;

create table dev.truv_mdcrf_etl as
select enrolid::text, 
	   fachdid::text, 
	   max(billtyp) as billtyp
  from truven.mdcrf  
group by enrolid, fachdid
distributed by (enrolid, fachdid);

analyze dev.truv_mdcrf_etl;

/*
 * 3) Build claims tables
 */

---inpatient commercial: ccaes

drop table if exists dev.ccaes_etl;

create table dev.ccaes_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::text,
	   msclmid::text,
	   year,
	   dstatus,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   fachdid::text,
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
  distributed by (enrolid, msclmid);
 
analyze dev.ccaes_etl;

----inpatient medicare: mdcrs

drop table if exists dev.mdcrs_etl;

create table dev.mdcrs_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::text,
	   msclmid::text,
	   year,
	   svcdate,
	   tsvcdat,
	   netpay,
	   fachdid::text,
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
       stdprov 
  from truven.mdcrs  
  distributed by (enrolid, msclmid);

 analyze dev.mdcrs_etl;

------------

/*
drop table if exists dev.ccaeo_etl;

create table dev.ccaeo_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::text,
	   msclmid::text,
	   year,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   fachdid::text,
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
 distributed by (enrolid, msclmid);

analyze dev.ccaeo_etl;
*/

drop table if exists dev.ccaeo_etl;

create table dev.ccaeo_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::text,
	   msclmid::text,
	   year,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   fachdid::text,
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
  where "year" between 2011 and 2013
 distributed by (enrolid, msclmid);

analyze dev.ccaeo_etl;

----
insert into dev.ccaeo_etl 
select enrolid::text,
	   msclmid::text,
	   year,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   fachdid::text,
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
  where "year" between 2014 and 2018
	;

analyze dev.ccaeo_etl;

---

insert into dev.ccaeo_etl 
select enrolid::text,
	   msclmid::text,
	   year,
	   svcdate,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   fachdid::text,
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
  where "year" between 2019 and 2021;

analyze dev.ccaeo_etl;

-------------------------------------------------

drop table if exists dev.mdcro_etl;

create table dev.mdcro_etl with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 ) as 
select enrolid::text,
	   msclmid::text,
	   svcdate,
	   year,
	   tsvcdat,
	   netpay,
	   pay,
	   facprof,
	   stdplac,
	   ntwkprov,
	   paidntwk,
	   fachdid::text,
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
 distributed by (enrolid, msclmid);

analyze dev.mdcro_etl;

 
 