
/* ******************************************************************************************************
 *  claim diag load for truven
 * ******************************************************************************************************
*   jw001   || 9/24/2021  || need to put claim sequence number in insert and select statements. 
 * ******************************************************************************************************
 *  gmunoz  || 10/29/2021 || added fiscal year logic with function dev.fiscal_year_func
 * ******************************************************************************************************
 *  gmunoz  || 11/05/2021 || removed the data_year column in insert on all four inserts
 * ******************************************************************************************************
 *  jwozny  || 1/03/2022  || removed icd_type, year, fiscal_year 
 * ******************************************************************************************************
 *  iperez  || 09/28/2022 || added claim id source and member id source to columns
 * ******************************************************************************************************
 *  iperez  || 09/30/2022 || removed claim id source and member id source to columns
 * ******************************************************************************************************
 *  xzhang  || 04/18/2023 || change msclmid to claim_id_derv
 * */

select 'Truven Claim Diag script started' as message;

drop table if exists dw_staging.claim_diag;

--create empty table
create table dw_staging.claim_diag
(like data_warehouse.claim_diag including defaults) 
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

alter table dw_staging.claim_diag owner to uthealth_dev;

vacuum analyze dw_staging.claim_diag;

 -------------------------------- truven commercial inpatient ------
insert into dw_staging.claim_diag 
( 
data_source, 
year,
uth_member_id, 
uth_claim_id, 
from_date_of_service,
diag_cd, 
diag_position, 
poa_src,
icd_version,
claim_id_src,
member_id_src,
load_date
)  					
with diag_agg as (
select  'truv', 
         enrolid,
         claim_id_derv,
         min(svcdate) as svcdate,
		 min(year) as year,
		 min(dxver) as dxver,
		 min(pdx) as pdx,
		 min(dx1) as dx1,
		 min(dx2) as dx2,
		 min(dx3) as dx3,
		 min(dx4) as dx4
   from staging_clean.ccaes_etl  	 
  group by enrolid, claim_id_derv 
  )
 select * from (
  select 
   'truv', 
	 year,
     b.uth_member_id, 
     b.uth_claim_id, 
	 a.svcdate,
     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
	 unnest(array[1,2,3,4,5]) as dx_pos,
     null,
     a.dxver,
     a.claim_id_derv,
     a.enrolid::text,
     current_date
from diag_agg a
join staging_clean.truv_dim_id  b 
  on b.member_id_src = a.enrolid 
 and b.claim_id_src = a.claim_id_derv 
 ) dx where dx_cd is not null
;

analyze dw_staging.claim_diag_1_prt_truv;

 
-------------------------------- truven medicare inpatient ------
insert into dw_staging.claim_diag 
( 
data_source, 
year,
uth_member_id, 
uth_claim_id, 
from_date_of_service,
diag_cd, 
diag_position, 
poa_src,
icd_version,
claim_id_src,
member_id_src,
load_date
)  					
with diag_agg as (
select  'truv', 
         enrolid,
         claim_id_derv,
         min(svcdate) as svcdate,
		 min(year) as year,
		 min(dxver) as dxver,
		 min(pdx) as pdx,
		 min(dx1) as dx1,
		 min(dx2) as dx2,
		 min(dx3) as dx3,
		 min(dx4) as dx4
   from staging_clean.mdcrs_etl 	 
  group by enrolid, claim_id_derv 
  )
  select * from (
	  select 
	   'truv', 
		 year,
	     b.uth_member_id, 
	     b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4,5]) as dx_pos,
	     null,
	     a.dxver,
	     a.claim_id_derv,
	     a.enrolid::text,
	     current_date
	from diag_agg a
	join staging_clean.truv_dim_id  b 
	  on b.member_id_src = a.enrolid 
	 and b.claim_id_src = a.claim_id_derv 
 ) dx where dx_cd is not null
;

analyze dw_staging.claim_diag_1_prt_truv;

--clean up null rows


-------------------------------- truven medicare outpatient -------------------------------------- 4min
insert into dw_staging.claim_diag 
( 
data_source, 
year,
uth_member_id, 
uth_claim_id, 
from_date_of_service,
diag_cd, 
diag_position, 
poa_src,
icd_version,
claim_id_src,
member_id_src,
load_date
)  					
with diag_agg as (
select  'truv', 
         enrolid,
         claim_id_derv,
         min(svcdate) as svcdate,
		 min(year) as year,
		 min(dxver) as dxver,
		 min(dx1) as dx1,
		 min(dx2) as dx2,
		 min(dx3) as dx3,
		 min(dx4) as dx4
   from staging_clean.mdcro_etl  	 
  group by enrolid, claim_id_derv 
  )
  select * from (
	  select 
	   'truv', 
		 year,
	     b.uth_member_id, 
	     b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
	     null,
	     a.dxver,
	     a.claim_id_derv,
	     a.enrolid::text,
	     current_date
	from diag_agg a
	join staging_clean.truv_dim_id  b 
	  on b.member_id_src = a.enrolid 
	 and b.claim_id_src = a.claim_id_derv 
 ) dx where dx_cd is not null
;

analyze dw_staging.claim_diag_1_prt_truv;

-------------------------------- truven commercial outpatient -------------------------------------- 22m
insert into dw_staging.claim_diag 
( 
data_source, 
year,
uth_member_id, 
uth_claim_id, 
from_date_of_service,
diag_cd, 
diag_position, 
poa_src,
icd_version,
claim_id_src,
member_id_src,
load_date
)  					
with diag_agg as (
select  'truv', 
         enrolid,
         claim_id_derv,
         min(svcdate) as svcdate,
		 min(year) as year,
		 min(dxver) as dxver,
		 min(dx1) as dx1,
		 min(dx2) as dx2,
		 min(dx3) as dx3,
		 min(dx4) as dx4
   from staging_clean.ccaeo_etl  	 
  group by enrolid, claim_id_derv 
  )
select * from (
	  select 
	   'truv', 
		 year,
	     b.uth_member_id, 
	     b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array[1,2,3,4]) as dx_pos,
	     null,
	     a.dxver,
	     a.claim_id_derv,
	     a.enrolid::text,
	     current_date
	from diag_agg a
	join staging_clean.truv_dim_id  b 
	  on b.member_id_src = a.enrolid 
	 and b.claim_id_src = a.claim_id_derv 
 ) dx where dx_cd is not null
;

analyze dw_staging.claim_diag_1_prt_truv;

select 'Truven Claim Diag script completed' as message;
