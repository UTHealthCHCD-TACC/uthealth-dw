
/* ******************************************************************************************************
 *  claim diag load for truven medicare
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
 * ******************************************************************************************************
 *  xzhang  || 07/20/2023 || Split into trum and truc, added table_id_src
 * ******************************************************************************************************
 *  xzhang  || 10/17/2023 || Converted diag_pos to text and indicated which one is primary
 * */

select 'Truven MDCR Claim Diag script started at ' || current_timestamp as message;

drop table if exists dw_staging.trum_claim_diag;

--create empty table
create table dw_staging.trum_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

vacuum analyze dw_staging.trum_claim_diag;

select 'Inserting from mdcrs: ' || current_timestamp as message;
 
-------------------------------- truven medicare inpatient ------
insert into dw_staging.trum_claim_diag 
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
load_date,
table_id_src
)  					
with diag_agg as (
select  'trum', 
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
	   'trum', 
		 year,
	     b.uth_member_id, 
	     b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array['P','1','2','3','4']) as dx_pos,
	     null,
	     a.dxver,
	     a.claim_id_derv,
	     a.enrolid::text,
	     current_date,
	     'mdcrs' as table_id_src
	from diag_agg a
	join staging_clean.trum_dim_id  b 
	  on b.member_id_src = a.enrolid 
	 and b.claim_id_src = a.claim_id_derv 
 ) dx where dx_cd is not null
;

select 'Analyze: ' || current_timestamp as message;

analyze dw_staging.trum_claim_diag;


select 'Inserting from mdcro: ' || current_timestamp as message;


-------------------------------- truven medicare outpatient -------------------------------------- 4min
insert into dw_staging.trum_claim_diag 
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
load_date,
table_id_src
)  					
with diag_agg as (
select  'trum', 
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
	   'trum', 
		 year,
	     b.uth_member_id, 
	     b.uth_claim_id, 
		 a.svcdate,
	     unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx_cd,
		 unnest(array['1','2','3','4']) as dx_pos,
	     null,
	     a.dxver,
	     a.claim_id_derv,
	     a.enrolid::text,
	     current_date,
	     'mdcro' as table_id_src
	from diag_agg a
	join staging_clean.trum_dim_id  b 
	  on b.member_id_src = a.enrolid 
	 and b.claim_id_src = a.claim_id_derv 
 ) dx where dx_cd is not null
;

select 'Analyze: ' || current_timestamp as message;

analyze dw_staging.trum_claim_diag;

select 'Truven MDCR Claim Diag script completed at ' || current_timestamp as message;
