
/* ******************************************************************************************************
 *  load claim icd proc for truven
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001   || 1/3/2022   ||  removed year, fiscal_year, icd_type
 * ****************************************************************************************************** 
 *  iperez  || 09/28/2022 || added claim id source and member id source to columns
 * ******************************************************************************************************
 *  iperez  || 09/30/2022 || removed claim id source and member id source to columns
 * ******************************************************************************************************
 *  xzhang  || 04/18/2023 || change msclmid to claim_id_derv
  * ******************************************************************************************************
 *  xzhang  || 07/20/2023 || Split into trum and truc
 
 
 
 Note on ICD PROC codes:
 
 ICD PROC codes only live in the 'f' tables (there are 6)
	and 's' tables (PPROC)
 There are no ICD PROC codes in 'o' tables (no PPROC column)
 the PPROC column in 'i' tables are the PROC code that contributed most to the DRG assignment
 	and are NOT ICD PROC codes.
 * */

select 'Truven CCAE Claim ICD Proc script started at ' || current_timestamp as message;

drop table if exists dw_staging.trum_claim_icd_proc;

--create empty table
create table dw_staging.trum_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

vacuum analyze dw_staging.trum_claim_icd_proc;

select 'ETL procs codes from mdcrs: ' || current_timestamp as message;
-------------get procs from claims table 

drop table if exists staging_clean.truven_proc;

create table staging_clean.truven_proc as
select distinct * from 
(
select enrolid, claim_id_derv, year, svcdate, pproc as proc_cd, 1 as proc_pos, dxver 
  from truven.mdcrs  
 where pproc is not null
 ) a
 distributed by (enrolid, claim_id_derv);

analyze staging_clean.truven_proc;

select 'ETL procs codes from mdcrf ' || current_timestamp as message;
----- get procs from fac header table

drop table if exists staging_clean.mdcrf_proc;

create table staging_clean.mdcrf_proc as 
with procs as (
	select 
	enrolid,
	claim_id_derv,
	"year", 
    svcdate ,
    unnest(array[proc1, proc2, proc3, proc4, proc5, proc6])  as proc_cd,
    unnest(array[1,2,3,4,5,6]) as proc_pos,
    dxver
  from truven.mdcrf 
)
select distinct * 
  from procs 
 where proc_cd is not null
 distributed by (enrolid, claim_id_derv ); 

analyze staging_clean.mdcrf_proc ;

-------------------


with all_procs as 
(
select * from staging_clean.truven_proc
union all 
select * from staging_clean.mdcrf_proc
),
dis_all as 
(
select distinct * from all_procs
)
insert into dw_staging.trum_claim_icd_proc 
(
data_source,
uth_member_id,
uth_claim_id,
from_date_of_service,
proc_cd,
proc_position,
icd_version,
load_date,
"year",
member_id_src,
claim_id_src
)
select 'truv',
		b.uth_member_id,
		b.uth_claim_id,
		a.svcdate ,
		a.proc_cd ,
		a.proc_pos ,
		a.dxver, 
		current_date,
		year,
		a.enrolid::text,
		a.claim_id_derv
   from dis_all a 
   join staging_clean.truv_dim_id b
     on a.enrolid = b.member_id_src
    and a.claim_id_derv = b.claim_id_src 
;

vacuum analyze dw_staging.trum_claim_icd_proc_1_prt_truv;

--- get rid of duplicate procs 

delete  
from dw_staging.trum_claim_icd_proc_1_prt_truv a 
where exists 
(
select 1 from dw_staging.trum_claim_icd_proc_1_prt_truv b
where a.uth_member_id = b.uth_member_id 
and a.uth_claim_id = b.uth_claim_id 
and a.proc_cd = b.proc_cd 
and a.from_date_of_service = b.from_date_of_service 
and a.proc_position > b.proc_position 
);

vacuum analyze dw_staging.trum_claim_icd_proc_1_prt_truv;

drop table if exists staging_clean.mdcrf_proc;
drop table if exists staging_clean.ccaef_proc ;
drop table if exists staging_clean.truven_proc;

select 'Truven Claim ICD Proc script completed' as message;
