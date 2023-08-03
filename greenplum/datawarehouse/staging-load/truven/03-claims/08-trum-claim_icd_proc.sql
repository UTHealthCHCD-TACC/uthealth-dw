
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
 *  xzhang  || 07/20/2023 || Split into trum and truc, added table_id_src, rewrote dupe row deletion portion
 * 							 so that we are now retaining the highest position number and mdcrs table, preferentially
 
 
 Note on ICD PROC codes:
 
 ICD PROC codes only live in the 'f' tables (there are 6)
	and 's' tables (PPROC)
 There are no ICD PROC codes in 'o' tables (no PPROC column)
 the PPROC column in 'i' tables are the PROC code that contributed most to the DRG assignment
 	and are NOT ICD PROC codes.
 * */

select 'Truven MDCR Claim ICD Proc script started at ' || current_timestamp as message;

select 'ETL procs codes from mdcrs: ' || current_timestamp as message;
-------------get procs from claims table 

drop table if exists staging_clean.mdcr_proc;

create table staging_clean.mdcr_proc as
select distinct * from 
(
select enrolid, claim_id_derv, year, svcdate, pproc as proc_cd, 1 as proc_pos, dxver,
	'mdcrs'::varchar(5) as table_id_src
  from truven.mdcrs  
 where claim_id_derv is not null and pproc is not null
 ) a
 distributed by (enrolid, claim_id_derv);

analyze staging_clean.mdcr_proc;

select 'ETL procs codes from mdcrf ' || current_timestamp as message;
----- get procs from fac header table

with procs as (
	select 
	enrolid,
	claim_id_derv,
	"year", 
    svcdate ,
    unnest(array[proc1, proc2, proc3, proc4, proc5, proc6]) as proc_cd,
    unnest(array[1,2,3,4,5,6]) as proc_pos,
    dxver,
    'mdcrf' as table_id_src
  from truven.mdcrf 
)
insert into staging_clean.mdcr_proc
select distinct * 
  from procs 
 where claim_id_derv is not null and proc_cd is not null; 

analyze staging_clean.mdcr_proc ;

select 'Create empty trum_claim_icd_proc table: ' || current_timestamp as message;

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

select 'Insert into trum_claim_icd_proc: ' || current_timestamp as message;

--select highest proc pos and join to dim tables

with a as (
select year, enrolid, claim_id_derv, svcdate,
	proc_cd, proc_pos, dxver, table_id_src,
	row_number() over (
		partition by enrolid, claim_id_derv, svcdate, proc_cd 
		order by proc_pos, table_id_src desc) as rn
from staging_clean.mdcr_proc
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
claim_id_src,
table_id_src
)
select 'trum',
		b.uth_member_id,
		b.uth_claim_id,
		a.svcdate ,
		a.proc_cd ,
		a.proc_pos ,
		a.dxver, 
		current_date,
		year,
		a.enrolid::text,
		a.claim_id_derv, 
		a.table_id_src
   from a 
   left join staging_clean.trum_dim_id b
     on a.enrolid = b.member_id_src
    and a.claim_id_derv = b.claim_id_src
	where a.rn = 1
;

/*QA:
 * 
select table_id_src, count(*) from staging_clean.mdcr_proc group by table_id_src;

select table_id_src, count(*) from dw_staging.trum_claim_icd_proc group by table_id_src;
 */

select 'vacuum analyze and cleanup: ' || current_timestamp as message;
vacuum analyze dw_staging.trum_claim_icd_proc;

drop table if exists staging_clean.mdcr_proc;

select 'Truven MDCR Claim ICD Proc script completed at ' || current_timestamp as message;
