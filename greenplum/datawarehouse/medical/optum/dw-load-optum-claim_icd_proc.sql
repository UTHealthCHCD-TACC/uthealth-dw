
/* ******************************************************************************************************
 *  load claim icd proc for optum zip and optum dod 
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 9/20/2021 || add comment block. migrate to dw_staging load 
 * ****************************************************************************************************** 
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ****************************************************************************************************** 
 * */


--------------- BEGIN SCRIPT -------

drop table if exists dw_staging.claim_icd_proc;

---create a copy of production data warehouse table 
create table dw_staging.claim_icd_proc
with (appendonly=true, orientation=column) as 
select data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
       from_date_of_service, proc_cd, proc_position, icd_type, fiscal_year
from data_warehouse.claim_icd_proc
where data_source not in ('optd','optz')
distributed by (uth_member_id) 
;

vacuum analyze dw_staging.claim_icd_proc;

--------------------------------------------------------------------------------------------------
--- ** OPTD **
--------------------------------------------------------------------------------------------------
insert into dw_staging.claim_icd_proc (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, proc_cd, proc_position, icd_type, fiscal_year
) 
select 'optd', extract(year from a.fst_dt) as yr, b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.proc, a.proc_position, a.icd_flag, dev.fiscal_year_func(a.fst_dt)
from optum_dod."procedure" a 
  join dw_staging.optd_uth_claim_id b  
    on a.patid::text = b.member_id_src
   and a.clmid = b.claim_id_src 
;




--------------------------------------------------------------------------------------------------
--- ** OPTZ **
--------------------------------------------------------------------------------------------------
insert into dw_staging.claim_icd_proc (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, proc_cd, proc_position, icd_type, fiscal_year
) 
select 'optz', extract(year from a.fst_dt) as yr, b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.proc, a.proc_position, a.icd_flag, dev.fiscal_year_func(a.fst_dt)
from optum_zip."procedure" a 
  join dw_staging.optz_uth_claim_id b  
    on a.patid::text = b.member_id_src
   and a.clmid = b.claim_id_src 
;

--va
vacuum analyze dw_staging.claim_icd_proc;

--final check 
select data_source, year, count(*)
from dw_staging.claim_icd_proc
group by data_source, year 
order by data_source, year 
;

----cleanup 
drop table if exists dw_staging.optz_uth_claim_id; 
drop table if exists dw_staging.optd_uth_claim_id;



--------------- END SCRIPT -------