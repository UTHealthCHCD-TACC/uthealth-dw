
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



--------------------------------------------------------------------------------------------------
--- ** OPTD **
--------------------------------------------------------------------------------------------------
insert into dw_staging.claim_icd_proc (
		data_source, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, proc_cd, proc_position, icd_version 
) 
select 'optd',  b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.proc, a.proc_position, case when trim(icd_flag) = '10' then '0' else '9' end as icd_ver
from optum_dod."procedure" a 
  join dw_staging.optd_uth_claim_id b  
    on a.patid::text = b.member_id_src
   and a.clmid = b.claim_id_src 
;




--------------------------------------------------------------------------------------------------
--- ** OPTZ **
--------------------------------------------------------------------------------------------------
insert into dw_staging.claim_icd_proc (
		data_source, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, proc_cd, proc_position, icd_version 
) 
select 'optz',  b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.proc, a.proc_position, case when trim(icd_flag) = '10' then '0' else '9' end as icd_ver
from optum_zip."procedure" a 
  join dw_staging.optz_uth_claim_id b  
    on a.patid::text = b.member_id_src
   and a.clmid = b.claim_id_src 
;

--va
analyze dw_staging.claim_icd_proc;

--final check 
select data_source,count(*)
from dw_staging.claim_icd_proc
group by data_source
order by data_source
;

----cleanup 
drop table if exists dw_staging.optz_uth_claim_id; 
drop table if exists dw_staging.optd_uth_claim_id;



--------------- END SCRIPT -------