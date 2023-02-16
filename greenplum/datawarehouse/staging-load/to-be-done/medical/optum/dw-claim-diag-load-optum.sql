
/* ******************************************************************************************************
 *  load claim diag for optum zip and optum dod 
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
insert into dw_staging.claim_diag (
		data_source, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, diag_cd, diag_position, poa_src, icd_version 
) 
select 'optd', b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.diag, a.diag_position,  a.poa, case when trim(icd_flag) = '10' then '0' else '9' end as icd_ver
from optum_dod.diagnostic a 
  join dw_staging.optd_uth_claim_id b  
    on a.member_id_src = b.member_id_src
   and a.clmid = b.claim_id_src 
;


select count(*), data_source from dw_staging.claim_diag cd group by 2;

--------------------------------------------------------------------------------------------------
--- ** OPTZ **
--------------------------------------------------------------------------------------------------
insert into dw_staging.claim_diag (
		data_source, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, diag_cd, diag_position, poa_src, icd_version 
) 
select 'optz',  b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.diag, a.diag_position,  a.poa, case when trim(icd_flag) = '10' then '0' else '9' end as icd_ver
from optum_zip.diagnostic a 
  join dw_staging.optz_uth_claim_id b  
    on a.member_id_src = b.member_id_src
   and a.clmid = b.claim_id_src 
;

--va
analyze dw_staging.claim_diag;

--final check 
select data_source, count(*)
from dw_staging.claim_diag 
group by data_source
order by data_source
;

--------------- END SCRIPT -------