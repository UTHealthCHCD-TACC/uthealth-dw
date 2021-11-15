
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

---create a copy of production data warehouse table 
create table dw_staging.claim_diag
with (appendonly=true, orientation=column) as 
select data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
       from_date_of_service, diag_cd, diag_position, icd_type, poa_src, fiscal_year
from data_warehouse.claim_diag
where data_source not in ('optd','optz')
distributed by (uth_member_id) 
;

vacuum analyze dw_staging.claim_diag; 

--------------------------------------------------------------------------------------------------
--- ** OPTD **
--------------------------------------------------------------------------------------------------
insert into dw_staging.claim_diag (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, diag_cd, diag_position, icd_type, poa_src, fiscal_year
) 
select 'optd', extract(year from a.fst_dt) as yr, b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.poa,
       dev.fiscal_year_func(a.fst_dt)
from optum_dod.diagnostic a 
  join dw_staging.optd_uth_claim_id b  
    on a.member_id_src = b.member_id_src
   and a.clmid = b.claim_id_src 
;


--------------------------------------------------------------------------------------------------
--- ** OPTZ **
--------------------------------------------------------------------------------------------------
insert into dw_staging.claim_diag (
		data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, 
        from_date_of_service, diag_cd, diag_position, icd_type, poa_src, fiscal_year
) 
select 'optz', extract(year from a.fst_dt) as yr, b.uth_member_id, b.uth_claim_id, 1 as clmseq, 
       a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.poa,
       dev.fiscal_year_func(a.fst_dt)
from optum_zip.diagnostic a 
  join dw_staging.optz_uth_claim_id b  
    on a.member_id_src = b.member_id_src
   and a.clmid = b.claim_id_src 
;

--va
vacuum analyze dw_staging.claim_diag;

--final check 
select data_source, year, count(*)
from dw_staging.claim_diag 
group by data_source, year 
order by data_source, year 
;

--------------- END SCRIPT -------