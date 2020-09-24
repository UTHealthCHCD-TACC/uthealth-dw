
 

drop table data_warehouse.medicare_supplemental_enrollment; --supplemental;

create table data_warehouse.medicare_supplemental_enrollment 
with(appendonly=true,orientation=column,compresstype=zlib)
as 
 select b.uth_member_id, a.bene_enrollmt_ref_yr::int as enrollment_year, a.covstart::date as original_coverage_start, a.entlmt_rsn_orig as entitlement_reason_original, 
 		a.entlmt_rsn_curr as entitlement_reason_current,
        a.esrd_ind as esrd_indicator, a.bene_pta_trmntn_cd as bene_part_a_treatment_cd, a.bene_ptb_trmntn_cd as bene_part_b_treatment_cd, 
        a.bene_hi_cvrage_tot_mons::float::int as part_a_coverage_months, a.bene_smi_cvrage_tot_mons::float::int as part_b_coverage_months,
        a.bene_state_buyin_tot_mons::float::int as state_buyin_total_months, a.bene_hmo_cvrage_tot_mons::float::int as hmo_coverage_months, 
        a.ptd_plan_cvrg_mons::float::int as part_d_coverage_months, a.rds_cvrg_mons::float::int as rds_coverage_months, a.dual_elgbl_mons::float::int as dual_eligible_months,
        a.* 
 from medicare.mbsf_abcd_summary a 
   join data_warehouse.dim_uth_member_id b
     on b.member_id_src = a.bene_id 
    and b.data_source = 'mdcr'
distributed by(uth_member_id );
 
--remove unneeded columns			


alter table data_warehouse.medicare_supplemental_enrollment drop column bene_id;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_enrollmt_ref_yr;
alter table data_warehouse.medicare_supplemental_enrollment drop column enrl_src;
alter table data_warehouse.medicare_supplemental_enrollment drop column sample_group;
alter table data_warehouse.medicare_supplemental_enrollment drop column enhanced_five_percent_flag;
alter table data_warehouse.medicare_supplemental_enrollment drop column crnt_bic_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column state_code;
alter table data_warehouse.medicare_supplemental_enrollment drop column county_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column zip_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column age_at_end_ref_yr;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_birth_dt;
alter table data_warehouse.medicare_supplemental_enrollment drop column valid_death_dt_sw;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_death_dt;
alter table data_warehouse.medicare_supplemental_enrollment drop column sex_ident_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_race_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column rti_race_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column covstart;
alter table data_warehouse.medicare_supplemental_enrollment drop column entlmt_rsn_orig;
alter table data_warehouse.medicare_supplemental_enrollment drop column entlmt_rsn_curr;
alter table data_warehouse.medicare_supplemental_enrollment drop column esrd_ind;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_pta_trmntn_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_ptb_trmntn_cd;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_hi_cvrage_tot_mons;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_smi_cvrage_tot_mons;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_state_buyin_tot_mons;
alter table data_warehouse.medicare_supplemental_enrollment drop column bene_hmo_cvrage_tot_mons;
alter table data_warehouse.medicare_supplemental_enrollment drop column ptd_plan_cvrg_mons;
alter table data_warehouse.medicare_supplemental_enrollment drop column rds_cvrg_mons;
alter table data_warehouse.medicare_supplemental_enrollment drop column dual_elgbl_mons;


vacuum analyze data_warehouse.medicare_supplemental_enrollment;


select count(*)
from data_warehouse.medicare_supplemental_enrollment


select count(*)
from medicare.mbsf_abcd_summary;


select * 
from data_warehouse.medicare_supplemental_enrollment
