

 ----------------------------------------------------------------------------------------------		
 ---create and populate yearly data table 
 ----------------------------------------------------------------------------------------------		
 /*

  create table data_warehouse.medicare_enrollment ( 
  	uth_member_id bigint,
  	enrollment_year int,
  	original_coverage_start date,
  	entitlement_reason_original char(1),
  	entitlement_reason_current char(1),
  	esrd_ind char(1),
  	bene_partA_treatment_cd char(1),
  	bene_partB_treatment_cd char(1), 
  	partA_coverage_months int, 
  	partB_coverage_months int,
  	state_buyin_total_months int,
  	hmo_coverage_months int,
  	partD_coverage_months int,
  	rds_coverage_months int,
  	dual_eligible_months int,
  ) WITH (appendonly=true, orientation=column)
distributed by(uth_member_id);
  
 
 
 insert into data_warehouse.medicare_enrollment_year (
			   	uth_member_id,
			  	enrollment_year,
			  	original_coverage_start,
			  	entitlement_reason_original,
			  	entitlement_reason_current,
			  	esrd_ind,
			  	bene_partA_treatment_cd,
			  	bene_partB_treatment_cd, 
			  	partA_coverage_months, ---bene_hi_cov_tot_mons
			  	partB_coverage_months, ---bene_smi_cov_tot_mons
			  	state_buyin_total_months,
			  	hmo_coverage_months,
			  	partD_coverage_months,
			  	rds_coverage_months,
			  	dual_eligible_months
 ) */
 
 

drop table data_warehouse.medicare_enrollment_detail;


 select b.uth_member_id, a.bene_enrollmt_ref_yr::int as enrollment_year, a.covstart::date as original_coverage_start, a.entlmt_rsn_orig as entitlement_reason_original, 
 		a.entlmt_rsn_curr as entitlement_reason_current,
        a.esrd_ind as esrd_indicator, a.bene_pta_trmntn_cd as bene_part_a_treatment_cd, a.bene_ptb_trmntn_cd as bene_part_b_treatment_cd, 
        a.bene_hi_cvrage_tot_mons::float::int as part_a_coverage_months, a.bene_smi_cvrage_tot_mons::float::int as part_b_coverage_months,
        a.bene_state_buyin_tot_mons::float::int as state_buyin_total_months, a.bene_hmo_cvrage_tot_mons::float::int as hmo_coverage_months, 
        a.ptd_plan_cvrg_mons::float::int as part_d_coverage_months, a.rds_cvrg_mons::float::int as rds_coverage_months, a.dual_elgbl_mons::float::int as dual_eligible_months,
        a.* 
      into data_warehouse.medicare_enrollment_detail
 from medicare.mbsf_abcd_summary a 
   join data_warehouse.dim_uth_member_id b
     on b.member_id_src = a.bene_id 
    and b.data_source = 'mdcr'
;
 

---validate yearly load, should return 0 rows
select * 
from data_warehouse.member_enrollment_monthly a 
where data_source = 'mdcr' 
and not exists ( select 1 
				 from data_warehouse.medicare_eligibility_year y 
				 where y.uth_member_id = a.uth_member_id);
 

				


alter table data_warehouse.medicare_enrollment_detail drop column bene_id;
alter table data_warehouse.medicare_enrollment_detail drop column bene_enrollmt_ref_yr;
alter table data_warehouse.medicare_enrollment_detail drop column enrl_src;
alter table data_warehouse.medicare_enrollment_detail drop column sample_group;
alter table data_warehouse.medicare_enrollment_detail drop column enhanced_five_percent_flag;
alter table data_warehouse.medicare_enrollment_detail drop column crnt_bic_cd;
alter table data_warehouse.medicare_enrollment_detail drop column state_code;
alter table data_warehouse.medicare_enrollment_detail drop column county_cd;
alter table data_warehouse.medicare_enrollment_detail drop column zip_cd;
alter table data_warehouse.medicare_enrollment_detail drop column age_at_end_ref_yr;
alter table data_warehouse.medicare_enrollment_detail drop column bene_birth_dt;
alter table data_warehouse.medicare_enrollment_detail drop column valid_death_dt_sw;
alter table data_warehouse.medicare_enrollment_detail drop column bene_death_dt;
alter table data_warehouse.medicare_enrollment_detail drop column sex_ident_cd;
alter table data_warehouse.medicare_enrollment_detail drop column bene_race_cd;
alter table data_warehouse.medicare_enrollment_detail drop column rti_race_cd;
alter table data_warehouse.medicare_enrollment_detail drop column covstart;
alter table data_warehouse.medicare_enrollment_detail drop column entlmt_rsn_orig;
alter table data_warehouse.medicare_enrollment_detail drop column entlmt_rsn_curr;
alter table data_warehouse.medicare_enrollment_detail drop column esrd_ind;
alter table data_warehouse.medicare_enrollment_detail drop column bene_pta_trmntn_cd;
alter table data_warehouse.medicare_enrollment_detail drop column bene_ptb_trmntn_cd;
alter table data_warehouse.medicare_enrollment_detail drop column bene_hi_cvrage_tot_mons;
alter table data_warehouse.medicare_enrollment_detail drop column bene_smi_cvrage_tot_mons;
alter table data_warehouse.medicare_enrollment_detail drop column bene_state_buyin_tot_mons;
alter table data_warehouse.medicare_enrollment_detail drop column bene_hmo_cvrage_tot_mons;
alter table data_warehouse.medicare_enrollment_detail drop column ptd_plan_cvrg_mons;
alter table data_warehouse.medicare_enrollment_detail drop column rds_cvrg_mons;
alter table data_warehouse.medicare_enrollment_detail drop column dual_elgbl_mons;


analyze data_warehouse.medicare_enrollment_detail;


select dbo.set_all_perms();

select count(*)
from data_warehouse.medicare_enrollment_detail


select count(*)
from medicare.mbsf_abcd_summary;
