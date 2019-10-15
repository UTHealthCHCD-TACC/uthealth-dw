
 ----------------------------------------------------------------------------------------------		
 ---create and populate yearly data table 
 ----------------------------------------------------------------------------------------------		
  create table data_warehouse.medicare_eligibility_year ( 
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
  	dual_eligible_months int
  );
  
 
 
 insert into data_warehouse.medicare_eligibility_year (
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
 ) 
 select b.uth_member_id, a.bene_enrollmt_ref_yr::int, a.covstart::date, a.entlmt_rsn_orig, a.entlmt_rsn_curr,
        a.esrd_ind, a.bene_pta_trmntn_cd, a.bene_ptb_trmntn_cd, a.bene_hi_cvrage_tot_mons::float::int, a.bene_smi_cvrage_tot_mons::float::int,
        a.bene_state_buyin_tot_mons::float::int, a.bene_hmo_cvrage_tot_mons::float::int, a.ptd_plan_cvrg_mons::float::int, a.rds_cvrg_mons::float::int, a.dual_elgbl_mons::float::int
 from medicare.mbsf_abcd_summary a 
   join data_warehouse.dim_member_id_src b 
     on b.member_id_src = a.bene_id 
    and b.data_source = 'mdcr'
;
 

---validate yearly load, should return 0 rows
select * from data_warehouse.member_enrollment_monthly a 
where data_source = 'mdcr' 
and not exists ( select 1 
				 from data_warehouse.medicare_eligibility_year y 
				 where y.uth_member_id = a.uth_member_id);
 

				
----------------------------------------------------------------------------------------------				
---create and populate medicare monthly detail table
----------------------------------------------------------------------------------------------		
 
 
 select b.uth_member_id, a.bene_enrollmt_ref_yr::int as enrollment_year, a.*
 into data_warehouse.medicare_eligibility_month_year
 from medicare.mbsf_abcd_summary a 
  join data_warehouse.dim_member_id_src b 
    on b.member_id_src = a.bene_id 
   and b.data_source = 'mdcr'
;   



alter table data_warehouse.medicare_eligibility_month_year drop column bene_id;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_enrollmt_ref_yr;
alter table data_warehouse.medicare_eligibility_month_year drop column enrl_src;
alter table data_warehouse.medicare_eligibility_month_year drop column sample_group;
alter table data_warehouse.medicare_eligibility_month_year drop column enhanced_five_percent_flag;
alter table data_warehouse.medicare_eligibility_month_year drop column crnt_bic_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column state_code;
alter table data_warehouse.medicare_eligibility_month_year drop column county_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column zip_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column age_at_end_ref_yr;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_birth_dt;
alter table data_warehouse.medicare_eligibility_month_year drop column valid_death_dt_sw;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_death_dt;
alter table data_warehouse.medicare_eligibility_month_year drop column sex_ident_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_race_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column rti_race_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column covstart;
alter table data_warehouse.medicare_eligibility_month_year drop column entlmt_rsn_orig;
alter table data_warehouse.medicare_eligibility_month_year drop column entlmt_rsn_curr;
alter table data_warehouse.medicare_eligibility_month_year drop column esrd_ind;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_pta_trmntn_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_ptb_trmntn_cd;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_hi_cvrage_tot_mons;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_smi_cvrage_tot_mons;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_state_buyin_tot_mons;
alter table data_warehouse.medicare_eligibility_month_year drop column bene_hmo_cvrage_tot_mons;
alter table data_warehouse.medicare_eligibility_month_year drop column ptd_plan_cvrg_mons;
alter table data_warehouse.medicare_eligibility_month_year drop column rds_cvrg_mons;
alter table data_warehouse.medicare_eligibility_month_year drop column dual_elgbl_mons;




select * 
from data_warehouse.medicare_eligibility_month_year;
