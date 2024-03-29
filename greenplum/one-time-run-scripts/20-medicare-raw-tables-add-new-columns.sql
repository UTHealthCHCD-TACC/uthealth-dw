alter table medicare_texas.bcarrier_line_k add column line_adjust_grp_cd varchar;
alter table medicare_texas.bcarrier_line_k add column line_adjust_rsn_cd varchar; 
alter table medicare_texas.bcarrier_line_k add column line_ra_rmrk_cd varchar; 
alter table medicare_texas.dme_line_k add column dmerc_oxgn_equip_initl_dt varchar; 
alter table medicare_texas.dme_line_k add column dmerc_oxgn_initl_dt_cd varchar; 
alter table medicare_texas.dme_line_k add column dmerc_oxgn_equip_prvs_dt varchar; 
alter table medicare_texas.hha_base_claims_k add column clm_adjust_grp_cd varchar; 
alter table medicare_texas.hha_base_claims_k add column clm_adjust_rsn_cd varchar; 
alter table medicare_texas.hha_base_claims_k add column clm_prcr_vrsn_cd varchar; 
alter table medicare_texas.hha_base_claims_k add column prvdr_full_ccn_num varchar; 
alter table medicare_texas.hha_revenue_center_k add column rev_cntr_adjust_grp_cd varchar; 
alter table medicare_texas.hha_revenue_center_k add column rev_cntr_adjust_rsn_cd varchar; 
alter table medicare_texas.hha_revenue_center_k add column rev_cntr_ra_rmrk_cd varchar; 
alter table medicare_texas.hospice_base_claims_k add column clm_adjust_grp_cd varchar; 
alter table medicare_texas.hospice_base_claims_k add column clm_adjust_rsn_cd varchar; 
alter table medicare_texas.hospice_base_claims_k add column clm_prcr_vrsn_cd varchar; 
alter table medicare_texas.hospice_base_claims_k add column prvdr_full_ccn_num varchar; 
alter table medicare_texas.hospice_revenue_center_k add column rev_cntr_adjust_grp_cd varchar; 
alter table medicare_texas.hospice_revenue_center_k add column rev_cntr_adjust_rsn_cd varchar; 
alter table medicare_texas.hospice_revenue_center_k add column rev_cntr_ra_rmrk_cd varchar; 
alter table medicare_texas.inpatient_base_claims_k add column owng_prvdr_tin_num varchar; 
alter table medicare_texas.inpatient_base_claims_k add column clm_adjust_grp_cd varchar; 
alter table medicare_texas.inpatient_base_claims_k add column clm_adjust_rsn_cd varchar; 
alter table medicare_texas.inpatient_base_claims_k add column clm_prcr_vrsn_cd varchar; 
alter table medicare_texas.inpatient_base_claims_k add column ms_drg_grpr_vrsn_cd varchar; 
alter table medicare_texas.inpatient_base_claims_k add column prvdr_full_ccn_num varchar; 
alter table medicare_texas.inpatient_revenue_center_k add column rev_cntr_adjust_grp_cd varchar; 
alter table medicare_texas.inpatient_revenue_center_k add column rev_cntr_adjust_rsn_cd varchar; 
alter table medicare_texas.outpatient_base_claims_k add column owng_prvdr_tin_num varchar; 
alter table medicare_texas.outpatient_base_claims_k add column clm_adjust_grp_cd varchar; 
alter table medicare_texas.outpatient_base_claims_k add column clm_adjust_rsn_cd varchar; 
alter table medicare_texas.outpatient_base_claims_k add column clm_prcr_vrsn_cd varchar; 
alter table medicare_texas.outpatient_base_claims_k add column prvdr_full_ccn_num varchar; 
alter table medicare_texas.outpatient_base_claims_k add column esrd_trtmt_chs_ind_cd varchar; 
alter table medicare_texas.outpatient_base_claims_k add column clm_op_pps_ind varchar; 
alter table medicare_texas.outpatient_revenue_center_k add column rev_cntr_cra_tpnies_amt varchar; 
alter table medicare_texas.outpatient_revenue_center_k add column rev_cntr_thrpy_rdctn_amt varchar; 
alter table medicare_texas.outpatient_revenue_center_k add column rev_cntr_adjust_grp_cd varchar; 
alter table medicare_texas.outpatient_revenue_center_k add column rev_cntr_adjust_rsn_cd varchar; 
alter table medicare_texas.outpatient_revenue_center_k add column rev_cntr_ra_rmrk_cd varchar; 
alter table medicare_texas.snf_base_claims_k add column clm_adjust_grp_cd varchar; 
alter table medicare_texas.snf_base_claims_k add column clm_adjust_rsn_cd varchar; 
alter table medicare_texas.snf_base_claims_k add column clm_prcr_vrsn_cd varchar; 
alter table medicare_texas.snf_base_claims_k add column ms_drg_grpr_vrsn_cd varchar; 
alter table medicare_texas.snf_base_claims_k add column prvdr_full_ccn_num varchar; 
alter table medicare_texas.snf_revenue_center_k add column rev_cntr_adjust_grp_cd varchar; 
alter table medicare_texas.snf_revenue_center_k add column rev_cntr_adjust_rsn_cd varchar;