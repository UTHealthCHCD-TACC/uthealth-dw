/***************************************
 * This script replaces the old 2019 Medicare MBSF data with the new data imported from SAS
 * 
 * Dependency: script 01 (R Markdown) which imports the raw data from SAS
 */


/********************
 * Stage 1: Exploration
 *******************/

--get column names for both tables and see if we have any discrepancies
select column_name, ordinal_position as pos, udt_name ||
	case when character_maximum_length is not null then '(' || character_maximum_length || ')'
	else '' end as data_type
from information_schema.columns
where table_schema = 'medicare_texas' and table_name = 'mbsf_abcd_summary'
order by ordinal_position;

select column_name, ordinal_position as pos, udt_name ||
	case when character_maximum_length is not null then '(' || character_maximum_length || ')'
	else '' end as data_type
from information_schema.columns
where table_schema = 'dev' and table_name = 'medicare_tx_mbsf_2019_temp'
order by ordinal_position;

--float to text conversion? --it's fine
select bene_enrollmt_ref_yr, bene_enrollmt_ref_yr::text
from dev.medicare_tx_mbsf_2019_temp limit 10;

--date to text conversion? --need to_char
select bene_birth_dt, to_char(bene_birth_dt, 'DDMONYYYY') as bene_birth_dt2
from dev.medicare_tx_mbsf_2019_temp limit 10;

--check if date imported correctly
select bene_birth_dt
from dev.medicare_tx_mbsf_2019_temp where bene_id = 'ggggggggggggyBu';
--yes, it imported correctly

/********************
 * Stage 2: Data type conversion
 *******************/
drop table if exists dev.medicare_tx_mbsf_2019_cleaned;

create table dev.medicare_tx_mbsf_2019_cleaned as
select '2019'::text as year,
	bene_id,
	bene_enrollmt_ref_yr::text as bene_enrollmt_ref_yr,
	enrl_src,
	sample_group,
	enhanced_five_percent_flag,
	crnt_bic_cd,
	state_code,
	county_cd,
	zip_cd,
	state_cnty_fips_cd_01,
	state_cnty_fips_cd_02,
	state_cnty_fips_cd_03,
	state_cnty_fips_cd_04,
	state_cnty_fips_cd_05,
	state_cnty_fips_cd_06,
	state_cnty_fips_cd_07,
	state_cnty_fips_cd_08,
	state_cnty_fips_cd_09,
	state_cnty_fips_cd_10,
	state_cnty_fips_cd_11,
	state_cnty_fips_cd_12,
	age_at_end_ref_yr::text as age_at_end_ref_yr,
	to_char(bene_birth_dt, 'DDMONYYYY') as bene_birth_dt,
	valid_death_dt_sw,
	to_char(bene_death_dt, 'DDMONYYYY') as bene_death_dt,
	sex_ident_cd,
	bene_race_cd,
	rti_race_cd,
	to_char(covstart, 'DDMONYYYY') as covstart,
	entlmt_rsn_orig,
	entlmt_rsn_curr,
	esrd_ind,
	mdcr_status_code_01,
	mdcr_status_code_02,
	mdcr_status_code_03,
	mdcr_status_code_04,
	mdcr_status_code_05,
	mdcr_status_code_06,
	mdcr_status_code_07,
	mdcr_status_code_08,
	mdcr_status_code_09,
	mdcr_status_code_10,
	mdcr_status_code_11,
	mdcr_status_code_12,
	bene_pta_trmntn_cd,
	bene_ptb_trmntn_cd,
	bene_hi_cvrage_tot_mons::text as bene_hi_cvrage_tot_mons,
	bene_smi_cvrage_tot_mons::text as bene_smi_cvrage_tot_mons,
	bene_state_buyin_tot_mons::text as bene_state_buyin_tot_mons,
	bene_hmo_cvrage_tot_mons::text as bene_hmo_cvrage_tot_mons,
	ptd_plan_cvrg_mons::text as ptd_plan_cvrg_mons,
	rds_cvrg_mons::text as rds_cvrg_mons,
	dual_elgbl_mons::text as dual_elgbl_mons,
	mdcr_entlmt_buyin_ind_01,
	mdcr_entlmt_buyin_ind_02,
	mdcr_entlmt_buyin_ind_03,
	mdcr_entlmt_buyin_ind_04,
	mdcr_entlmt_buyin_ind_05,
	mdcr_entlmt_buyin_ind_06,
	mdcr_entlmt_buyin_ind_07,
	mdcr_entlmt_buyin_ind_08,
	mdcr_entlmt_buyin_ind_09,
	mdcr_entlmt_buyin_ind_10,
	mdcr_entlmt_buyin_ind_11,
	mdcr_entlmt_buyin_ind_12,
	hmo_ind_01,
	hmo_ind_02,
	hmo_ind_03,
	hmo_ind_04,
	hmo_ind_05,
	hmo_ind_06,
	hmo_ind_07,
	hmo_ind_08,
	hmo_ind_09,
	hmo_ind_10,
	hmo_ind_11,
	hmo_ind_12,
	ptc_cntrct_id_01,
	ptc_cntrct_id_02,
	ptc_cntrct_id_03,
	ptc_cntrct_id_04,
	ptc_cntrct_id_05,
	ptc_cntrct_id_06,
	ptc_cntrct_id_07,
	ptc_cntrct_id_08,
	ptc_cntrct_id_09,
	ptc_cntrct_id_10,
	ptc_cntrct_id_11,
	ptc_cntrct_id_12,
	ptc_pbp_id_01,
	ptc_pbp_id_02,
	ptc_pbp_id_03,
	ptc_pbp_id_04,
	ptc_pbp_id_05,
	ptc_pbp_id_06,
	ptc_pbp_id_07,
	ptc_pbp_id_08,
	ptc_pbp_id_09,
	ptc_pbp_id_10,
	ptc_pbp_id_11,
	ptc_pbp_id_12,
	ptc_plan_type_cd_01,
	ptc_plan_type_cd_02,
	ptc_plan_type_cd_03,
	ptc_plan_type_cd_04,
	ptc_plan_type_cd_05,
	ptc_plan_type_cd_06,
	ptc_plan_type_cd_07,
	ptc_plan_type_cd_08,
	ptc_plan_type_cd_09,
	ptc_plan_type_cd_10,
	ptc_plan_type_cd_11,
	ptc_plan_type_cd_12,
	ptd_cntrct_id_01,
	ptd_cntrct_id_02,
	ptd_cntrct_id_03,
	ptd_cntrct_id_04,
	ptd_cntrct_id_05,
	ptd_cntrct_id_06,
	ptd_cntrct_id_07,
	ptd_cntrct_id_08,
	ptd_cntrct_id_09,
	ptd_cntrct_id_10,
	ptd_cntrct_id_11,
	ptd_cntrct_id_12,
	ptd_pbp_id_01,
	ptd_pbp_id_02,
	ptd_pbp_id_03,
	ptd_pbp_id_04,
	ptd_pbp_id_05,
	ptd_pbp_id_06,
	ptd_pbp_id_07,
	ptd_pbp_id_08,
	ptd_pbp_id_09,
	ptd_pbp_id_10,
	ptd_pbp_id_11,
	ptd_pbp_id_12,
	ptd_sgmt_id_01,
	ptd_sgmt_id_02,
	ptd_sgmt_id_03,
	ptd_sgmt_id_04,
	ptd_sgmt_id_05,
	ptd_sgmt_id_06,
	ptd_sgmt_id_07,
	ptd_sgmt_id_08,
	ptd_sgmt_id_09,
	ptd_sgmt_id_10,
	ptd_sgmt_id_11,
	ptd_sgmt_id_12,
	rds_ind_01,
	rds_ind_02,
	rds_ind_03,
	rds_ind_04,
	rds_ind_05,
	rds_ind_06,
	rds_ind_07,
	rds_ind_08,
	rds_ind_09,
	rds_ind_10,
	rds_ind_11,
	rds_ind_12,
	dual_stus_cd_01,
	dual_stus_cd_02,
	dual_stus_cd_03,
	dual_stus_cd_04,
	dual_stus_cd_05,
	dual_stus_cd_06,
	dual_stus_cd_07,
	dual_stus_cd_08,
	dual_stus_cd_09,
	dual_stus_cd_10,
	dual_stus_cd_11,
	dual_stus_cd_12,
	cst_shr_grp_cd_01,
	cst_shr_grp_cd_02,
	cst_shr_grp_cd_03,
	cst_shr_grp_cd_04,
	cst_shr_grp_cd_05,
	cst_shr_grp_cd_06,
	cst_shr_grp_cd_07,
	cst_shr_grp_cd_08,
	cst_shr_grp_cd_09,
	cst_shr_grp_cd_10,
	cst_shr_grp_cd_11,
	cst_shr_grp_cd_12
from dev.medicare_tx_mbsf_2019_temp;

select * from dev.medicare_tx_mbsf_2019_cleaned;


/***************
 * Stage 3: Back up old data
 */
drop table if exists backup.medicare_tx_mbsf_2019;

create table backup.medicare_tx_mbsf_2019 as
select *
from medicare_texas.mbsf_abcd_summary
where year = '2019';

/****************
 * Stage 4: delete old data
 */

delete from medicare_texas.mbsf_abcd_summary
where year = '2019';

/****************
 * Stage 5: insert cleaned records into raw data
 */

insert into medicare_texas.mbsf_abcd_summary
select * from dev.medicare_tx_mbsf_2019_cleaned;

/*QA*/
select year, count(*) from medicare_texas.mbsf_abcd_summary
group by 1 order by 1;






