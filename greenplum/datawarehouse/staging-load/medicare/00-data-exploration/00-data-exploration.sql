/*************
 * This SQL script holds tiny chunks of code meant to explore the Medicare National and Medicare Texas datasets
 */

/**********
 * Question: How recent is this data?
 * 
 * As of 5-23-23, most recent data goes to December 2020
 */

select month_year_id, count(*)
from data_warehouse.member_enrollment_monthly
where data_source = 'mcrn'
group by month_year_id
order by month_year_id desc
limit 10;

--dw is current to december 2020

select to_date(covstart, 'DDMonYYYY'), count(*)
from medicare_national.mbsf_abcd_summary
group by to_date(covstart, 'DDMonYYYY')
order by to_date(covstart, 'DDMonYYYY') desc
limit 10;
--medicare national is current to dec 2020, so dw is current

select to_date(covstart, 'DDMonYYYY'), count(*)
from medicare_texas.mbsf_abcd_summary
group by to_date(covstart, 'DDMonYYYY')
order by to_date(covstart, 'DDMonYYYY') desc
limit 10;
--same for medicare texas

/***************
 * Just caught an error in the medicare texas dim_uth_rx_claim_id script
 * 
 * Are the medicare texas rx claims ok?
 */

select * from data_warehouse.pharmacy_claims_1_prt_mcrt; --claims exist
select * from data_warehouse.pharmacy_claims_1_prt_mcrt where uth_rx_claim_id is null; --nothing here

select count(distinct pde_id) from medicare_texas.pde_file; --564725466

select count(distinct uth_rx_claim_id) from data_warehouse.pharmacy_claims_1_prt_mcrt; --471564058

--yeah we's missing like several million files

/********************
 * is postgresql case sensitive?
 */

select * from medicare_texas.mbsf_abcd_summary where bene_id = 'ggggggggangnuuw';

2014	ggggggggangnuuw
2015	ggggggggangnuuw
2016	ggggggggangnuuw
2017	ggggggggangnuuw
2018	ggggggggangnuuw
2019	ggggggggangnuuw
2020	ggggggggangnuuw

select * from medicare_texas.mbsf_abcd_summary where bene_id = 'Ggggggggangnuuw';

--nothing!

select * from medicare_texas.mbsf_abcd_summary;


/*****************
 * what's up w/ the death date eh
 * 
 * --oh what's up is that the dob was on the same line of code as
 * something unrelated
 * 
 * who codes like that
 */

select bene_birth_dt, bene_death_dt from medicare_texas.mbsf_abcd_summary;

/*****************
 * Did I clean sex right
 * 
 * --turns out it won't matter, there are like 2 people with 'U' and they're uniformly unknown gender code
 * 
 * --no one in mcrn has gender_cd = 'U'
 */

select * from dw_staging.mcrt_member_enrollment_monthly where gender_cd = 'U';
select * from dw_staging.mcrn_member_enrollment_monthly where gender_cd = 'U';

/**************
 * Isrrael found issues where there's a some number differences for A and B plans
 * 
 * so apparently plan types are based on buying codes
C	AB
A	A
3	AB
0	
B	B
1	A
2	B
 */

/********************
 * Are there null years?
 * 
 * --no
 */

select * from medicare_texas.mbsf_abcd_summary where year is null;

/***********************
 * Is Medicare enrollment one row = 1 person per year?
 */

select bene_id, bene_enrollmt_ref_yr, count(*)
from medicare_texas.mbsf_abcd_summary
group by bene_id, bene_enrollmt_ref_yr
having count(*) > 1;
--nothing!

select bene_id, bene_enrollmt_ref_yr, count(*)
from medicare_national.mbsf_abcd_summary
group by bene_id, bene_enrollmt_ref_yr
having count(*) > 1;
--also nothing

/******************************
 * How well do Medicare claim table pairs match up?
 */

--DME
select count(*) from ( select bene_id, clm_id from medicare_texas.dme_claims_k group by bene_id, clm_id) t;
--27049702
select count(*) from ( select bene_id, clm_id from medicare_texas.dme_line_k group by bene_id, clm_id) t;
--27049702

--they are exact. Try another one

--inpatient
select count(*) from ( select bene_id, clm_id from medicare_texas.inpatient_base_claims_k group by bene_id, clm_id) t;
--5577182
select count(*) from ( select bene_id, clm_id from medicare_texas.inpatient_revenue_center_k group by bene_id, clm_id) t;
--5577182

--good enough for me

/*********************
 * Check behavior of these columns:
 * 
 * clm_bene_pd_amt
line_bene_ptb_ddctbl_amt
line_coinsrnc_amt
 */

select bene_id, clm_id, line_sbmtd_chrg_amt, line_alowd_chrg_amt, 
	line_bene_pmt_amt, line_bene_ptb_ddctbl_amt, line_coinsrnc_amt from medicare_texas.bcarrier_line_k;

select bene_id, clm_id, count(*) from medicare_texas.inpatient_revenue_center_k
group by clm_id, bene_id
having count(*) > 2;

gggggggjynjwawn	gggggnnyfwuwyyg	17
ggggggByjwfByyy	gggggnAuyuAyyag	15
ggggggggnjnfAnu	gggggnayygwBafa	20

--Case study 1: gggggggjynjwawn	gggggnnyfwuwyyg	17
select bene_id, clm_id, clm_line_num::int, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt, rev_cntr, REV_CNTR_DDCTBL_COINSRNC_CD, REV_CNTR_PRCNG_IND_CD,
 REV_CNTR_UNIT_CNT, REV_CNTR_RATE_AMT, REV_CNTR_PRCNG_IND_CD, RC_MODEL_REIMBRSMT_AMT, THRPY_CAP_IND_CD1, THRPY_CAP_IND_CD2
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg'
order by clm_line_num::int;
--all under tot_chrg_amt, nothing under not covered
--REV CENTER 001 = SUM
--total is 15172.66, pharmacy charges are 651.00 (rev code = 250)

select *
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg'
order by clm_line_num::int;

select 15172.66 - 

select bene_id, clm_id, sum(rev_cntr_tot_chrg_amt::float), sum(rev_cntr_ncvrd_chrg_amt::float)
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg'
group by 1, 2;
--30,345.32

select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt,
	clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int) as real_total_cost,
	NCH_PRMRY_PYR_CLM_PD_AMT, NCH_BENE_IP_DDCTBL_AMT, NCH_PROFNL_CMPNT_CHRG_AMT, NCH_IP_NCVRD_CHRG_AMT, NCH_IP_TOT_DDCTN_AMT,
	CLM_TOT_PPS_CPTL_AMT, CLM_PPS_CPTL_FSP_AMT, CLM_PPS_CPTL_OUTLIER_AMT, CLM_PPS_CPTL_DSPRPRTNT_SHR_AMT, CLM_PPS_CPTL_IME_AMT,
	CLM_PPS_CPTL_EXCPTN_AMT, CLM_PPS_OLD_CPTL_HLD_HRMLS_AMT, NCH_DRG_OUTLIER_APRVD_PMT_AMT, DSH_OP_CLM_VAL_AMT, CLM_IP_LOW_VOL_PMT_AMT,
	CLM_BASE_OPRTG_DRG_AMT, CLM_UNCOMPD_CARE_PMT_AMT, CLM_BNDLD_ADJSTMT_PMT_AMT, CLM_VBP_ADJSTMT_PMT_AMT, CLM_VBP_ADJSTMT_PMT_AMT,
	CLM_HRR_ADJSTMT_PMT_AMT, EHR_PYMT_ADJSTMT_AMT, PPS_STD_VAL_PYMT_AMT, FINL_STD_AMT, CLM_FULL_STD_PYMT_AMT, CLM_SS_OUTLIER_STD_PYMT_AMT,
	CLM_SITE_NTRL_PYMT_CST_AMT, CLM_SITE_NTRL_PYMT_IPPS_AMT, CLM_MODEL_REIMBRSMT_AMT, LTCH_DSCHRG_PYMT_ADJSTMT_AMT
from medicare_texas.inpatient_base_claims_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg';
gggggggjynjwawn	gggggnnyfwuwyyg	12087.89	15172.66	0.00	8	15172.66

select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt,
	clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int) as real_total_cost,
	NCH_IP_NCVRD_CHRG_AMT, NCH_IP_TOT_DDCTN_AMT
	(clm_pmt_amt::float / clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int)) as proportion_covered
from medicare_texas.inpatient_base_claims_k;


select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt,
	clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int) as real_total_cost,
	NCH_PRMRY_PYR_CLM_PD_AMT, NCH_BENE_IP_DDCTBL_AMT, NCH_PROFNL_CMPNT_CHRG_AMT, NCH_IP_NCVRD_CHRG_AMT, NCH_IP_TOT_DDCTN_AMT,
	CLM_TOT_PPS_CPTL_AMT, CLM_PPS_CPTL_FSP_AMT, CLM_PPS_CPTL_OUTLIER_AMT, CLM_PPS_CPTL_DSPRPRTNT_SHR_AMT, CLM_PPS_CPTL_IME_AMT,
	CLM_PPS_CPTL_EXCPTN_AMT, CLM_PPS_OLD_CPTL_HLD_HRMLS_AMT, NCH_DRG_OUTLIER_APRVD_PMT_AMT, DSH_OP_CLM_VAL_AMT, CLM_IP_LOW_VOL_PMT_AMT,
	CLM_BASE_OPRTG_DRG_AMT, CLM_UNCOMPD_CARE_PMT_AMT, CLM_BNDLD_ADJSTMT_PMT_AMT, CLM_VBP_ADJSTMT_PMT_AMT, CLM_VBP_ADJSTMT_PMT_AMT,
	CLM_HRR_ADJSTMT_PMT_AMT, EHR_PYMT_ADJSTMT_AMT, PPS_STD_VAL_PYMT_AMT, FINL_STD_AMT, CLM_FULL_STD_PYMT_AMT, CLM_SS_OUTLIER_STD_PYMT_AMT,
	CLM_SITE_NTRL_PYMT_CST_AMT, CLM_SITE_NTRL_PYMT_IPPS_AMT, CLM_MODEL_REIMBRSMT_AMT, LTCH_DSCHRG_PYMT_ADJSTMT_AMT
from medicare_texas.inpatient_base_claims_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg';

select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt,
	clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int) as real_total_cost,
	NCH_PRMRY_PYR_CLM_PD_AMT, NCH_BENE_IP_DDCTBL_AMT, NCH_PROFNL_CMPNT_CHRG_AMT, NCH_IP_NCVRD_CHRG_AMT, NCH_IP_TOT_DDCTN_AMT,
	CLM_TOT_PPS_CPTL_AMT, CLM_PPS_CPTL_FSP_AMT, CLM_PPS_CPTL_OUTLIER_AMT, CLM_PPS_CPTL_DSPRPRTNT_SHR_AMT, CLM_PPS_CPTL_IME_AMT,
	CLM_PPS_CPTL_EXCPTN_AMT, CLM_PPS_OLD_CPTL_HLD_HRMLS_AMT, NCH_DRG_OUTLIER_APRVD_PMT_AMT, DSH_OP_CLM_VAL_AMT, CLM_IP_LOW_VOL_PMT_AMT,
	CLM_BASE_OPRTG_DRG_AMT, CLM_UNCOMPD_CARE_PMT_AMT, CLM_BNDLD_ADJSTMT_PMT_AMT, CLM_VBP_ADJSTMT_PMT_AMT, CLM_VBP_ADJSTMT_PMT_AMT,
	CLM_HRR_ADJSTMT_PMT_AMT, EHR_PYMT_ADJSTMT_AMT, PPS_STD_VAL_PYMT_AMT, FINL_STD_AMT, CLM_FULL_STD_PYMT_AMT, CLM_SS_OUTLIER_STD_PYMT_AMT,
	CLM_SITE_NTRL_PYMT_CST_AMT, CLM_SITE_NTRL_PYMT_IPPS_AMT, CLM_MODEL_REIMBRSMT_AMT, LTCH_DSCHRG_PYMT_ADJSTMT_AMT
from medicare_texas.inpatient_base_claims_k
where (NCH_BENE_PTA_COINSRNC_LBLTY_AM is not null and NCH_BENE_PTA_COINSRNC_LBLTY_AM::float != 0)
	or 
	(NCH_IP_TOT_DDCTN_AMT is not null and NCH_IP_TOT_DDCTN_AMT::float != 0);

select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt,
	clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int) as real_total_cost,
	NCH_BENE_PTA_COINSRNC_LBLTY_AM, NCH_IP_TOT_DDCTN_AMT
from medicare_texas.inpatient_base_claims_k
where (NCH_BENE_PTA_COINSRNC_LBLTY_AM is not null and NCH_BENE_PTA_COINSRNC_LBLTY_AM::float != 0)
	or 
	(NCH_IP_TOT_DDCTN_AMT is not null and NCH_IP_TOT_DDCTN_AMT::float != 0);

allowed amount = claim paid amount + deductible

-- :/ wtf?

--Case study 2: ggggggByjwfByyy	gggggnAuyuAyyag	15
select bene_id, clm_id, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'ggggggByjwfByyy' and clm_id = 'gggggnAuyuAyyag';
--all under tot_chrg_amt, nothing under not covered

select *
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'ggggggByjwfByyy' and clm_id = 'gggggnAuyuAyyag'
order by clm_line_num::int;

select bene_id, clm_id, sum(rev_cntr_tot_chrg_amt::float), sum(rev_cntr_ncvrd_chrg_amt::float)
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'ggggggByjwfByyy' and clm_id = 'gggggnAuyuAyyag'
group by 1, 2;
--49756.4

select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt,
	clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int) as real_total_cost,
	nch_prmry_pyr_clm_pd_amt, nch_bene_ip_ddctbl_amt, nch_bene_pta_coinsrnc_lblty_am, nch_bene_blood_ddctbl_lblty_am,
	nch_ip_tot_ddctn_amt
from medicare_texas.inpatient_base_claims_k
where bene_id = 'ggggggByjwfByyy' and clm_id = 'gggggnAuyuAyyag';
ggggggByjwfByyy	gggggnAuyuAyyag	7421.32	24878.20	0.00	2	24878.2

--are there diag codes in DW that aren't capitalized?
select data_source, count(diag_cd)
from data_warehouse.claim_diag
where diag_cd ~ '[a-z]'
group by data_source;

--get column names AND data type
select column_name, udt_name ||
	case when character_maximum_length is not null then '(' || character_maximum_length || ')'
	else '' end
from information_schema.columns
where table_schema = 'data_warehouse' and table_name = 'claim_detail'
order by ordinal_position;


/**********************
 * Check for agreement between year and bene_enrollmt_ref_yr
 * in mbsf_abcd_summary
 */

select "year", bene_enrollmt_ref_yr
from medicare_texas.mbsf_abcd_summary
where year != bene_enrollmt_ref_yr;

--WHEW perfect agreement


---
select bene_id, clm_id, count(*) from medicare_texas.outpatient_revenue_center_k
group by clm_id, bene_id
having count(*) > 2;

gggggggnggwyuaw	gggggnfgnaBnfay	6
ggggggjAnaBwAaw	gggggugnawgAwua	3
ggggggnjjAwjwju	ggggguygjAngnfA	13
ggggggjAuffwyff	gggggnyjBwyAAAj	50

--Case study 1: gggggggjynjwawn	gggggnnyfwuwyyg	17
select bene_id, clm_id, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt, rev_cntr, REV_CNTR_DDCTBL_COINSRNC_CD, REV_CNTR_PRCNG_IND_CD
from medicare_texas.outpatient_revenue_center_k
where bene_id = 'gggggggnggwyuaw' and clm_id = 'gggggnfgnaBnfay';
--all under tot_chrg_amt, nothing under not covered

select bene_id, clm_id, sum(rev_cntr_tot_chrg_amt::float), sum(rev_cntr_ncvrd_chrg_amt::float)
from medicare_texas.outpatient_revenue_center_k
where bene_id = 'gggggggnggwyuaw' and clm_id = 'gggggnfgnaBnfay'
group by 1, 2;
--966.0

select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt,
NCH_PRMRY_PYR_CLM_PD_AMT, NCH_BENE_BLOOD_DDCTBL_LBLTY_AM, NCH_PROFNL_CMPNT_CHRG_AMT,
NCH_BENE_PTB_DDCTBL_AMT, NCH_BENE_PTB_COINSRNC_AMT, CLM_OP_PRVDR_PMT_AMT, CLM_OP_BENE_PMT_AMT, CLM_MODEL_REIMBRSMT_AMT
from medicare_texas.outpatient_base_claims_k
where bene_id = 'gggggggnggwyuaw' and clm_id = 'gggggnfgnaBnfay';
gggggggjynjwawn	gggggnnyfwuwyyg	12087.89	15172.66	0.00	8	15172.66


/**************
 * Anyone else have rev code = 0001?
 */

select data_source, revenue_cd, count(*)
from data_warehouse.claim_detail
where revenue_cd = '0001'
group by data_source, revenue_cd;

/*
optz	0001	17
truv	0001	8758461  out of 10593715668
mcrt	0001	79770223
mhtw	0001	9
mcpp	0001	1
optd	0001	17
mcrn	0001	69797666
mdcd	0001	8597
*/

select count(*)
from data_warehouse.claim_detail_1_prt_truv;

truven might be medicare supplemental
8758461 out of 10593715668

allowed amount = paid amount + copay + ddb + coins + other ins


select a.year, a.bene_id, b.plan_type, a.pde_id, a.rx_srvc_rfrnc_num, a.cvrd_d_plan_pd_amt, a.ncvrd_plan_pd_amt, a.ptnt_pay_amt, a.prod_srvc_id
from medicare_texas.pde_file a left join dw_staging.mcrt_member_enrollment_yearly b 
on a.bene_id = b.member_id_src and a.year = b.year::text
where cvrd_d_plan_pd_amt != '0.00' and ncvrd_plan_pd_amt != '0.00'
order by a.bene_id, a.pde_id;

2014	ggggggBaaaAAAuj	AB	ggggBjyjyBwufBw	1112315	67.48	-67.48	3.60	00085128801
2014	ggggggBaaaaAnBf
2014	ggggggBaaaafAan
2019	ggggggBaaaajgAj

--doughnut hole = hole in the middle of part D coverage

select a.bene_id, b.rx_coverage, a.pde_id, a.srvc_dt::date, a.cvrd_d_plan_pd_amt, a.ncvrd_plan_pd_amt, a.ptnt_pay_amt, a.prod_srvc_id as ndc, a.fill_num,
a.frmlry_rx_id, a.DRUG_CVRG_STUS_CD
from medicare_texas.pde_file a left join dw_staging.mcrt_member_enrollment_yearly b 
on a.bene_id = b.member_id_src and a.year = b.year::text
where a.bene_id = 'ggggggBaaaAAAuj' and a.year = '2014'
order by SRVC_DT::date;

select a.bene_id, b.rx_coverage, a.pde_id, a.srvc_dt::date, a.cvrd_d_plan_pd_amt, a.ncvrd_plan_pd_amt, a.ptnt_pay_amt, a.prod_srvc_id as ndc, a.fill_num,
a.frmlry_rx_id, a.DRUG_CVRG_STUS_CD
from medicare_texas.pde_file a left join dw_staging.mcrt_member_enrollment_yearly b 
on a.bene_id = b.member_id_src and a.year = b.year::text
where a.bene_id = 'ggggggBaaaaAnBf' and a.year = '2014'
order by SRVC_DT::date;

select a.bene_id, b.rx_coverage, a.pde_id, a.srvc_dt::date, a.cvrd_d_plan_pd_amt, a.ncvrd_plan_pd_amt, a.ptnt_pay_amt, a.prod_srvc_id as ndc, a.fill_num,
a.frmlry_rx_id, a.DRUG_CVRG_STUS_CD
from medicare_texas.pde_file a left join dw_staging.mcrt_member_enrollment_yearly b 
on a.bene_id = b.member_id_src and a.year = b.year::text
where a.bene_id = 'ggggggBaaaajgAj' and a.year = '2019'
order by SRVC_DT::date;

/*
contact CMS directly

total_paid_amount - amount actually paid by Medicare
total_allowed_amount - amount that is covered, includes paid by the plan and paid by the beneficiary (coins, deductible, copay)

what is cvrd_d_plan_pd_amt, ncvrd_plan_pd_amt
*/


select a.bene_id, b.plan_type, a.pde_id, count(*)
from medicare_texas.pde_file a left join dw_staging.mcrt_member_enrollment_yearly b 
on a.bene_id = b.member_id_src and a.year = b.year::text
group by a.bene_id, b.plan_type, a.pde_id;

select pde_id, count(*)
from medicare_texas.pde_file
group by pde_id
having count(*) > 1;

--PDE_ID is one ID per drug fill



/******************
 * Checking out the relationship of LINE_NCH_PMT_AMT, LINE_BENE_PMT_AMT, LINE_PRVDR_PMT_AMT in bcarrier_line
 */

select bene_id, line_nch_pmt_amt, line_bene_pmt_amt, line_prvdr_pmt_amt
from medicare_texas.bcarrier_line_k;

select bene_id, line_nch_pmt_amt, line_bene_pmt_amt, line_prvdr_pmt_amt
from medicare_texas.bcarrier_line_k
where line_nch_pmt_amt != line_prvdr_pmt_amt;
--it looks like line_nch_pmt_amt = line_bene_pmt_amt + line_prvdr_pmt_amt

select bene_id, line_nch_pmt_amt, line_bene_pmt_amt, line_prvdr_pmt_amt,
	line_prvdr_pmt_amt::float + line_bene_pmt_amt::float as sum_bene_prvdr, 
	line_prvdr_pmt_amt::float + line_bene_pmt_amt::float - line_nch_pmt_amt::float as diff
from medicare_texas.bcarrier_line_k
where abs(line_nch_pmt_amt::float - (line_prvdr_pmt_amt::float + line_bene_pmt_amt::float)) > 0.01;


/***************
 * Enrollment table - part C contracts - what possible permutations are there?
 */

--first look at distribution of part c contract id

--get denom first
select count(*) from dw_staging.mcrt_member_enrollment_monthly_etl;
--333982790

select count(*) from dw_staging.mcrt_member_enrollment_monthly_etl
where ptc_cntrct_id is null;
--0 no nulls!

select count(*) from dw_staging.mcrt_member_enrollment_monthly_etl
where ptc_cntrct_id != 'N';
--119845962

select count(*) from dw_staging.mcrt_member_enrollment_monthly_etl
where length(trim(ptc_cntrct_id)) > 1;
--119845962

select length(trim(ptc_cntrct_id)), count(*) from dw_staging.mcrt_member_enrollment_monthly_etl
group by length(trim(ptc_cntrct_id));
/*
5	119845962
1	214136828
*/

--ptc_contract_id be clean af

select mdcr_entlmt_buyin_ind, count(*) as count,
	count(*) * 1.0/(select count(*) from dw_staging.mcrt_member_enrollment_monthly_etl where ptc_cntrct_id != 'N') as pct
from dw_staging.mcrt_member_enrollment_monthly_etl
where ptc_cntrct_id != 'N'
group by mdcr_entlmt_buyin_ind;



--what percent of columns are null for state?
select count(*) from medicare_texas.mbsf_abcd_summary
where state_code is null;

--what about after it's joined to the state table
select count(*)
from medicare_texas.mbsf_abcd_summary a left join reference_tables.ref_medicare_state_codes b
     on a.state_code = b.medicare_state_cd
where b.medicare_state_cd is null;
--2492

select a.state_code, count(*)
from medicare_texas.mbsf_abcd_summary a left join reference_tables.ref_medicare_state_codes b
     on a.state_code = b.medicare_state_cd
where b.medicare_state_cd is null
and a.state_code is not null
group by a.state_code
order by a.state_code;

--RESDAC documentation says that state_code = '74' = Texas, an alternative to '45' - how many?
select count(*)
from medicare_texas.mbsf_abcd_summary where state_code = '74';
--none

--check medicare_national
select count(*)
from medicare_national.mbsf_abcd_summary where state_code = '74';
--also none

/*************
 * Running into some null uth_member_ids for mcrn
 */

select * from dw_staging.mcrn_member_enrollment_monthly where uth_member_id is null;

select * from medicare_national.mbsf_abcd_summary where bene_id = 'gggggggjaAgujnu';

select * from data_warehouse.dim_uth_member_id where member_id_src = 'gggggggjaAgujnu' and data_source = 'mcrn';
--none

select * from data_warehouse.dim_uth_member_id where member_id_src = 'gggggggjaAgujnu' and data_source = 'mcrt';

--ah needed to run the dim script again, and take out a in ('mcrn', 'mcrt') blurb

/***************
 * How clean is dual status code
 */

select dual_stus_cd, count(*) from dw_staging.mcrn_member_enrollment_monthly_etl
group by dual_stus_cd
order by dual_stus_cd;
--CLEAN. that'll do, pig.

/***************
 * Issue with counts not lining up between raw tables and dw_staging tables for mcrt only
 */

select count(*) from medicare_texas.mbsf_abcd_summary;
--29270230

select count(*) from dw_staging.mcrt_member_enrollment_yearly;
--29268186

select 29268186.0/29270230; --0.99993016795563273674

select count(*) from medicare_texas.mbsf_abcd_summary where bene_enrollmt_ref_yr = '2020';
--4538440

select count(*) from dw_staging.mcrt_member_enrollment_yearly where year = 2020;
--4538294

select count(*) from dw_staging.mcrt_member_enrollment_monthly_etl where year = 2020;
--51868937


select count(*) from medicare_national.mbsf_abcd_summary;
--21938353

select count(*) from dw_staging.mcrn_member_enrollment_yearly;
--23476855

select 23476855.0/21938353; --0.99993016795563273674

select count(*) from dw_staging.mcrn_member_enrollment_monthly_etl where year = 2020;
--38770661

select count(*) from dw_staging.mcrn_member_enrollment_monthly where year = 2020;
--38770661

select count(*) from dw_staging.mcrn_member_enrollment_yearly;
--21935699

select count(*) from medicare_national.mbsf_abcd_summary;
--21938353

select 21935699.0/21938353;


select usename, pid, state, waiting, query_start , query, *
from pg_catalog.pg_stat_activity where
usename in ('xrzhang') and ---- put your username
state = 'active'
order by state, usename;

select  pg_terminate_backend(186518);


vacuum analyze data_warehouse.claim_detail;


/***********************
 * 6-29-23 some exploration done after ResDAC sent us stuff
 * 
 * Looks like finding the allowed amount gets split depending on the type of claim
 * 
 * Method 1: CLM_TOT_CHRG_AMT - NCH_IP_NCVR_CHRG_AMT
 * Method 2: CLM_PMT_AMT + NCH_PRMRY_PYR_CLM_PD_AMT + NCH_IP_TOT_DDCTN_AMT
 *
 * */

--let's take a look at the difference between method 1 and method 2 across tables
drop table if exists dev.xz_mcr_costs_exploration;

--inpatient
create table dev.xz_mcr_costs_exploration as
select 'inpatient'::text as src_table, 2020::int as year, sum(clm_tot_chrg_amt::float - nch_ip_ncvrd_chrg_amt::float) as method_1,
	sum(clm_pmt_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::float)
	+ nch_prmry_pyr_clm_pd_amt::float + nch_ip_tot_ddctn_amt::float) as method_2
from medicare_texas.inpatient_base_claims_k
where "year" = '2020';

insert into dev.xz_mcr_costs_exploration
select 'inpatient'::text as src_table, 2016::int as year, sum(clm_tot_chrg_amt::float - nch_ip_ncvrd_chrg_amt::float) as method_1,
	sum(clm_pmt_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::float)
	+ nch_prmry_pyr_clm_pd_amt::float + nch_ip_tot_ddctn_amt::float) as method_2
from medicare_texas.inpatient_base_claims_k
where "year" = '2016';

--outpatient
--nch_ip_ncvrd_chrg_amt does not exist
--nch_ip_tot_ddctn_amt does not exist
insert into dev.xz_mcr_costs_exploration
select 'outpatient'::text as src_table, 2020::int as year, null as method_1,
	sum(clm_pmt_amt::float + nch_prmry_pyr_clm_pd_amt::float + nch_ip_tot_ddctn_amt::float) as method_2
from medicare_texas.outpatient_base_claims_k
where "year" = '2020';

insert into dev.xz_mcr_costs_exploration
select 'outpatient'::text as src_table, 2016::int as year, null as method_1,
	sum(clm_pmt_amt::float + nch_prmry_pyr_clm_pd_amt::float + nch_ip_tot_ddctn_amt::float) as method_2
from medicare_texas.outpatient_base_claims_k
where "year" = '2016';




--Population rates

--inpatient
select sum(case when clm_pmt_amt is not null and abs(clm_pmt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_pmt_amt,
	sum(case when clm_tot_chrg_amt is not null and abs(clm_tot_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_tot_chrg_amt,
	sum(case when nch_ip_ncvrd_chrg_amt is not null and abs(nch_ip_ncvrd_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_ip_ncvrd_chrg_amt,
	sum(case when nch_ip_tot_ddctn_amt is not null and abs(nch_ip_tot_ddctn_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_ip_tot_ddctn_amt,
	sum(case when nch_prmry_pyr_clm_pd_amt is not null and abs(nch_prmry_pyr_clm_pd_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_prmry_pyr_clm_pd_amt
from medicare_texas.inpatient_base_claims_k;

--snf
select sum(case when clm_pmt_amt is not null and abs(clm_pmt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_pmt_amt,
	sum(case when clm_tot_chrg_amt is not null and abs(clm_tot_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_tot_chrg_amt,
	sum(case when nch_ip_ncvrd_chrg_amt is not null and abs(nch_ip_ncvrd_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_ip_ncvrd_chrg_amt,
	sum(case when nch_ip_tot_ddctn_amt is not null and abs(nch_ip_tot_ddctn_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_ip_tot_ddctn_amt,
	sum(case when nch_prmry_pyr_clm_pd_amt is not null and abs(nch_prmry_pyr_clm_pd_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_prmry_pyr_clm_pd_amt
from medicare_texas.snf_base_claims_k;

--hha
select sum(case when clm_pmt_amt is not null and abs(clm_pmt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_pmt_amt,
	sum(case when clm_tot_chrg_amt is not null and abs(clm_tot_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_tot_chrg_amt,
	null as nch_ip_ncvrd_chrg_amt, --column does not exist
	null as nch_ip_tot_ddctn_amt, --column does not exist
	sum(case when nch_prmry_pyr_clm_pd_amt is not null and abs(nch_prmry_pyr_clm_pd_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_prmry_pyr_clm_pd_amt
from medicare_texas.hha_base_claims_k;

--hha - look at its payment columns
select clm_id, clm_pmt_amt,
	clm_tot_chrg_amt,
	nch_prmry_pyr_clm_pd_amt,
	pps_std_val_pymt_amt, --standard payment amount - standard amount w/o geographical payment adjustments
	finl_std_amt --applies additional standardization requirements, not sued for payments, used for comparisons
from medicare_texas.hha_base_claims_k;

--hha - population rate

select sum(case when clm_pmt_amt is not null and abs(clm_pmt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_pmt_amt,
	sum(case when clm_tot_chrg_amt is not null and abs(clm_tot_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_tot_chrg_amt,
	sum(case when nch_prmry_pyr_clm_pd_amt is not null and abs(nch_prmry_pyr_clm_pd_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_prmry_pyr_clm_pd_amt,
	sum(case when pps_std_val_pymt_amt is not null and abs(pps_std_val_pymt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as pps_std_val_pymt_amt, --standard payment amount - standard amount w/o geographical payment adjustments
	sum(case when finl_std_amt is not null and abs(finl_std_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as finl_std_amt --applies additional standardization requirements, not sued for payments, used for comparisons
from medicare_texas.hha_base_claims_k;

--hospice?
select clm_pmt_amt, nch_prmry_pyr_clm_pd_amt, clm_tot_chrg_amt, clm_model_reimbrsmt_amt
	--clm_model_reimbrsmt_amt = net reimbursement amount of what medicare would have paid for global budget services
from medicare_texas.hospice_base_claims_k;

--hospice
select sum(case when clm_pmt_amt is not null and abs(clm_pmt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_pmt_amt,
	sum(case when nch_prmry_pyr_clm_pd_amt is not null and abs(nch_prmry_pyr_clm_pd_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_prmry_pyr_clm_pd_amt,
	sum(case when clm_tot_chrg_amt is not null and abs(clm_tot_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_tot_chrg_amt,
	sum(case when clm_model_reimbrsmt_amt is not null and abs(clm_model_reimbrsmt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_model_reimbrsmt_amt
from medicare_texas.hospice_base_claims_k;








