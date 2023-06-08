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
select bene_id, clm_id, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg';
--all under tot_chrg_amt, nothing under not covered

select bene_id, clm_id, sum(rev_cntr_tot_chrg_amt::float), sum(rev_cntr_ncvrd_chrg_amt::float)
from medicare_texas.inpatient_revenue_center_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg'
group by 1, 2;
--30,345.32

select bene_id, clm_id, clm_pmt_amt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt,
	clm_tot_chrg_amt::float + (clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::int) as real_total_cost
from medicare_texas.inpatient_base_claims_k
where bene_id = 'gggggggjynjwawn' and clm_id = 'gggggnnyfwuwyyg';
gggggggjynjwawn	gggggnnyfwuwyyg	12087.89	15172.66	0.00	8	15172.66

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









