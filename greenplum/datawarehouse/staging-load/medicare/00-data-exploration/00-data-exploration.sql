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















