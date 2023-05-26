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
 */

select bene_birth_dt, bene_death_dt from medicare_texas.mbsf_abcd_summary;











