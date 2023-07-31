/*******************************
 * After some guidance from ResDAC we have some preliminary determinations on how
 * to calculate allowed amounts, but there are still some outstanding questions...
 * 
 * 1) Is the breakdown for acute inpatient/etc specific to the inpatient table or is it all tables?
 * 		What's the breakdown?
 * 2) Total charge - not covered - how does that compare to claim_pay + primary payer pay + deductibles?
 * 		Do deductibles add up to the nch ip tot ddbl amt?
 * 3) Examine interim claims
 * 		is discharge date null for 112/113/114?
 * 		how are costs different on claims with roll-up code 0001 vs other claims?
 * 4) does line nch = line_bene_pmt_amt + line_prvdr_pmt_amt?
 * 5) Does line_allowed = line_nch + coins + ded?
 */


/*****************
 * Question 1: Acute IP, Other IP, IPF/LTCH/IRF - in all tables or just IP?
 * 
 * Answer: Just IP
 */

--we need the nch_clm_type_cd and provider_id

select nch_clm_type_cd, count(*), count(*)*1.0/(select count(*) from medicare_national.inpatient_base_claims_k) as pct
from medicare_national.inpatient_base_claims_k
group by nch_clm_type_cd;
--all 60

select nch_clm_type_cd, count(*), count(*)*1.0/(select count(*) from medicare_national.hha_base_claims_k) as pct
from medicare_national.hha_base_claims_k
group by nch_clm_type_cd;
--all 10

select nch_clm_type_cd, count(*), count(*)*1.0/(select count(*) from medicare_national.hospice_base_claims_k) as pct
from medicare_national.hospice_base_claims_k
group by nch_clm_type_cd;
--all 50

select nch_clm_type_cd, count(*), count(*)*1.0/(select count(*) from medicare_national.snf_base_claims_k) as pct
from medicare_national.snf_base_claims_k
group by nch_clm_type_cd;
--either 30 or 20 (20 = 97.7%, 30 is 2.3%)

select nch_clm_type_cd, count(*), count(*)*1.0/(select count(*) from medicare_national.outpatient_base_claims_k) as pct
from medicare_national.outpatient_base_claims_k
group by nch_clm_type_cd;
--all 40

/*****************
 * 1b: What's the breakdown?
 */

drop table if exists dev.xz_mcr_ip_breakdown;

create table dev.xz_mcr_ip_breakdown as
select clm_id, 
	null::text as facility_type,
	(case when substring(prvdr_num, 3, 1) = '0' then 'Acute - Acute Hospital'
	when substring(prvdr_num, 3, 2) = '13' then 'Acute - Critical Access Hospital'
	when substring(prvdr_num, 3, 2) = '33' then 'Acute - Childrens Hospital'
	when prvdr_num in ('050146', '050660', '100079', '100271', '220162', '330154', '330354', '360242', '390196', '450076', '500138')
		then 'Acute - Cancer Hospital'
	when substring(prvdr_num, 3, 2) in ('40', '41', '42', '43', '44') then 'IPF'
	when substring(prvdr_num, 3, 1) = 'M' then 'IPF - CAH setting'
	when substring(prvdr_num, 3, 1) = 'S' then 'IPF - IPPS setting'
	when substring(prvdr_num, 3, 2) in ('20', '21', '22') then 'LTCH'
	when case when substring(prvdr_num, 3, 4) ~ '^[0-9]*$' then substring(prvdr_num, 3, 4)::int else null end between 3025 and 3099 then 'IRF'
	when substring(prvdr_num, 3, 1) = 'R' then 'IRF - CAH setting'
	when substring(prvdr_num, 3, 1) = 'T' then 'IRF - IPPS setting' end)::text as detailed_facility_type
from medicare_national.inpatient_base_claims_k;

update dev.xz_mcr_ip_breakdown
set facility_type = case when detailed_facility_type like 'Acute%' then 'Acute IP'
	when detailed_facility_type like 'IPF%' then 'IPF'
	when detailed_facility_type like 'IRF%' then 'IRF'
	else detailed_facility_type end;


--note that rows and distinct claims are equivalent (rows and unique claim ids are 1-1)
select detailed_facility_type, count(*) as count,
	count(*)*1.0/(select count(*) from dev.xz_mcr_ip_breakdown) as pct
from dev.xz_mcr_ip_breakdown
group by detailed_facility_type
order by detailed_facility_type;

select facility_type, count(*) as count,
	count(*)*1.0/(select count(*) from dev.xz_mcr_ip_breakdown) as pct
from dev.xz_mcr_ip_breakdown
group by facility_type
order by facility_type;
/*
Acute IP	3502301	0.91431118629643117737
IPF	143907	0.03756838144019046948
IRF	142374	0.03716817624692112198
LTCH	41839	0.01092249516059767108
[NULL]	114	0.000029760855859560087560
*/

/****************************
 * Question 2: Total charge - not covered vs claim_pay + primary payer pay + deductibles?
 */

--population rates (not null or 0)
select sum(case when clm_pmt_amt is not null and abs(clm_pmt_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_pmt_amt,
	sum(case when clm_pass_thru_per_diem_amt is not null and abs(clm_pass_thru_per_diem_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_pass_thru_per_diem_amt,
	sum(case when clm_utlztn_day_cnt is not null and abs(clm_utlztn_day_cnt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_utlztn_day_cnt,
	sum(case when clm_tot_chrg_amt is not null and abs(clm_tot_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as clm_tot_chrg_amt,
	sum(case when nch_ip_ncvrd_chrg_amt is not null and abs(nch_ip_ncvrd_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_ip_ncvrd_chrg_amt,
	sum(case when nch_ip_tot_ddctn_amt is not null and abs(nch_ip_tot_ddctn_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_ip_tot_ddctn_amt,
	sum(case when nch_prmry_pyr_clm_pd_amt is not null and abs(nch_prmry_pyr_clm_pd_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as nch_prmry_pyr_clm_pd_amt
from medicare_texas.inpatient_base_claims_k;

clm_pmt_amt	clm_pass_thru_per_diem_amt	clm_utlztn_day_cnt	clm_tot_chrg_amt	nch_ip_ncvrd_chrg_amt	nch_ip_tot_ddctn_amt	nch_prmry_pyr_clm_pd_amt
0.95572513143734595715	0.46324201003302384609	0.95019079527976673524	1.00000000000000000000	0.04377407802004668307	0.65519002248805938196	0.02619656306715470286

--from base claims
select clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_pmt_amt,
	clm_pass_thru_per_diem_amt,
	clm_utlztn_day_cnt,
	clm_tot_chrg_amt,
	nch_ip_ncvrd_chrg_amt,
	nch_ip_tot_ddctn_amt,
	nch_prmry_pyr_clm_pd_amt,
	clm_tot_chrg_amt::float - nch_ip_ncvrd_chrg_amt::float as difference_method,
	clm_pmt_amt::float + (clm_utlztn_day_cnt::float * clm_pass_thru_per_diem_amt::float)
	+ nch_prmry_pyr_clm_pd_amt::float + nch_ip_tot_ddctn_amt::float as sum_paid_method
from medicare_national.inpatient_base_claims_k;

--from revenue center
select "year", bene_id, clm_id, rev_cntr, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt
from medicare_national.inpatient_revenue_center_k
where year = '2020';

--how many rows each claim got
with cte as (
	select clm_id, count(*) as rowcount from medicare_national.inpatient_revenue_center_k
	group by clm_id
	)
select rowcount, count(*) as claims
from cte
group by rowcount
order by rowcount;

--what about claims with 2 rows
with cte as (
	select clm_id, count(*) as rowcount from medicare_national.inpatient_revenue_center_k
	group by clm_id
	)
select clm_id, rowcount
from cte
where rowcount = 2;

gggggugyuyjawgn	2
gggggugjfgAnffy	2
ggggguufBuggBjB	2
ggggguyjBgjgwfB	2
ggggguaBfuajgAn	2
ggggguujggaujAu	2
gggggnujjjanwnw	2
gggggujgBnfguaf	2

select "year", bene_id, clm_id, rev_cntr, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt
from medicare_national.inpatient_revenue_center_k
where clm_id = 'gggggugyuyjawgn';


--do all claim_ids have rev_cntr = '0001' - inpatient tables
with cte as (
	select clm_id, sum(case when rev_cntr = '0001' then 1 else 0 end) as has_total
	from medicare_national.inpatient_revenue_center_k
	group by clm_id
)select * from cte 
where has_total = 0;
--answer: yes

--snf
with cte as (
	select clm_id, sum(case when rev_cntr = '0001' then 1 else 0 end) as has_total
	from medicare_national.snf_revenue_center_k
	group by clm_id
)select * from cte 
where has_total = 0;
--also yes

--hha
with cte as (
	select clm_id, sum(case when rev_cntr = '0001' then 1 else 0 end) as has_total
	from medicare_national.hha_revenue_center_k
	group by clm_id
)select * from cte 
where has_total = 0;
--also yes

--hospice
with cte as (
	select clm_id, sum(case when rev_cntr = '0001' then 1 else 0 end) as has_total
	from medicare_national.hospice_revenue_center_k
	group by clm_id
)select * from cte 
where has_total = 0;
--also yes

--outpatient
with cte as (
	select clm_id, sum(case when rev_cntr = '0001' then 1 else 0 end) as has_total
	from medicare_national.outpatient_revenue_center_k
	group by clm_id
)select * from cte 
where has_total = 0;
--still yes

--spot check
select year, bene_id, clm_id, clm_line_num::int, rev_cntr,
	rev_cntr_prvdr_pmt_amt, rev_cntr_bene_pmt_amt
	from medicare_national.outpatient_revenue_center_k
where year = '2020'
order by bene_id, clm_id, clm_line_num::int;

select year, bene_id, clm_id, clm_line_num::int, rev_cntr,
	rev_cntr_tot_chrg_amt
	from medicare_national.inpatient_revenue_center_k
where year = '2020'
order by bene_id, clm_id, clm_line_num::int;

--population rates only for rev_cntr = '0001'
select sum(case when rev_cntr_tot_chrg_amt is not null and abs(rev_cntr_tot_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as rev_cntr_tot_chrg_amt,
	sum(case when rev_cntr_ncvrd_chrg_amt is not null and abs(rev_cntr_ncvrd_chrg_amt::float) > 0.01 then 1 else 0 end)*1.0 / count(*) as rev_cntr_ncvrd_chrg_amt
from medicare_texas.inpatient_revenue_center_k
where rev_cntr = '0001';

--1.00000000000000000000	0.04377407802004668307


/******************
 * Question: Do deductibles add up to the nch_ip_deductible amount?
 * Answer: YES
 */

select nch_ip_tot_ddctn_amt, nch_bene_ip_ddctbl_amt, nch_bene_pta_coinsrnc_lblty_am, nch_bene_blood_ddctbl_lblty_am
from medicare_national.inpatient_base_claims_k
where abs((nch_bene_ip_ddctbl_amt::float + nch_bene_pta_coinsrnc_lblty_am::float + nch_bene_blood_ddctbl_lblty_am::float) - nch_ip_tot_ddctn_amt::float) > 0.01;
--none

select nch_ip_tot_ddctn_amt, nch_bene_ip_ddctbl_amt, nch_bene_pta_coinsrnc_lblty_am, nch_bene_blood_ddctbl_lblty_am
from medicare_national.inpatient_base_claims_k
where abs((nch_bene_ip_ddctbl_amt::float + nch_bene_pta_coinsrnc_lblty_am::float) - nch_ip_tot_ddctn_amt::float) > 0.01;

/****************
 * Get column names - see how columns match up
 */

select table_name, ordinal_position, column_name
from information_schema.columns
where table_schema = 'medicare_national' and table_name like '%claims%'
	and (column_name like '%amt%' or column_name in ('nch_bene_blood_ddctbl_lblty_am', 'nch_bene_pta_coinsrnc_lblty_am'))
order by table_name, ordinal_position;

select table_name, ordinal_position, column_name
from information_schema.columns
where table_schema = 'medicare_national' and (table_name like '%line%' or table_name like '%revenue_center%')
	and (column_name like '%amt%' or column_name in ( 'nch_bene_blood_ddctbl_lblty_am', 'rev_cntr_coinsrnc_wge_adjstd_c'))
order by table_name, ordinal_position;


/**************************
 *  * 3) Examine interim claims
 * 		is discharge date null for 112/113/114?
 * 		how are costs different on claims with roll-up code 0001 vs other claims?
 */




/***************
 * 4) does line nch = line_bene_pmt_amt + line_prvdr_pmt_amt?
 * 
 * ANS: YES
 */
select bene_id, line_nch_pmt_amt, line_bene_pmt_amt, line_prvdr_pmt_amt
from medicare_texas.bcarrier_line_k
where abs(line_nch_pmt_amt::float - (line_prvdr_pmt_amt::float + line_bene_pmt_amt::float))> 0.01;
--no results!

--follow-up: does clm_pmt_amt = nch_clm_prvdr_pmt_amt + nch_clm_bene_pmt_amt?

select bene_id, clm_pmt_amt, nch_clm_bene_pmt_amt, nch_clm_prvdr_pmt_amt
from medicare_texas.bcarrier_claims_k
where abs(clm_pmt_amt::float - (nch_clm_bene_pmt_amt::float + nch_clm_prvdr_pmt_amt::float))> 0.01;
--surprisingly, no

--what percentage?
select sum(case when abs(clm_pmt_amt::float - (nch_clm_bene_pmt_amt::float + nch_clm_prvdr_pmt_amt::float))> 0.01 then 1 else 0 end) * 1.0 /
	count(*) as pct_nomatch,
	sum(case when (nch_clm_bene_pmt_amt::float + nch_clm_prvdr_pmt_amt::float) - clm_pmt_amt::float > 0.01 then 1 else 0 end) * 1.0 /
	count(*) as split_is_more
from medicare_national.bcarrier_claims_k;
--uhhhh it's ever so slightly more accurate to use the split version


/***************
 * 4) Does line_allowed = line_nch + coins + ded
 * 
 * ANS: NO, there's a line_other_applied_amount (7 columns) that can be used
 */

select line_alowd_chrg_amt, line_nch_pmt_amt, line_bene_prmry_pyr_pd_amt, line_bene_ptb_ddctbl_amt, line_coinsrnc_amt,
	line_alowd_chrg_amt::float - (line_nch_pmt_amt::float + line_bene_prmry_pyr_pd_amt::float + line_bene_ptb_ddctbl_amt::float + line_coinsrnc_amt::float) as diff
from medicare_national.bcarrier_line_k
where abs(line_alowd_chrg_amt::float - (line_nch_pmt_amt::float + line_bene_prmry_pyr_pd_amt::float + line_bene_ptb_ddctbl_amt::float + line_coinsrnc_amt::float))>0.01;


select line_alowd_chrg_amt::float - 
	(line_nch_pmt_amt::float + line_bene_prmry_pyr_pd_amt::float + line_bene_ptb_ddctbl_amt::float + line_coinsrnc_amt::float
	+ line_othr_apld_amt1::float
	+ line_othr_apld_amt2::float
	+ line_othr_apld_amt3::float
	+ line_othr_apld_amt4::float
	+ line_othr_apld_amt5::float
	+ line_othr_apld_amt6::float
	+ line_othr_apld_amt7::float
	) as diff,
 *
from medicare_national.bcarrier_line_k
where abs(line_alowd_chrg_amt::float - 
	(line_nch_pmt_amt::float + line_bene_prmry_pyr_pd_amt::float + line_bene_ptb_ddctbl_amt::float + line_coinsrnc_amt::float
	+ line_othr_apld_amt1::float
	+ line_othr_apld_amt2::float
	+ line_othr_apld_amt3::float
	+ line_othr_apld_amt4::float
	+ line_othr_apld_amt5::float
	+ line_othr_apld_amt6::float
	+ line_othr_apld_amt7::float
	))>0.01;


--do some calcs, make some graphs - bcarrier
drop table if exists dev.xz_mcr_explor_1;

create table dev.xz_mcr_explor_1 as
select year,
	line_alowd_chrg_amt::float as line_alowd_chrg_amt,
	line_nch_pmt_amt::float as line_nch_pmt_amt,
	line_bene_prmry_pyr_pd_amt::float as line_bene_prmry_pyr_pd_amt,
	line_bene_ptb_ddctbl_amt::float as line_bene_ptb_ddctbl_amt,
	line_coinsrnc_amt::float as line_coinsrnc_amt,
	case when line_othr_apld_ind_cd1 = 'N' then 0
		when line_othr_apld_ind_cd1 in ('M', 'Q') then line_othr_apld_amt1::float * 0
		when line_othr_apld_ind_cd1 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt1::float end as line_othr_apld_amt1,
	case when line_othr_apld_ind_cd2 = 'N' then 0
		when line_othr_apld_ind_cd2 in ('M', 'Q') then line_othr_apld_amt2::float * 0
		when line_othr_apld_ind_cd2 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt2::float end as line_othr_apld_amt2,
	case when line_othr_apld_ind_cd3 = 'N' then 0
		when line_othr_apld_ind_cd3 in ('M', 'Q') then line_othr_apld_amt3::float * 0
		when line_othr_apld_ind_cd3 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt3::float end as line_othr_apld_amt3,
	case when line_othr_apld_ind_cd4 = 'N' then 0
		when line_othr_apld_ind_cd4 in ('M', 'Q') then line_othr_apld_amt4::float * 0
		when line_othr_apld_ind_cd4 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt4::float end as line_othr_apld_amt4,
	case when line_othr_apld_ind_cd5 = 'N' then 0
		when line_othr_apld_ind_cd5 in ('M', 'Q') then line_othr_apld_amt5::float * 0
		when line_othr_apld_ind_cd5 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt5::float end as line_othr_apld_amt5,
	case when line_othr_apld_ind_cd6 = 'N' then 0
		when line_othr_apld_ind_cd6 in ('M', 'Q') then line_othr_apld_amt6::float * 0
		when line_othr_apld_ind_cd6 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt6::float end as line_othr_apld_amt6,
	case when line_othr_apld_ind_cd7 = 'N' then 0
		when line_othr_apld_ind_cd7 in ('M', 'Q') then line_othr_apld_amt7::float * 0
		when line_othr_apld_ind_cd7 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt7::float end as line_othr_apld_amt7,
	line_othr_apld_ind_cd1,
	line_othr_apld_ind_cd2,
	line_othr_apld_ind_cd3,
	line_othr_apld_ind_cd4,
	line_othr_apld_ind_cd5,
	line_othr_apld_ind_cd6,
	line_othr_apld_ind_cd7
from medicare_national.bcarrier_line_k;

select * from dev.xz_mcr_explor_1;

--I think this is as good as it's gonna get?
select line_alowd_chrg_amt - (line_nch_pmt_amt + line_bene_prmry_pyr_pd_amt + line_bene_ptb_ddctbl_amt + line_coinsrnc_amt
	+ line_othr_apld_amt1
	+ line_othr_apld_amt2
	+ line_othr_apld_amt3
	+ line_othr_apld_amt4
	+ line_othr_apld_amt5
	+ line_othr_apld_amt6
	+ line_othr_apld_amt7) as diff,
	*
from dev.xz_mcr_explor_1
where abs(line_alowd_chrg_amt - (line_nch_pmt_amt + line_bene_prmry_pyr_pd_amt + line_bene_ptb_ddctbl_amt + line_coinsrnc_amt
	+ line_othr_apld_amt1
	+ line_othr_apld_amt2
	+ line_othr_apld_amt3
	+ line_othr_apld_amt4
	+ line_othr_apld_amt5
	+ line_othr_apld_amt6
	+ line_othr_apld_amt7))>0.01;
	
--see if the primary payer is always an issue
select line_alowd_chrg_amt - (line_nch_pmt_amt + line_bene_prmry_pyr_pd_amt + line_bene_ptb_ddctbl_amt + line_coinsrnc_amt
	+ line_othr_apld_amt1
	+ line_othr_apld_amt2
	+ line_othr_apld_amt3
	+ line_othr_apld_amt4
	+ line_othr_apld_amt5
	+ line_othr_apld_amt6
	+ line_othr_apld_amt7) as diff,
	*
from dev.xz_mcr_explor_1
where line_bene_prmry_pyr_pd_amt > 0.01;

--get sums
drop table if exists dev.xz_mcr_explor_2;

create table dev.xz_mcr_explor_2 as
select year, sum(line_alowd_chrg_amt) as line_alowd_chrg_amt,
	sum(line_nch_pmt_amt) as line_nch_pmt_amt,
	sum(line_bene_prmry_pyr_pd_amt) as line_bene_prmry_pyr_pd_amt,
	sum(line_bene_ptb_ddctbl_amt) as line_bene_ptb_ddctbl_amt,
	sum(line_coinsrnc_amt) as line_coinsrnc_amt,
	sum(line_othr_apld_amt1
	+ line_othr_apld_amt2
	+ line_othr_apld_amt3
	+ line_othr_apld_amt4
	+ line_othr_apld_amt5
	+ line_othr_apld_amt6
	+ line_othr_apld_amt7) as line_othr_apld_amt,
	null::float as mdcr_paid_pct,
	null::float as other_prmry_paid_pct,
	null::float as bene_deduct_pct,
	null::float as bene_coins_pct,
	null::float as othr_aplyd_pct
from dev.xz_mcr_explor_1
group by year
order by year;

insert into dev.xz_mcr_explor_2
select 'ALL' as year, sum(line_alowd_chrg_amt) as line_alowd_chrg_amt,
	sum(line_nch_pmt_amt) as line_nch_pmt_amt,
	sum(line_bene_prmry_pyr_pd_amt) as line_bene_prmry_pyr_pd_amt,
	sum(line_bene_ptb_ddctbl_amt) as line_bene_ptb_ddctbl_amt,
	sum(line_coinsrnc_amt) as line_coinsrnc_amt,
	sum(line_othr_apld_amt1
	+ line_othr_apld_amt2
	+ line_othr_apld_amt3
	+ line_othr_apld_amt4
	+ line_othr_apld_amt5
	+ line_othr_apld_amt6
	+ line_othr_apld_amt7) as line_othr_apld_amt,
	null::float as mdcr_paid_pct,
	null::float as other_prmry_paid_pct,
	null::float as bene_deduct_pct,
	null::float as bene_coins_pct,
	null::float as othr_aplyd_pct
from dev.xz_mcr_explor_1;

update dev.xz_mcr_explor_2
set mdcr_paid_pct = line_nch_pmt_amt/line_alowd_chrg_amt,
	other_prmry_paid_pct = line_bene_prmry_pyr_pd_amt/line_alowd_chrg_amt,
	bene_deduct_pct = line_bene_ptb_ddctbl_amt/line_alowd_chrg_amt,
	bene_coins_pct = line_coinsrnc_amt/line_alowd_chrg_amt,
	othr_aplyd_pct = line_othr_apld_amt/line_alowd_chrg_amt;

select * from dev.xz_mcr_explor_2;


--do some calcs, make some graphs - dme
drop table if exists dev.xz_mcr_explor_1;

create table dev.xz_mcr_explor_1 as
select year,
	line_alowd_chrg_amt::float as line_alowd_chrg_amt,
	line_nch_pmt_amt::float as line_nch_pmt_amt,
	line_bene_prmry_pyr_pd_amt::float as line_bene_prmry_pyr_pd_amt,
	line_bene_ptb_ddctbl_amt::float as line_bene_ptb_ddctbl_amt,
	line_coinsrnc_amt::float as line_coinsrnc_amt,
	case when line_othr_apld_ind_cd1 = 'N' then 0
		when line_othr_apld_ind_cd1 in ('M', 'Q') then line_othr_apld_amt1::float * 0
		when line_othr_apld_ind_cd1 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt1::float end as line_othr_apld_amt1,
	case when line_othr_apld_ind_cd2 = 'N' then 0
		when line_othr_apld_ind_cd2 in ('M', 'Q') then line_othr_apld_amt2::float * 0
		when line_othr_apld_ind_cd2 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt2::float end as line_othr_apld_amt2,
	case when line_othr_apld_ind_cd3 = 'N' then 0
		when line_othr_apld_ind_cd3 in ('M', 'Q') then line_othr_apld_amt3::float * 0
		when line_othr_apld_ind_cd3 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt3::float end as line_othr_apld_amt3,
	case when line_othr_apld_ind_cd4 = 'N' then 0
		when line_othr_apld_ind_cd4 in ('M', 'Q') then line_othr_apld_amt4::float * 0
		when line_othr_apld_ind_cd4 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt4::float end as line_othr_apld_amt4,
	case when line_othr_apld_ind_cd5 = 'N' then 0
		when line_othr_apld_ind_cd5 in ('M', 'Q') then line_othr_apld_amt5::float * 0
		when line_othr_apld_ind_cd5 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt5::float end as line_othr_apld_amt5,
	case when line_othr_apld_ind_cd6 = 'N' then 0
		when line_othr_apld_ind_cd6 in ('M', 'Q') then line_othr_apld_amt6::float * 0
		when line_othr_apld_ind_cd6 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt6::float end as line_othr_apld_amt6,
	case when line_othr_apld_ind_cd7 = 'N' then 0
		when line_othr_apld_ind_cd7 in ('M', 'Q') then line_othr_apld_amt7::float * 0
		when line_othr_apld_ind_cd7 = 'E' then line_othr_apld_amt1::float * -1
		else line_othr_apld_amt7::float end as line_othr_apld_amt7,
	line_othr_apld_ind_cd1,
	line_othr_apld_ind_cd2,
	line_othr_apld_ind_cd3,
	line_othr_apld_ind_cd4,
	line_othr_apld_ind_cd5,
	line_othr_apld_ind_cd6,
	line_othr_apld_ind_cd7
from medicare_national.dme_line_k;

select * from dev.xz_mcr_explor_1;

--get sums
drop table if exists dev.xz_mcr_explor_2;

create table dev.xz_mcr_explor_2 as
select year, sum(line_alowd_chrg_amt) as line_alowd_chrg_amt,
	sum(line_nch_pmt_amt) as line_nch_pmt_amt,
	sum(line_bene_prmry_pyr_pd_amt) as line_bene_prmry_pyr_pd_amt,
	sum(line_bene_ptb_ddctbl_amt) as line_bene_ptb_ddctbl_amt,
	sum(line_coinsrnc_amt) as line_coinsrnc_amt,
	sum(line_othr_apld_amt1
	+ line_othr_apld_amt2
	+ line_othr_apld_amt3
	+ line_othr_apld_amt4
	+ line_othr_apld_amt5
	+ line_othr_apld_amt6
	+ line_othr_apld_amt7) as line_othr_apld_amt,
	null::float as mdcr_paid_pct,
	null::float as other_prmry_paid_pct,
	null::float as bene_deduct_pct,
	null::float as bene_coins_pct,
	null::float as othr_aplyd_pct
from dev.xz_mcr_explor_1
group by year
order by year;

insert into dev.xz_mcr_explor_2
select 'ALL' as year, sum(line_alowd_chrg_amt) as line_alowd_chrg_amt,
	sum(line_nch_pmt_amt) as line_nch_pmt_amt,
	sum(line_bene_prmry_pyr_pd_amt) as line_bene_prmry_pyr_pd_amt,
	sum(line_bene_ptb_ddctbl_amt) as line_bene_ptb_ddctbl_amt,
	sum(line_coinsrnc_amt) as line_coinsrnc_amt,
	sum(line_othr_apld_amt1
	+ line_othr_apld_amt2
	+ line_othr_apld_amt3
	+ line_othr_apld_amt4
	+ line_othr_apld_amt5
	+ line_othr_apld_amt6
	+ line_othr_apld_amt7) as line_othr_apld_amt,
	null::float as mdcr_paid_pct,
	null::float as other_prmry_paid_pct,
	null::float as bene_deduct_pct,
	null::float as bene_coins_pct,
	null::float as othr_aplyd_pct
from dev.xz_mcr_explor_1;

update dev.xz_mcr_explor_2
set mdcr_paid_pct = line_nch_pmt_amt/line_alowd_chrg_amt,
	other_prmry_paid_pct = line_bene_prmry_pyr_pd_amt/line_alowd_chrg_amt,
	bene_deduct_pct = line_bene_ptb_ddctbl_amt/line_alowd_chrg_amt,
	bene_coins_pct = line_coinsrnc_amt/line_alowd_chrg_amt,
	othr_aplyd_pct = line_othr_apld_amt/line_alowd_chrg_amt;

select * from dev.xz_mcr_explor_2;

/****************
 * Question: how does the revenue center patient add-on payment work?
 * 
 * 
 * 
 * 
 */

select year, bene_id, clm_id, 
	rev_cntr_blood_ddctbl_amt,
	rev_cntr_cash_ddctbl_amt,
	rev_cntr_coinsrnc_wge_adjstd_c,
	rev_cntr_rdcd_coinsrnc_amt,
	rev_cntr_prvdr_pmt_amt,
	rev_cntr_bene_pmt_amt,
	rev_cntr_pmt_amt_amt,
	rev_cntr_tot_chrg_amt,
	rev_cntr_ncvrd_chrg_amt,
	rc_ptnt_add_on_pymt_amt
from medicare_national.outpatient_revenue_center_k
where rev_cntr_bene_pmt_amt::float != 0;

select year, bene_id, clm_id,
	clm_pmt_amt,
	nch_prmry_pyr_clm_pd_amt,
	clm_tot_chrg_amt,
	nch_bene_blood_ddctbl_lblty_am,
	nch_profnl_cmpnt_chrg_amt,
	clm_model_reimbrsmt_amt,
	nch_bene_ptb_ddctbl_amt,
	nch_bene_ptb_coinsrnc_amt,
	clm_op_prvdr_pmt_amt,
	clm_op_bene_pmt_amt
from medicare_national.outpatient_base_claims_k
where bene_id = 'gggggggayfnjjgB' and clm_id = 'gggggnjAuujnuwn';
--the add-on payment is not factored in on claim-level it looks like

select year, bene_id, clm_id,
	clm_pmt_amt,
	nch_prmry_pyr_clm_pd_amt,
	clm_tot_chrg_amt,
	nch_bene_blood_ddctbl_lblty_am,
	nch_profnl_cmpnt_chrg_amt,
	clm_model_reimbrsmt_amt,
	nch_bene_ptb_ddctbl_amt,
	nch_bene_ptb_coinsrnc_amt,
	clm_op_prvdr_pmt_amt,
	clm_op_bene_pmt_amt
from medicare_national.outpatient_base_claims_k
where bene_id = 'ggggggByjagjuwn' and clm_id = 'gggggnuawyufnfy';
--the bene_pmt_amt is included

--now how accurate is the rev_cntr_pmt_amt_amt
select sum(case when abs(rev_cntr_pmt_amt_amt::float - (rev_cntr_bene_pmt_amt::float + rev_cntr_prvdr_pmt_amt::float)) > 0.01 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.outpatient_revenue_center_k;
--0.04589910127978002724

select rev_cntr_pmt_amt_amt, rev_cntr_bene_pmt_amt, rev_cntr_prvdr_pmt_amt, rev_cntr_1st_msp_pd_amt, rev_cntr_2nd_msp_pd_amt, rev_cntr_rdcd_coinsrnc_amt,
	case when rev_cntr_prvdr_pmt_amt::float < 0 and abs(rev_cntr_pmt_amt_amt::float - 0) < 0.01 then 1 else 0 end as neg_prvdr_paid_amt,
	case when rev_cntr_pmt_amt_amt::float = 0 then 0
		when abs((rev_cntr_pmt_amt_amt::float - rev_cntr_prvdr_pmt_amt::float)/rev_cntr_pmt_amt_amt::float)<.05 then 1 else 0 end as probably_typo
from medicare_national.outpatient_revenue_center_k
where abs(rev_cntr_pmt_amt_amt::float - (rev_cntr_bene_pmt_amt::float + rev_cntr_prvdr_pmt_amt::float)) > 0.01;

--what % of the time is it bc the prvdr pmt amt is negative
select sum(case when rev_cntr_prvdr_pmt_amt::float < 0 and abs(rev_cntr_pmt_amt_amt::float - 0) < 0.01 then 1 else 0 end) * 1.0/count(*) as neg_prvdr_paid_amt,
	sum(case when rev_cntr_pmt_amt_amt::float = 0 then 0
		when abs((rev_cntr_pmt_amt_amt::float - rev_cntr_prvdr_pmt_amt::float)/rev_cntr_pmt_amt_amt::float)<.05 then 1 else 0 end) * 1.0/count(*) as probably_typo
from medicare_national.outpatient_revenue_center_k
where abs(rev_cntr_pmt_amt_amt::float - (rev_cntr_bene_pmt_amt::float + rev_cntr_prvdr_pmt_amt::float)) > 0.01;

select *
from medicare_national.outpatient_revenue_center_k
where abs(rev_cntr_pmt_amt_amt::float - (rev_cntr_bene_pmt_amt::float + rev_cntr_prvdr_pmt_amt::float)) > 0.01;

select *
from medicare_national.outpatient_revenue_center_k
where abs(rev_cntr_ptnt_rspnsblty_pmt::float - (rev_cntr_cash_ddctbl_amt::float + rev_cntr_blood_ddctbl_amt::float + rev_cntr_coinsrnc_wge_adjstd_c::float)) > 0.01;

select sum(case when abs(rev_cntr_ptnt_rspnsblty_pmt::float - (rev_cntr_cash_ddctbl_amt::float + rev_cntr_blood_ddctbl_amt::float + rev_cntr_coinsrnc_wge_adjstd_c::float)) > 0.01 then 1 else 0 end)
	* 1.0 / count(*)
from medicare_national.outpatient_revenue_center_k;
0.00033242893923946576
--these might just be outliers

--on HHA rev tables, can provider payment be negatives?
select *
from medicare_national.hha_revenue_center_k
where rev_cntr_prvdr_pmt_amt::float < 0;
--88 rows

select sum(case when rev_cntr_prvdr_pmt_amt::float < 0 then 1 else 0 end), count(*)
from medicare_national.hha_revenue_center_k;

select *
from medicare_national.hha_revenue_center_k
where abs(rev_cntr_prvdr_pmt_amt::float - rev_cntr_pmt_amt_amt::float) > 0.01;

/*******************
 * Unresolved Questions:
 * 
 * hospice - bene_pmt_amt is included?
 * --yes it is included in the total pay amount
 * --but its population rate is like...... very close to 0. 19 rows total across all years
 * 
 * rates on ncvrd_chrg_amt
 * 
 * then MOVE ON to claim-level tables
 */

select * from medicare_national.hospice_revenue_center_k
where rev_cntr_bene_pmt_amt::float != 0;
ggggggggBfygfyg	gggggunanggnfwA
ggggggggBgfffnn	gggggnwunAyfyyB

select clm_pmt_amt
from medicare_national.hospice_base_claims_k
where bene_id = 'ggggggggBfygfyg' and clm_id = 'gggggunanggnfwA';

--and what % is not null
select sum(case when rev_cntr_bene_pmt_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.hospice_revenue_center_k;
--0.000000510260273291642562
--19 rows

/***************
 * Rates on ncvrd charge
 */

select sum(case when rev_cntr_ncvrd_chrg_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.inpatient_revenue_center_k;
--0.02354240711967645263

select sum(case when rev_cntr_ncvrd_chrg_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.snf_revenue_center_k;
--0.04257230401261652574

select sum(case when rev_cntr_ncvrd_chrg_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.hha_revenue_center_k;
--0.01854544504066130432

select sum(case when rev_cntr_ncvrd_chrg_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.hospice_revenue_center_k;
--0.01311121828964032801

select sum(case when rev_cntr_ncvrd_chrg_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.outpatient_revenue_center_k;
--0.07900354292850324881

--line tables don't have ncvrd

--ok that's the revenue tables, what about the claims tables?
select sum(case when nch_ip_ncvrd_chrg_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.inpatient_base_claims_k;
--0.05181521641232882613

select sum(case when nch_ip_ncvrd_chrg_amt::float != 0 then 1 else 0 end) * 1.0 / count(*)
from medicare_national.snf_base_claims_k;
--0.15097740207546529441


/***********
 * mini-calc of charged, allowed, paid from bcarrier
******/

select sum(coalesce(nch_carr_clm_sbmtd_chrg_amt::float, 0)) as nch_carr_clm_sbmtd_chrg_amt,
	sum(coalesce(nch_carr_clm_alowd_amt::float, 0)) as nch_carr_clm_alowd_amt,
	sum(coalesce(clm_pmt_amt::float, 0)) as clm_pmt_amt
from medicare_texas.bcarrier_claims_k
where year::int = 2020;

/****************
 * There's no coinsurance column for bcarrier claims table - why?
 */

select sum(coalesce(line_coinsrnc_amt::float, 0)) as coins
from medicare_national.bcarrier_line_k
where year = '2020';

--1,102,335,244.5699763

--question to answer: is coins on line table mapping to anything on claims table? bcarrier/dme
--let's look at a case study
select year, bene_id, clm_id, line_nch_pmt_amt, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_coinsrnc_amt, line_alowd_chrg_amt
from medicare_national.bcarrier_line_k
where coalesce(line_coinsrnc_amt::float, 0) != 0;
2015	gggggggAjafufyn	ggggBBjBaBjfuga	66.42	0.00	66.42	0.00	16.95	84.73
2015	ggggggjgwBABjyg	ggggBBfnffgnyfw	31.24	0.00	31.24	0.00	7.97	39.85
2015	ggggggjguunBnwA	ggggBfawjAwujwf	78.62	0.00	78.62	0.00	20.05	100.27

select year, bene_id, clm_id, line_nch_pmt_amt, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_coinsrnc_amt, line_alowd_chrg_amt
from medicare_national.bcarrier_line_k
where bene_id = 'gggggggAjafufyn' and clm_id = 'ggggBBjBaBjfuga';
year	bene_id			clm_id			nch_pmt	to_bene	to_prv	ded		coins	allwd	
2015	gggggggAjafufyn	ggggBBjBaBjfuga	66.42	0.00	66.42	0.00	16.95	84.73
2015	gggggggAjafufyn	ggggBBjBaBjfuga	888.10	0.00	888.10	0.00	226.56	1132.78


select year, bene_id, clm_id, clm_pmt_amt, nch_carr_clm_alowd_amt, carr_clm_cash_ddctbl_apld_amt, clm_bene_pd_amt
from medicare_national.bcarrier_claims_k
where bene_id = 'gggggggAjafufyn' and clm_id = 'ggggBBjBaBjfuga';

--hm, is clm_bene_pd_amt always 0?
select year, bene_id, clm_id, clm_pmt_amt, nch_carr_clm_alowd_amt, carr_clm_cash_ddctbl_apld_amt, clm_bene_pd_amt
from medicare_national.bcarrier_claims_k
where coalesce(clm_bene_pd_amt::float, 0) != 0
and year = '2020';
year	bene_id			clm_id			paid	allowed	ded		bene_pd_amt
2014	ggggggByfjgfyju	ggggBgyyuABafyg	65.92	84.09	0.00	30.00
2020	gggggggwuangyyy	ggggBwjfgygAggA	0.00	74.83	74.83	70.00

--what does this map to
select year, bene_id, clm_id, line_nch_pmt_amt, line_bene_ptb_ddctbl_amt, line_coinsrnc_amt, line_alowd_chrg_amt
from medicare_national.bcarrier_line_k
where bene_id = 'ggggggByfjgfyju' and clm_id = 'ggggBgyyuABafyg';
2014	ggggggByfjgfyju	ggggBgyyuABafyg	65.92	0.00	16.82	84.09
--nothing. absolutely nothing.

select year, bene_id, clm_id, line_nch_pmt_amt, line_bene_ptb_ddctbl_amt, line_coinsrnc_amt, line_alowd_chrg_amt
from medicare_national.bcarrier_line_k
where bene_id = 'gggggggwuangyyy' and clm_id = 'ggggBwjfgygAggA';
2020	gggggggwuangyyy	ggggBwjfgygAggA	0.00	74.83	0.00	74.83

--what percent of paid is adjustments?
select sum(clm_pmt_amt::float) as clm_pmt_amt,
	sum(coalesce(clm_pass_thru_per_diem_amt::float * clm_utlztn_day_cnt::float, 0)) as utilization,
	sum(coalesce(nch_prmry_pyr_clm_pd_amt::float, 0)) as nch_prmry_pyr_clm_pd_amt,
	sum(coalesce(nch_ip_tot_ddctn_amt::float, 0)) as nch_ip_tot_ddctn_amt,
	sum(coalesce(clm_ip_low_vol_pmt_amt::float, 0)) as clm_ip_low_vol_pmt_amt,
	sum(coalesce(clm_uncompd_care_pmt_amt::float, 0)) as clm_uncompd_care_pmt_amt,
	sum(coalesce(clm_bndld_adjstmt_pmt_amt::float, 0)) as clm_bndld_adjstmt_pmt_amt,
	sum(coalesce(clm_vbp_adjstmt_pmt_amt::float, 0)) as clm_vbp_adjstmt_pmt_amt,
	sum(coalesce(clm_hrr_adjstmt_pmt_amt::float, 0)) as clm_hrr_adjstmt_pmt_amt,
	sum(coalesce(ehr_pymt_adjstmt_amt::float, 0)) as ehr_pymt_adjstmt_amt
from medicare_national.inpatient_base_claims_k
where year = '2020';

/*****************
 * Question: do continuing claims really not have a discharge date
 * 
 * Answer: They really don't.
 */
drop table if exists dev.xz_temp1;

create table dev.xz_temp1 as
select year, bene_id, clm_id,
	clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	nch_bene_dschrg_dt
from medicare_national.inpatient_base_claims_k;

select * from dev.xz_temp1 where bill in ('111', '112', '113', '114');
111 has discharge date
114 has discharge date
112, 113 does not

select bill, sum(case when nch_bene_dschrg_dt is null then 1 else 0 end) as null_discharge, 
 	sum(case when nch_bene_dschrg_dt is not null then 1 else 0 end) as not_null_discharge, 
 	sum(case when nch_bene_dschrg_dt is null then 1 else 0 end) * 1.0 /count(*) as null_pct,
 	sum(case when nch_bene_dschrg_dt is not null then 1 else 0 end) * 1.0 /count(*) as not_null_pct
from dev.xz_temp1
where bill in ('111', '112', '113', '114')
group by bill
order by bill;

/**************************
 * Get some encounters for continuing bills
 */

select *
from data_warehouse.admission_acute_ip_claims
where uth_claim_id in (
	select uth_claim_id from data_warehouse.claim_detail_1_prt_mcrn
	where bill = '114'
);

--check out these claims
532482626-000-2020
532686669-004-2020
533147398-000-2020
530942645-001-2016
533333760-000-2015
532428739-002-2016

--532482626-000-2020
select * from data_warehouse.admission_acute_ip_claims
where derived_uth_admission_id = '532482626-000-2020';
ggggguwBnnBfAfa
ggggBwnnungyuyn

select * from data_warehouse.claim_detail_1_prt_mcrn
where claim_id_src = 'ggggguwBnnBfAfa'; --114, inpatient_revenue_center

select * from data_warehouse.claim_detail_1_prt_mcrn
where claim_id_src = 'ggggBwnnungyuyn'; --no bill, bcarrier

--532686669-004-2020
select * from data_warehouse.admission_acute_ip_claims
where derived_uth_admission_id = '532686669-004-2020';
ggggBwwnwwgBaga
ggggBwwjgfAajwy
ggggBwwfufBByBy
ggggBwwwBBuywgn
ggggguyfuyyfAwa --the only F claim type on here
ggggBwwwBBuywgj
ggggBwwngyBgaaA
ggggBwwnwwgBafg
ggggBwwjgfAajwa
ggggBwwfufBByBa
ggggBwwwBBuywgB
ggggBwwABfBaAjw
ggggBwwjgfAajww
ggggBwwnwwgBagy

select * from data_warehouse.claim_detail_1_prt_mcrn
where claim_id_src = 'ggggguyfuyyfAwa'; --114, inpatient_revenue_center
--member id src is gggggggfnAwujBf, year is 2020, June 09-July 10

select clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill from medicare_national.inpatient_base_claims_k
where year = '2020' and bene_id = 'gggggggfnAwujBf';
ggggguAyfggBuAw	111
ggggguwanAnwygn	112
ggggguyfuyyfAwa	114

select clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt,
	ptnt_dschrg_stus_cd
from medicare_national.inpatient_base_claims_k
where year = '2020' and bene_id = 'gggggggfnAwujBf';
ggggguAyfggBuAw	111	21DEC2019	16JAN2020	37330.46	0.00	26	32589.08
ggggguwanAnwygn	112	11MAY2020	08JUN2020	60440.22	0.00	29	54351.13
ggggguyfuyyfAwa	114	09JUN2020	10JUL2020	53312.15	0.00	31	49202.18




--check out some more
select year, bene_id, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill from medicare_national.inpatient_base_claims_k
where clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd = '114';
2014	gggggggjjwjuynA	gggggnfanfyugnw	114
2015	gggggggBAfjwwwn	gggggnnjfffAgnA	114
2019	gggggggjffgBawu	ggggguAjwaAjfaw	114
2020	ggggggggAjABBaj	ggggguwwjwjafwg	114
2018	ggggggjyAgjaugA	gggggujajanfnBB	114

select year, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt
from medicare_national.inpatient_base_claims_k
where year = '2014' and bene_id = 'gggggggjjwjuynA';
--this one is consecutive
2014 gggggnfanfyugnw	114	17APR2014	20MAY2014	0.00	33	33445.05
2014 gggggnfwnyuufwu	111	08APR2014	17APR2014	0.00	9	10288.20

select year, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt
from medicare_national.inpatient_base_claims_k
where year = '2015' and bene_id = 'gggggggBAfjwwwn'
order by clm_from_dt::date;
2015	gggggnnfgyynngf	111	18MAR2015	23MAR2015	0.00	5	11101.53
2015	gggggnngyuugaAw	112	23MAR2015	31MAR2015	0.00	9	7868.07
2015	gggggnnjfffAgnA	114	01APR2015	21APR2015	0.00	20	20474.29
2015	gggggnufgwnajAn	111	09AUG2015	13AUG2015	0.00	4	13194.60

select year, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt
from medicare_national.inpatient_base_claims_k
where year = '2019' and bene_id = 'gggggggjffgBawu'
order by clm_from_dt::date;
2019	ggggguuggByjnuw	111	15APR2019	19APR2019	0.00	4	20130.14
2019	ggggguABufAuajA	112	30SEP2019	30SEP2019	0.00	1	4528.13		<--112 and 114 should be one single admission
2019	ggggguAjwaAjfaw	114	01OCT2019	04OCT2019	0.00	3	17594.55
2019	ggggguAAwBAafyA	111	09DEC2019	16DEC2019	0.00	7	41053.94

select year, derived_uth_admission_id, admit_date, discharge_date, from_date_of_service, to_date_of_service, 
	charge_amount, member_id_src, claim_id_src
from data_warehouse.admission_acute_ip_claims
where derived_uth_admission_id in (
	select derived_uth_admission_id from data_warehouse.admission_acute_ip_claims
	where claim_id_src = 'ggggguAjwaAjfaw'
);
2019	532783167-001-2019	2019-10-01	2019-10-04	2019-10-01	2019-10-01	8.32	gggggggjffgBawu	ggggBwgaagAfwuA
2019	532783167-001-2019	2019-10-01	2019-10-04	2019-10-01	2019-10-04	278.18	gggggggjffgBawu	ggggBwgaagAfwuw
2019	532783167-001-2019	2019-10-01	2019-10-04	2019-10-01	2019-10-04	0.00	gggggggjffgBawu	ggggguAjwaAjfaw

--check out some more
select year, bene_id, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill from medicare_national.inpatient_base_claims_k
where clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd = '113';
2015	ggggggggaufawwu	gggggnAjufwfffu	113
2018	ggggggjyBAwfuBn	gggggujnBwAauBy	113
2020	ggggggBygAwwygj	ggggguanunnafBf	113
2015	ggggggguwajfjgu	gggggnufauBawyA	113

select year, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt,
	ptnt_dschrg_stus_cd
from medicare_national.inpatient_base_claims_k
where year in ('2018', '2019') and bene_id = 'ggggggjyBAwfuBn'
	and clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd in ('112', '113', '114')
order by clm_from_dt::date;
2018	ggggguBwByfAwwa	112	13MAR2018	11APR2018	473176.15	0.00	30	118438.19	30
2018	gggggujnBwAauBy	113	12APR2018	11MAY2018	323941.31	0.00	30	110659.74	30
2018	gggggujufBnBjAA	114	12MAY2018	17MAY2018	34801.92	0.00	5	17239.52	01

/**********
 * isrrael built 2018 medicare national admits so let's check
 */
select * from dev.gm_dw_ip_admit_claim where member_id_src = 'ggggggjyBAwfuBn' and claim_id_src = 'ggggguBwByfAwwa';
--admit id 666515635-001-2018

select admit_id, admit_date, discharge_date, member_id_src, claim_id_src
from dev.gm_dw_ip_admit_claim where member_id_src = 'ggggggjyBAwfuBn' and claim_type = 'F' order by admit_date;

--what about when there's no 113
select year, bene_id, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt,
	ptnt_dschrg_stus_cd
from medicare_national.inpatient_base_claims_k
where year in ('2018') 
	and clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd = '114';
2018	gggggggfgByBBBy	ggggguBwafAaBAB	114	01JUL2018
2018	gggggggfnnwfguw	ggggguBujawjyfy	114	12JUN2018
2018	gggggggunuwBuaB	gggggujgawBwjay	114	30APR2018
2018	gggggggfuawnABA	ggggguBygBjBjBn	114	01JUL2018
2018	ggggggByggfnujf	gggggujjajAnnju	114	01SEP2018

select year, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt,
	ptnt_dschrg_stus_cd
from medicare_national.inpatient_base_claims_k
where year in ('2018') and bene_id = 'gggggggfgByBBBy'
	and clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd in ('112', '113', '114')
order by clm_from_dt::date;
2018	ggggguBAyfAnguB	112	27JUN2018	30JUN2018	7327.57	0.00	4	5436.95	30
2018	ggggguBwafAaBAB	114	01JUL2018	02JUL2018	1507.68	0.00	1	1388.87	06

--112-114 works just fine
select admit_id, admit_date, discharge_date, member_id_src, claim_id_src
from dev.gm_dw_ip_admit_claim where member_id_src = 'gggggggfgByBBBy' and claim_type = 'F' order by admit_date;

/**************************
 * bcarrier/dme tables - where is the coinsurance hiding?
 * 	carr_clm_cash_ddctbl_apld_amt should = sum(deductible)
 *  clm_bene_pd_amt = ???
 */

select year, bene_id, clm_id, carr_clm_cash_ddctbl_apld_amt, clm_bene_pd_amt
from medicare_national.bcarrier_claims_k
where carr_clm_cash_ddctbl_apld_amt != '0.00' and clm_bene_pd_amt != '0.00'
and year = '2020';
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	101.11	80.01
2020	gggggggfguuunyg	ggggBwjaffagyBB	79.24	15.00

select year, bene_id, clm_id, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt,
	line_othr_apld_amt1
from medicare_national.bcarrier_line_k
where clm_id = 'ggggBwjnBAfgjgn';
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	101.11	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
2020	ggggggjnaaAgAgj	ggggBwjnBAfgjgn	0.00	0.00
--wow that does not match up at 

--while we're here let's look at line_alowd_chrg_amt vs car_line_cl_chrg_amt
/*from dictionary:
 * 	line allowed charge amount = amt of lallowed charges for line item, used to compute pay to providers or reimbursement to beneficiaries
 *  car_line_cl_chrg_amt = clinical lab charge amount on the carrier line
 * 
 * uh-oh... so do we need to add up charge amounts?
 * 
 * Answer: NO - it's a duplication when there's a carrier line clinic charge amount
 */

select line_sbmtd_chrg_amt, carr_line_cl_chrg_amt from medicare_national.bcarrier_line_k
where carr_line_cl_chrg_amt != '0.00';
75.00	75.00
50.00	50.00
90.00	90.00
20.00	20.00
4.31	4.31
14.37	14.37




/****************************************************
 * Part D exploration
 */

--what columns we got?

select column_name, ordinal_position
from information_schema.columns
where table_schema = 'medicare_national' and table_name = 'pde_file'
and (column_name like '%amt' or column_name like '%rptd_gap%')
order by ordinal_position;

gdc_blw_oopt_amt	26
gdc_abv_oopt_amt	27
ptnt_pay_amt	28
othr_troop_amt	29
lics_amt	30
plro_amt	31
cvrd_d_plan_pd_amt	32
ncvrd_plan_pd_amt	33
tot_rx_cst_amt	34
rptd_gap_dscnt_num	44

--ok so... what's positive, what's negative, and what's null?

select sum(case when ncvrd_plan_pd_amt = '0.00' then 1 else 0 end) *1.0 / count(*) as zero,
	sum(case when ncvrd_plan_pd_amt::float > 0.005 then 1 else 0 end) *1.0 / count(*) as positive,
	sum(case when ncvrd_plan_pd_amt::float < -0.005 then 1 else 0 end) *1.0 / count(*) as negative, 
	count(*)
from medicare_national.pde_file
where year = '2019';

select ncvrd_plan_pd_amt, case when ncvrd_plan_pd_amt = '0.00' then 1 else 0 end as zero
from medicare_national.pde_file;

select count(*) from medicare_national.pde_file where ncvrd_plan_pd_amt is null and year = '2020';


































