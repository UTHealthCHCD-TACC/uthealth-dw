/***************************
 * 7/25/2023 Discussion topics
 */


/**************************
 * TOPIC 1: Bill type 112, 113, and 114 - how are claims split up amongst them?
 * 
 * Answer: Charges are not duplicated on final bill. To accurately get the cost of a long-term
 * inpatient stay, we need to sum up amounts across bill types 112 and 113
 * 
 * Already spoke to Lopita/Isrrael and we are editing the admit logic
 */

--Example: this person had some bill types worth considering
--	1) Bill types 112, 113, and 114 - the classic combination
--  2) Bill types 112 and 114 alone - these are fairly frequently seen as well
--	3) Bill type 117 - should this be added as part of the admissions definition?
select year, clm_id, clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
	clm_from_dt, clm_thru_dt, clm_tot_chrg_amt, clm_pass_thru_per_diem_amt, clm_utlztn_day_cnt, clm_pmt_amt,
	ptnt_dschrg_stus_cd
from medicare_national.inpatient_base_claims_k
where year in ('2018', '2019') and bene_id = 'ggggggjyBAwfuBn'
order by clm_from_dt::date;


/********************
 * Answering question from last session:
 * ncvrd charge - is the whole line denied or is there partial coverage?
 * 
 * Answer: it looks like partial coverage is a thing, although entirely not covered is more common
 */

select year, bene_id, clm_id, rev_cntr, rev_cntr_unit_cnt, rev_cntr_rate_amt, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt
from medicare_national.inpatient_revenue_center_k
where abs(rev_cntr_ncvrd_chrg_amt::float - 0) >= 0.01
and year = '2020';

--what revenue codes are most frequently not covered?
with a as (
	select year, bene_id, clm_id, rev_cntr, rev_cntr_unit_cnt, rev_cntr_rate_amt, rev_cntr_tot_chrg_amt, rev_cntr_ncvrd_chrg_amt,
		case when abs(rev_cntr_ncvrd_chrg_amt::float - 0) >= 0.01 and rev_cntr_tot_chrg_amt = rev_cntr_ncvrd_chrg_amt then '3 - all denied'
			when abs(rev_cntr_ncvrd_chrg_amt::float - 0) >= 0.01 and rev_cntr_tot_chrg_amt::float - rev_cntr_ncvrd_chrg_amt::float >= 0.01 then '2 - partially denied'
			when abs(rev_cntr_ncvrd_chrg_amt::float - 0) >= 0.01 and rev_cntr_tot_chrg_amt::float - rev_cntr_ncvrd_chrg_amt::float <= -0.01 then 'erroneous'
			else '1 - not denied' end as denial_status
	from medicare_national.inpatient_revenue_center_k
	where year = '2020'
),
b as (select rev_cntr, count(*) as total_count from medicare_national.inpatient_revenue_center_k
	where year = '2020' group by rev_cntr)
select a.rev_cntr, a.denial_status, count(a.*), b.total_count, count(a.*) * 1.0 / b.total_count as percentage
from a left join b on a.rev_cntr = b.rev_cntr
where denial_status != '1 - not denied'
group by 1, 2, 4 
having count(a.*) > 50
order by count(a.*) * 1.0 / b.total_count desc
;

top rev_codes
rev		denial_status	cnt	tot	percentage
0994	3 - all denied	263	263	1.00000000000000000000 	--patient convenience item tv/radio
0999	3 - all denied	85	85	1.00000000000000000000	--patient convenience item other
0993	3 - all denied	247	247	1.00000000000000000000	--patient convenience item telephone/telegraph
0991	3 - all denied	107	107	1.00000000000000000000	--patient convenience item cafeteria/guest tray
0990	3 - all denied	180	180	1.00000000000000000000	--patient convenience item general classification
0154	3 - all denied	111	145	0.76551724137931034483  --room and board ward psychiatric
0919	3 - all denied	53	189	0.28042328042328042328	--behavioral health treatment/services - other
0771	3 - all denied	164	803	0.20423412204234122042  --preventative care services vaccine adminiatration
0100	3 - all denied	163	918	0.17755991285403050109  --all inclusive room and board + ancillary
0637	3 - all denied	1189 7891 0.150677987580788239	--self-administered drugs administered in an emergency situation not requiring detailed coding
0180	3 - all denied	73	796	0.09170854271356783920  --leave of absence general classification

0001	2 - partially denied	8348	454122	0.01838272534693320297		--total charge
0110	2 - partially denied	1482	82365	0.01799307958477508651		--private medical or general - general classification
0111	2 - partially denied	495	49437	0.01001274349171673038			--private medical or genreal - medical/surgical/gyn
0124	2 - partially denied	130	17927	0.00725163161711385062			--semi-private 2 bed (medical or general) - psychiatric
0250	2 - partially denied	1764	504661	0.00349541573452277866		--pharmacy - general classification

/***********************
 * 08/01/23 Questions
 */

/*********************
 * CLAIM LEVEL B Carrier
 * 
 * There are 2 colums that contain information about how much the beneficiary paid:
 * 
 * Carrier Claim Cash Deductible Applied Amount (carr_clm_cash_ddctbl_apld_amt)
 * 		The amount of cash deductible as submitted on the claim
 * 		Equal to sum of all line-level deductible amounts
 * 
 * Carrier Claim Beneficiary Paid Amount (clm_bene_pd_amt)
 * 		The amount paid by the beneficiary for the non-institutional Part B (carrier, or DMERC) claim
 * 
 * Question: Can we put clm_bene_pd_amt down as coinsurance?
 * 
 * Answer: it's too small, fuggedaboutit
 * 
 * 
 */

--what relationship, if any, exists between these two columns?
select carr_clm_cash_ddctbl_apld_amt, clm_bene_pd_amt from medicare_national.bcarrier_claims_k
where clm_bene_pd_amt != '0.00';
--Answer: They seem independent. clm_bene_pd_amt appears sto be unrelated to the deductible.

--what's the population rate of these fields?
select sum(case when carr_clm_cash_ddctbl_apld_amt != '0.00' then 1 else 0 end) as ddctb_nonzero,
sum(case when carr_clm_cash_ddctbl_apld_amt != '0.00' then 1 else 0 end) * 1.0 / count(*) as ddctb_nonzero_pct,
sum(case when clm_bene_pd_amt != '0.00' then 1 else 0 end) as bene_pd_nonzero,
sum(case when clm_bene_pd_amt != '0.00' then 1 else 0 end) * 1.0 / count(*) as bene_pd_nonzero_pct
from medicare_national.bcarrier_claims_k
where year = '2020';

ddctb_nonzero	ddctb_nonzero_pct	bene_pd_nonzero	bene_pd_nonzero_pct
3189464			0.07719480473463	90114			0.00218103500583693219

--Is clm_bene_pd_amt related to coinsurance on the line-level table?
select year, bene_id, clm_id, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt,line_othr_apld_amt1
from medicare_national.bcarrier_line_k where line_coinsrnc_amt != '0.00' and year='2020';
"year"	bene_id	clm_id	line_bene_ptb_ddctbl_amt	line_bene_prmry_pyr_pd_amt	line_coinsrnc_amt	line_othr_apld_amt1
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	110.88	0.00	2.97	0.24
--has both ptb ddctbl and line consrnc amt

select year, bene_id, clm_id, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt,line_othr_apld_amt1
from medicare_national.bcarrier_line_k where year = '2020' and clm_id = 'ggggBwAuggyayaA';
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	110.88	0.00	2.97	0.24
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	0.00	0.00	4.03	0.32
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	0.00	0.00	0.00	0.06
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	0.00	0.00	0.00	0.14
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	0.00	0.00	0.00	0.09
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	0.00	0.00	0.00	0.23

select year, bene_id, clm_id, carr_clm_cash_ddctbl_apld_amt, clm_bene_pd_amt
from medicare_national.bcarrier_claims_k where year = '2020' and clm_id = 'ggggBwAuggyayaA';
2020	ggggggjujjwaaAu	ggggBwAuggyayaA	110.88	0.00 --this says that clm_bene_pd_amt is 0

--find a claim with clm_bene_pd_amt
select year, clm_id, carr_clm_cash_ddctbl_apld_amt, clm_bene_pd_amt from medicare_national.bcarrier_claims_k
where clm_bene_pd_amt != '0.00' and year = '2020';
2020	ggggBwjuwaBuwnf	192.33	15.00
--for this claim, the beneficiary paid $15

--look at the line-level
select year, bene_id, clm_id, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt,line_othr_apld_amt1
from medicare_national.bcarrier_line_k where year = '2020' and clm_id = 'ggggBwjuwaBuwnf';
2020	ggggggggjjujyjB	ggggBwjuwaBuwnf	18.29	0.00	0.00	0.00
2020	ggggggggjjujyjB	ggggBwjuwaBuwnf	0.00	0.00	0.00	0.00
2020	ggggggggjjujyjB	ggggBwjuwaBuwnf	174.04	0.00	0.00	0.00
--it's not listed here as a deductible or coinsurance. could it be copay?

--let's look at if there are discrete values that look copay-ish
select clm_bene_pd_amt, count(*)
from medicare_national.bcarrier_claims_k
where clm_bene_pd_amt in ('10.00', '15.00', '20.00')
group by 1 order by 1;
--hm, it could be copay actually... pull it into R to look at distribution

--so a little bit yes, a little bit no. It looks like it's just... honestly, a catch-all column
--what are the numbers?
select sum(case when clm_bene_pd_amt in
		('5.00', '10.00', '15.00', '20.00', '25.00', '30.00', '35.00', '40.00', '45.00', '50.00')
		then 1 else 0 end) as copay_ish,
		sum(case when clm_bene_pd_amt in
		('5.00', '10.00', '15.00', '20.00', '25.00', '30.00', '35.00', '40.00', '45.00', '50.00')
		then 1 else 0 end) * 1.0 / count(*) as copay_ish
from medicare_national.bcarrier_claims_k
where clm_bene_pd_amt != '0.00';
--about 35% of non-zero amounts are copay-ish

--final test... what % of the money pie does it account for?
select sum(clm_pmt_amt::float) as clm_pmt_amt, sum(clm_bene_pd_amt::float) as bene_pt_amt,
	sum(clm_bene_pd_amt::float) / sum(clm_pmt_amt::float) as pct
from medicare_national.bcarrier_claims_k
where year = '2020';
0.001, so 0.1%

--I lied, let's look at if it counts for allowed amount
select year, clm_id, nch_carr_clm_alowd_amt, carr_clm_cash_ddctbl_apld_amt, clm_bene_pd_amt from medicare_national.bcarrier_claims_k
where clm_bene_pd_amt != '0.00' and year = '2020';
2020	ggggBwjBBunaaBn	145.75	145.75	166.00
2020	ggggBwjBBunaaBu	17.14	17.14	19.00

select year, bene_id, clm_id, line_alowd_chrg_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt,line_othr_apld_amt1
from medicare_national.bcarrier_line_k where year = '2020' and clm_id = 'ggggBwjBBunaaBn';
2020	ggggggjwaffnAyB	ggggBwjBBunaaBn	145.75	145.75	0.00	0.00	0.00
--it looks like even Medicare doesn't consider the clm_bene_pd_amt part of the allowed amount


/***************Medicaid documentation part B beneficiary responsibility info*****************

Part B deductible - $226/year before Medicare begins to pay

Assignment = agreement by provider to be paid directly by Medicare.
If provider doesn't accept assignment, then can charge you 15% over Medicare-approved payment amt
for most part B services (this is called the "limiting charge" and does not apply to DME)

********************************************************************/


/******************************
 * Examine sum(coins) from BCarrier for Medicare
 */

drop table if exists dev.xz_temp1;

create table dev.xz_temp1 as
select "year", bene_id, clm_id, sum(line_alowd_chrg_amt::float) as sum_allowed,
	sum(line_nch_pmt_amt::float) as sum_clm_pmt_amt,
	sum(line_bene_prmry_pyr_pd_amt::float) as sum_prmry_pyr_pd_amt,
	sum(line_bene_ptb_ddctbl_amt::float) as sum_ddctbl,
	sum(line_coinsrnc_amt::float) as sum_coins
from medicare_national.bcarrier_line_k
where "year" = '2020'
group by 1,2,3
;

select * from dev.xz_temp1 where sum_coins != 0 limit 3;
"year"	bene_id			clm_id			allowed	paid	prmy	deduct	coins
2020	ggggggjyggABuyg	ggggBwnAfjAwgAy	587.93	463.11	0.0		0.0		117.58
2020	ggggggjyAfAgjwg	ggggBwAwwaauAwf	75.23	60.18	0.0		0.0		15.05
2020	ggggggBwyyjyynw	ggggBwawAgAjnwy	87.12	70.81	0.0		0.0		17.42

--look at example 1
select "year", bene_id, clm_id, line_nch_pmt_amt,
	line_bene_prmry_pyr_pd_amt, line_bene_ptb_ddctbl_amt, line_coinsrnc_amt
from medicare_national.bcarrier_line_k
where "year" = '2020' and clm_id = 'ggggBwnAfjAwgAy';
2020	ggggggjyggABuyg	ggggBwnAfjAwgAy	331.58	0.00	0.00	84.59
2020	ggggggjyggABuyg	ggggBwnAfjAwgAy	82.01	0.00	0.00	20.57
2020	ggggggjyggABuyg	ggggBwnAfjAwgAy	49.52	0.00	0.00	12.42

select "year", bene_id, clm_id, nch_carr_clm_alowd_amt, clm_pmt_amt, carr_clm_prmry_pyr_pd_amt, carr_clm_cash_ddctbl_apld_amt
from medicare_national.bcarrier_claims_k
where year = '2020' and clm_id = 'ggggBwnAfjAwgAy';
2020	ggggggjyggABuyg	ggggBwnAfjAwgAy	587.93	463.11	0.00	0.00

select 117.58 / 587.93 as pct;
--0.19998979470345109112, so basically 20%, which is standard medicaid coinsurance












