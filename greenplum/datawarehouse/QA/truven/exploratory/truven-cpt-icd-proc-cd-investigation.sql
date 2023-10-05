/************************
 * 06/21/2023
 * 
 * Xiaorui and Femi looked at CPT/ICD proc codes in Truven to try to figure out what codes are in what columns
 * 
 * Long story short: Yes, the codes are mixed up in the columns. Yes, there are a lot of mixed up codes,
 * but that is because Truven has A LOT OF DATA.
 * The actual percentage of codes in the wrong column is very small (< 1%)
 * 
 * It would be nice to do some cleaning, of course! But it's not an urgent need.
 */

/***************
 * Question: what procedure types are in S tables?
 * 
 * Ans: Mostly CPT and HCPCS codes, some CDT, a very small number ICD-9-CM codes
 */
select proctyp, count(*) from truven.ccaes
group by proctyp;

/*
*         	17896
1         	257434854
7         	8105564
8         	6915
[null]		309615682

FROM TRUVEN DATA DICTIONARY
*: ICD-9-CM
*0: ICD-10-CM
*1: CPT
*3: UB92 Revenue Code
*6: NABSP
*7: HCPC
*8: CDT (ADA)

*/

--What percentage is ICD-9-CM?
select 17896.0/(309615682 + 257434854);
--0.000031559797344058942923

/***********************
 * Question: Are there CPT codes mixed into ICD CODE columns in F tables?
 * They're supposed to be ICD PROC codes
 * 
 * Answer: YES but not that many, and some may be legitimately ICD PROC codes but 5 digits
 */

select count(*), sum(case when length(trim(proc1)) = 5 then 1 else 0 end) as possible_cpt
from truven.ccaef
where proc1 is not null;
/*
count		possible_cpt
245832831	805072	*/

--what percentage is that?
select 805072.0/245832831;
--0.00327487584439036949

--take a look at these possible cpt codes
select proc1
from truven.ccaef
where length(trim(proc1)) = 5;

/**********************
 * Looking at the raw tables, it's kind of hard to gather all the columns because there's proc1 - proc15
 * so let's instead look at the linearized version on DW to see how many codes are misplaced
 */

--Look at how many codes in the CPT_HCPCS_CD column are NOT CPT codes
select count(*), sum(case when length(trim(cpt_hcpcs_cd)) != 5 then 1 else 0 end) as not_len_5
from data_warehouse.claim_detail_1_prt_truv;
/*
count		not_len_5
10593715668	110943 */

--what percentage?
select 110943.0/10593715668;
--0.000010472529514372463711

--look at how many codes in the icd_proc_codes table are 5 digits, noting that some
--of these may legitimately be 5 digits
select count(*), sum(case when length(trim(proc_cd)) = 5 then 1 else 0 end) as length_5
from data_warehouse.claim_icd_proc_1_prt_truv;
211860038	1106346

--what percentage?
select 1106346.0/211860038;
--0.00522206080223586102


























