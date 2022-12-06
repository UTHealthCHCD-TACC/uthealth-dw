select distinct len(ndc), count(len(ndc)) from medicaid.dbo.CHIP_RX_FY12
group by len(ndc);
--ALL 11

select distinct len(ndc), count(len(ndc)) from medicaid.dbo.FFS_RX_FY12
group by len(ndc);
--Almost all 11, 89x 0, 9x 10-digit

select distinct len(ndc), count(len(ndc)) from medicaid.dbo.MCO_RX_FY12
group by len(ndc);
--almost all 11, 2 x 10-digit

select * from medicaid.dbo.FFS_RX_FY12
where len(ndc) = 10;


select distinct len(ndc), count(len(ndc)) from medicaid.dbo.FFS_RX_FY12
group by len(ndc);

select top 5 * from medicaid.dbo.FFS_RX_FY12;

select count(distinct ndc) as distinct_ndc, count(ndc) as count_ndc from medicaid.dbo.FFS_RX_FY12;


select count(distinct rx_nbr) as distinct_rxnbr, count(rx_nbr) as count_rxnbr from medicaid.dbo.FFS_RX_FY12;
--TCN is not used
--concat RX NBR + SEQ NUMBER?


select count(distinct tcn) as distinct_tcn, count(tcn) as count_tcn from medicaid.dbo.FFS_RX_FY12;

select top 10 rx_nbr from medicaid.dbo.FFS_RX_FY12
order by rx_nbr;

select top 10 rx_nbr from medicaid.dbo.FFS_RX_FY13
order by rx_nbr;


select top 20 * from medicaid.dbo.FFS_RX_FY12
order by rx_nbr;

select tcn, count(tcn) count_tcn from medicaid.dbo.FFS_RX_FY12
group by TCN
having count(tcn) > 1;

11251200011465260	3
11283200011144330	4
11258200200154231	3
12013200010692220	3
11256200012377130	3
12233200010527320	4
12227200010010140	3
12100200010508370	3
12171200010224500	3
11257200010751850	3
11341200011156750	3
11280200011772470	3
12234200010261180	4
11284200012063890	3







select * from medicaid.dbo.FFS_RX_FY12 where tcn = 11283200011144330;

select * from medicaid.dbo.FFS_RX_FY13 where pcn = '529014218';
select * from medicaid.dbo.MCO_RX_FY13 where pcn = '529014218' and ndc = '45802011222';
select * from medicaid.dbo.CHIP_RX_FY13 where pcn = '529014218';



select pcn, ndc, rx_fill_dt, rx_nbr, seq_nbr, prescriber_nbr, claim_status, amount_paid, gross_amt_due, TCN, PREV_TCN from medicaid.dbo.MCO_RX_FY13 where pcn = '529014218' and ndc = '45802011222';




select prescriber_nbr, ndc, rx_nbr, seq_nbr, claim_status, rx_quantity, DISP_EXP_AMT, drug_cost, amount_paid, gross_amt_due, TCN, PREV_TCN 
from medicaid.dbo.FFS_RX_FY12 where tcn =  12233200010527320;

select * from medicaid.dbo.FFS_RX_FY12 where tcn =  12233200010527320;


--FOR WORD DOC HERE
select count(distinct tcn) as distinct_tcn, count(tcn) as count_tcn from medicaid.dbo.MCO_RX_FY12;

select count(distinct tcn) as distinct_tcn, count(tcn) as count_tcn from medicaid.dbo.MCO_RX_FY21;
/*
distinct_tcn	count_tcn
30004287	30004287
*/

select top 10 tcn from medicaid.dbo.FFS_RX_FY13;

select len(tcn) as tcn_length, count(len(tcn)) as count from medicaid.dbo.FFS_RX_FY13
group by len(tcn)
order by len(tcn);

select claim_status, substring(tcn, 17, 1) as last_digit from medicaid.dbo.FFS_RX_FY13
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);

select claim_status, substring(tcn, 17, 1) as last_digit from medicaid.dbo.FFS_RX_FY21
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);


select claim_status, substring(tcn, 17, 1) as last_digit, count(*) as count from medicaid.dbo.MCO_RX_FY21
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);
--claim_status	last_digit	count
--PD			0			30004287


select claim_status, substring(tcn, 17, 1) as last_digit, count(*) as count from medicaid.dbo.FFS_RX_FY21
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);

--claim_status	last_digit	count
--PD	0	611807
--PR	0	81

select claim_status, substring(tcn, 17, 1) as last_digit, count(*) as count from medicaid.dbo.CHIP_RX_FY21
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);

--claim_status	last_digit	count
--PD	0	808699


select claim_status, substring(tcn, 17, 1) as last_digit, count(*) as count from medicaid.dbo.MCO_RX_FY15
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);
--claim_status	last_digit	count
--PR	0	926314
--PD	0	34389951
--RV	1	2241075

select claim_status, substring(tcn, 17, 1) as last_digit, count(*) as count from medicaid.dbo.FFS_RX_FY15
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);

--claim_status	last_digit	count
--PD		1
--PR	0	1132667
--PD	0	4862712
--RV	1	1144393

select claim_status, substring(tcn, 17, 1) as last_digit, count(*) as count from medicaid.dbo.CHIP_RX_FY15
group by claim_status, substring(tcn, 17, 1)
order by substring(tcn, 17, 1);
--claim_status	last_digit	count
--PD	0	1579167
--PR	0	94504
--RV	1	150826

select top 100 * from medicaid.dbo.CHIP_RX_FY15
order by rx_nbr;

select top 10 rx_nbr, count(rx_nbr) as count from medicaid.dbo.CHIP_RX_FY15
group by rx_nbr
order by count(rx_nbr) desc;
/*
rx_nbr	count
000000102407	61
000006049835	48
000000936229	44
000006043818	44
000000050484	42
000000082421	40
000006121892	37
000006127896	36
000006046608	36
000006050417	36
*/

select * from medicaid.dbo.CHIP_RX_FY15
where rx_nbr = '000006049835'
order by ndc, refill_nbr;


select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN
from medicaid.dbo.CHIP_RX_FY15
where rx_nbr = '000006049835'
order by ndc, refill_nbr;

select concat(rx_nbr, replace(rx_dt, '-', '')) as rx_nbr_concat, rx_nbr, rx_dt, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt
from medicaid.dbo.CHIP_RX_FY15
where rx_nbr = '000006049835'
order by ndc, refill_nbr;



select *
from medicaid.dbo.CHIP_RX_FY15
where rx_nbr = '000006049835'
order by ndc, refill_nbr;

select count(*) from medicaid.dbo.mco_rx_fy17;
select count(*) from medicaid.dbo.mco_rx_fy18;



select count(distinct tcn) as distinct_tcn, count(tcn) as count_tcn from medicaid.dbo.FFS_RX_FY13;

--get rows where TCN is not unique
drop table if exists work.dbo.xz_temp_mcdrx;

select tcn, count(*) as count
into work.dbo.xz_temp_mcdrx
from medicaid.dbo.FFS_RX_FY13
group by tcn
having count(*) > 1;

select top 50 * from medicaid.dbo.FFS_RX_FY13
where tcn in (select tcn from work.dbo.xz_temp_mcdrx where count > 3)
order by TCN;

--get column names
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'FFS_RX_FY13'

--get counts
drop table if exists work.dbo.xz_temp_mcdrx2;

select count(distinct tcn) as tcn,
count(*) as rows,
count(distinct PCN) as PCN,
count(distinct phmcy_nbr) as phmcy_nbr,
count(distinct rx_nbr) as rx_nbr,
count(distinct seq_nbr) as seq_nbr,
count(distinct rx_dt) as rx_dt,
count(distinct auth_refill) as auth_refill,
count(distinct prescriber_nbr) as prescriber_nbr,
count(distinct rx_fill_dt) as rx_fill_dt,
count(distinct ndc) as ndc,
count(distinct claim_status) as claim_status,
count(distinct rx_quantity) as rx_quantity,
count(distinct rx_days_supply) as rx_days_supply,
count(distinct client_location) as client_location,
count(distinct refill_nbr) as refill_nbr,
count(distinct amount_paid) as amount_paid,
count(distinct payment_dt) as payment_dt,
count(distinct unlimited_flag) as unlimited_flag,
count(distinct client_county) as client_county,
count(distinct phmcy_region) as phmcy_region,
count(distinct DISP_EXP_AMT) as DISP_EXP_AMT,
count(distinct drug_cost) as drug_cost,
count(distinct gcn_seq_nbr) as gcn_seq_nbr,
count(distinct gross_amt_due) as gross_amt_due,
count(distinct hmo_plan_id) as hmo_plan_id,
count(distinct client_dob) as client_dob,
count(distinct client_sex) as client_sex,
count(distinct npi) as npi,
count(distinct sig) as sig,
count(distinct cat) as cat,
count(distinct med_cov) as med_cov,
count(distinct tp) as tp,
count(distinct sd) as sd,
count(distinct bp) as bp,
count(distinct prescriber_npi) as prescriber_npi,
--count(distinct TCN) as TCN,
count(distinct PREV_TCN) as PREV_TCN,
count(distinct qty_prescribed) as qty_prescribed,
count(distinct unit_of_meas) as unit_of_meas
into work.dbo.xz_temp_mcdrx2
from medicaid.dbo.ffs_rx_fy13
where tcn in (select tcn from work.dbo.xz_temp_mcdrx)
group by tcn;

--table of icns and how many rows are occupado
select * from work.dbo.xz_temp_mcdrx2

select distinct SEQ_NBR from medicaid.dbo.ffs_rx_fy21;
select distinct SEQ_NBR from medicaid.dbo.ffs_rx_fy13;
--seqnbr is not useful

--make fake id number
select *, CONCAT(PCN, NDC, RX_FILL_DT) as concatenated
into work.dbo.xz_temp_mcdrx3
from medicaid.dbo.FFS_RX_FY13;

select count(*) as rows, count(distinct concatenated) as distinct_concatenated
from work.dbo.xz_temp_mcdrx3;

--get rows where the count for fake id number is > 1
select concatenated, count(*) as count
into work.dbo.xz_temp_mcdrx4
from work.dbo.xz_temp_mcdrx3
group by concatenated
having count(*) > 1;

select count(*) from work.dbo.xz_temp_mcdrx4; --1,035,195, distinct concatenated vs 83,945 distinct TCNs

select count(*) from medicaid.dbo.ffs_rx_fy13; -- 8643166
select count(*) from medicaid.dbo.mco_rx_fy13; --40501811

drop table if exists work.dbo.xz_temp_mcdrx5;

select count(distinct concatenated) as concatenated,
count(*) as rows,
count(distinct PCN) as PCN,
count(distinct phmcy_nbr) as phmcy_nbr,
count(distinct rx_nbr) as rx_nbr,
count(distinct seq_nbr) as seq_nbr,
count(distinct rx_dt) as rx_dt,
count(distinct auth_refill) as auth_refill,
count(distinct prescriber_nbr) as prescriber_nbr,
count(distinct rx_fill_dt) as rx_fill_dt,
count(distinct ndc) as ndc,
count(distinct claim_status) as claim_status,
count(distinct rx_quantity) as rx_quantity,
count(distinct rx_days_supply) as rx_days_supply,
count(distinct client_location) as client_location,
count(distinct refill_nbr) as refill_nbr,
count(distinct amount_paid) as amount_paid,
count(distinct payment_dt) as payment_dt,
count(distinct unlimited_flag) as unlimited_flag,
count(distinct client_county) as client_county,
count(distinct phmcy_region) as phmcy_region,
count(distinct DISP_EXP_AMT) as DISP_EXP_AMT,
count(distinct drug_cost) as drug_cost,
count(distinct gcn_seq_nbr) as gcn_seq_nbr,
count(distinct gross_amt_due) as gross_amt_due,
count(distinct hmo_plan_id) as hmo_plan_id,
count(distinct client_dob) as client_dob,
count(distinct client_sex) as client_sex,
count(distinct npi) as npi,
count(distinct sig) as sig,
count(distinct cat) as cat,
count(distinct med_cov) as med_cov,
count(distinct tp) as tp,
count(distinct sd) as sd,
count(distinct bp) as bp,
count(distinct prescriber_npi) as prescriber_npi,
count(distinct TCN) as TCN,
count(distinct PREV_TCN) as PREV_TCN,
count(distinct qty_prescribed) as qty_prescribed,
count(distinct unit_of_meas) as unit_of_meas
into work.dbo.xz_temp_mcdrx5
from work.dbo.xz_temp_mcdrx3
where concatenated in (select top 50000 concatenated from work.dbo.xz_temp_mcdrx4)
group by concatenated;

select * from work.dbo.xz_temp_mcdrx3
where concatenated in (select top 5 concatenated from work.dbo.xz_temp_mcdrx4)
order by concatenated;


SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
order by table_name;


SELECT table_name, column_name, ordinal_position, data_type
FROM INFORMATION_SCHEMA.COLUMNS
order by table_name;

SELECT table_name, max(ordinal_position) as numvars
from INFORMATION_SCHEMA.columns
group by table_name
order by table_name;

SELECT distinct table_name
FROM INFORMATION_SCHEMA.COLUMNS;

select top 10 mco_icn, tx_cd, sub_mco_pln, derv_enc from medicaid.dbo.enc_dx_21;

select len(icn), count(len(icn)) from medicaid.dbo.clm_dx_21
group by len(icn);

select top 10 * from medicaid.dbo.ADMIT_CLM;



select top 50 a.rx_nbr as rx_nbr, b.rx_nbr as FY15, c.rx_nbr as FY16
from ((select rx_nbr from medicaid.dbo.MCO_RX_FY15
		union
		select rx_nbr from medicaid.dbo.MCO_RX_FY16)) a
	left join (select distinct rx_nbr from medicaid.dbo.MCO_RX_FY15) b on a.rx_nbr = b.rx_nbr
	left join (select distinct rx_nbr from medicaid.dbo.MCO_RX_FY16) c on a.rx_nbr = c.rx_nbr
order by a.rx_nbr;

select top 5 * from medicaid.dbo.MCO_RX_FY15;


drop table if exists work.dbo.xz_mcdrx_temp1;
select CONCAT(PCN, NDC, replace(rx_fill_dt, '-', '')) as PCN_NDC_FILLDT, z.*
into work.dbo.xz_mcdrx_temp1
from (select '2015' as FY, * from medicaid.dbo.MCO_RX_FY15
	union all
	select '2016' as FY, * from medicaid.dbo.MCO_RX_FY16) z

select count(*) as count, count(distinct PCN_NDC_FILLDT) as PCN_NDC_FILLDT
from work.dbo.xz_mcdrx_temp1;




select count(*) as count, count(distinct PCN_NDC_FILLDT) as PCN_NDC_FILLDT
from work.dbo.xz_mcdrx_temp1
where ndc = '00000000000';


select count(*) as count, count(distinct PCN_NDC_FILLDT) as PCN_NDC_FILLDT
from work.dbo.xz_mcdrx_temp1
where ndc = '00000000000' or ndc is null or len(ndc) != 11;


select top 10 PCN_NDC_FILLDT, count(*)
from work.dbo.xz_mcdrx_temp1
group by PCN_NDC_FILLDT
order by count(*) desc;

/*
PCN_NDC_FILLDT	(No column name)
4168724011743398760320140909	26
5225301320060300263220141023	26
5285778800037836320520140910	25
5074247495615110300120141008	22
5181591130090479157020141002	22
5104302310829032491020141003	22
4168724011743398760320140906	22
5239059576586202115020160427	22
5253920556050500960020141006	22
5261028515880909990120140902	22 */

--Let's just take a subset of fields that actually change
select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN
from work.dbo.xz_mcdrx_temp1
where PCN_NDC_FILLDT = '4168724011743398760320140909'
order by TCN;

--try another one
select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN
from work.dbo.xz_mcdrx_temp1
where PCN_NDC_FILLDT = '5261028515880909990120140902'
order by TCN;


drop table if exists work.dbo.xz_mcdrx_temp2;
select *
into work.dbo.xz_mcdrx_temp2
from work.dbo.xz_mcdrx_temp1
where claim_status != 'RV' and
TCN not in (select PREV_TCN from work.dbo.xz_mcdrx_temp1 where claim_status = 'RV')
and ndc != '00000000000';

select count(*) as count, count(distinct PCN_NDC_FILLDT) as PCN_NDC_FILLDT
from work.dbo.xz_mcdrx_temp2;

select top 10 PCN_NDC_FILLDT from work.dbo.xz_mcdrx_temp1
where PCN_NDC_FILLDT not in (select PCN_NDC_FILLDT from work.dbo.xz_mcdrx_temp2);

/*PCN_NDC_FILLDT
1111186014354702761120150812
1111186014354702761120150812
1122290015374602530520150817
1122290015374602530520150817
1187362010014331425020150811
1187362010014331425020150811
1263727010037818031020150720
1263727010037818031020150720
1266521010009320495620150417
1266521010009320495620150417 */

--Here's a PCN-NCD-FILLDT combo we've lost
select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN
from work.dbo.xz_mcdrx_temp1
where PCN_NDC_FILLDT = '1266521010009320495620150417'
order by TCN;

select top 10 PCN_NDC_FILLDT as PCN_NDC_FILLDT, count(*) as count
from work.dbo.xz_mcdrx_temp2
group by PCN_NDC_FILLDT
order by count(*) desc;

/*PCN_NDC_FILLDT	count
5229470810018650403120150612	6
5184918506459703016020150623	5
5253603505880908296020150615	5
5111227500009372065620150626	5
5113373500016936871220150610	5
2743853010009331450120160502	5
5261994084456707081020151021	5
5144595507643901310420150617	5
5277462876787701060120150606	5
6164063731063104070120150505	5*/

select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN 
from work.dbo.xz_mcdrx_temp2
where PCN_NDC_FILLDT = '5261994084456707081020151021'
order by TCN;

select a.PCN_NDC_FILLDT, a.FY as 'FY2015', b.FY as 'FY2016'
from (select PCN_NDC_FILLDT, FY from work.dbo.xz_mcdrx_temp1 where FY = '2015') a
inner join (select PCN_NDC_FILLDT, FY from work.dbo.xz_mcdrx_temp1 where FY = '2016') b on a.PCN_NDC_FILLDT = b.PCN_NDC_FILLDT

--this is 46 rows
select * from medicaid.dbo.FFS_RX_FY13 where pcn = '517923144' and ndc = '00187301330' order by tcn;

select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN
from medicaid.dbo.FFS_RX_FY13 where pcn = '517923144' and ndc = '00187301330' order by tcn;

select rx_nbr, rx_fill_dt, count(distinct(payment_dt)) from medicaid.dbo.FFS_RX_FY13
group by rx_nbr, rx_fill_dt
order by count(distinct(payment_dt)) desc;

/*000000355212	2012-12-05	8
000001294187	2013-06-21	6
000001645514	2013-02-06	6
000001881322	2013-01-09	5
000000065874	2012-09-25	5
000000711353	2013-05-30	5
000000068881	2013-05-29	5
000000478958	2012-10-12	5
000001747579	2013-06-06	5
000001881319	2013-01-11	5
000000770999	2013-01-15	5
000000966954	2013-07-17	5
000001263537	2013-06-03	5
000000580799	2012-09-06	5*/

select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN
from medicaid.dbo.FFS_RX_FY13 where rx_nbr = '000001294187' order by tcn;

select rx_nbr, rx_dt, seq_nbr, rx_fill_dt, ndc, claim_status, refill_nbr, amount_paid, drug_cost, gross_amt_due, payment_dt, tcn, PREV_TCN
from medicaid.dbo.FFS_RX_FY13 where rx_nbr = '000001294187' order by tcn;

--

select * from medicaid.dbo.chip_rx_fy12;
select * from medicaid.dbo.ffs_rx_fy12;
select * from medicaid.dbo.mco_rx_fy12;


000000000 <--9 Digits in PCN

select count(*) as count
from medicaid.dbo.ffs_rx_fy12
where pcn is null; --none

select count(*) as count
from medicaid.dbo.ffs_rx_fy12
where trim(pcn) = ''; --none

select count(*) as count
from medicaid.dbo.ffs_rx_fy12
where pcn = '000000000'; --also none


select '2012' as fy, concat(pcn, ndc, replace(rx_fill_dt, '-', '')) as rx_id
into work.dbo.xz_dwqa_chiprx
from medicaid.dbo.chip_rx_fy12;

select '2012' as fy, concat(pcn, ndc, replace(rx_fill_dt, '-', '')) as rx_id
into work.dbo.xz_dwqa_ffsrx
from medicaid.dbo.ffs_rx_fy12;

select '2012' as fy, concat(pcn, ndc, replace(rx_fill_dt, '-', '')) as rx_id
into work.dbo.xz_dwqa_mcorx
from medicaid.dbo.mco_rx_fy12;

DELETE FROM work.dbo.xz_dwqa_chiprx WHERE substring(rx_id, 1, 9) = '000000000';
DELETE FROM work.dbo.xz_dwqa_ffsrx WHERE substring(rx_id, 1, 9) = '000000000';
DELETE FROM work.dbo.xz_dwqa_mcorx WHERE substring(rx_id, 1, 9) = '000000000';



select a.fy, count(distinct a.rx_id) from work.dbo.xz_dwqa_chiprx;

select a.fy, count(distinct a.rx_id) from work.dbo.xz_dwqa_ffsrx;

select a.fy, count(distinct a.rx_id) from work.dbo.xz_dwqa_mcorx;


with a as (select fy, count(distinct rx_id) as chip from work.dbo.xz_dwqa_chiprx group by fy),
b as (select fy, count(distinct rx_id) as ffs from work.dbo.xz_dwqa_ffsrx group by fy),
c as (select fy, count(distinct rx_id) as mco from work.dbo.xz_dwqa_mcorx group by fy)

select a.fy, a.chip, b.ffs, c.mco, (a.chip + b.ffs + c.mco) as sum
from a left join b on a.fy = b.fy left join c on a.fy = c.fy
order by a.fy;


select top 10 rx_id from work.dbo.xz_dwqa_chiprx tablesample(500 rows);

select top 10 rx_id from work.dbo.xz_dwqa_ffsrx tablesample(500 rows);

select top 10 rx_id from work.dbo.xz_dwqa_mcorx tablesample(500 rows);


select top 10 a.rx_id from work.dbo.xz_dwqa_chiprx a inner join work.dbo.xz_dwqa_ffsrx b
on a.rx_id = b.rx_id; -- ran for 5 mins, I canceled

select top 10 a.pcn, a.ndc, a.


---

select top 50
'2012' as spc_fy,
'chip' as spc_table,
rx_fill_dt as spc_fill_date,
ndc as spc_ndc,
rx_days_supply as spc_days_supply,
rx_nbr as spc_script_id,
refill_nbr as spc_refill_count,
rx_quantity as spc_quantity,
prescriber_npi as spc_provider_npi,
phmcy_nbr as spc_pharmacy_id,
gross_amt_due  as spc_total_charge_amount,
amount_paid as spc_total_paid_amount,
pcn as spc_member_id_src

from medicaid.dbo.ffs_rx_fy12 tablesample(6000 rows);

select count(*) from medicaid.dbo.FFS_RX_FY18_19_HTW; --392868

-- QTY mismatches

--memid____ ndc________ date____
--530233406 62037099910 20140102

select pcn, ndc, rx_fill_dt, rx_quantity from medicaid.dbo.ffs_rx_fy14
where pcn = '530233406' and ndc = '62037099910';

--checking for CHIP_FFS_RX data

select top 10 concat(pcn, ndc, replace(rx_fill_dt, '-', '')) as rx_id from medicaid.dbo.CHIP_FFS_RX_FY12;

A632897026340205100120120220
A632897026340205100120120220
A632897026340205100120120220
A632897026340205100120111120
A632897035914800071320111023
A632897036340205100120111120
A632897035914800081320120225
A632897035914800081320120225
A632897035914800081320120225
A632897030037820087720120223

select top 10 * from medicaid.dbo.CHIP_FFS_RX_FY12;

--TCNS
12051200200029611
12051200010280100
12051200010326470
11324200010509480
11296200010177470
11324200010509470
12056200010551220
12056200010538970
12056200200062181
12054200012132250
