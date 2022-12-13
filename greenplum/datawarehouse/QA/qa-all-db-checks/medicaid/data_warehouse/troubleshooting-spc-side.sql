select count(distinct pcn) from work.dbo.xz_5a_mcd_obesity_cohort;
--5,145,240

select * from work.dbo.xz_5a_mcd_obesity_cohort where pcn = '102399401'

select * from stage.dbo.AGG_ENRL_MCD_FY where CLIENT_NBR = '102399401'

select count(distinct pcn) from work.dbo.xz_5a_mcd_enrl_dec
where ENRL_FY= 2021;
--5,145,240

select count(distinct client_nbr) from stage.dbo.AGG_ENRL_MCD_FY
where ENRL_FY= 2021;
--5,766,346

select client_nbr, ELIG_DATE, ME_CODE from medicaid.dbo.enrl_2021 where client_nbr = '506374908'
/*
506374908	202108	R
506374908	202107	R
506374908	202106	R
506374908	202105	R
506374908	202104	R
506374908	202103	R
506374908	202102	R
506374908	202101	R
506374908	202012	R
506374908	202011	R
506374908	202010	R
506374908	202009	W
 */

select client_nbr, ELIG_DATE, ME_CODE from medicaid.dbo.enrl_2021 where client_nbr = '604627839'


select * from medicaid.dbo.enrl_2021 where client_nbr = '506374908'

select * from medicaid.dbo.enrl_2021 where client_nbr = '612385989';

select * from medicaid.dbo.enrl_2021 where client_nbr = '525196823'; --exists

select * from stage.dbo.AGG_ENRL_MCD_FY where client_nbr = '525196823'; --does not exist for FY2021

select * from stage.dbo.AGG_ENRL_MCD_FY where client_nbr = '612385989';



525196823
625625499
523322116
605424728
611894032
524454138
529027645

--DX
drop table if exists work.dbo.xz_temp1;
drop table if exists work.dbo.xz_temp2;

select '2021' as fy, icn, 'clm' as src_table
into work.dbo.xz_temp1
from medicaid.dbo.clm_dx_21 where
PRIM_DX_CD like 'F32%' or
DX_CD_1 like 'F32%' or
DX_CD_2 like 'F32%' or
DX_CD_3 like 'F32%' or
DX_CD_4 like 'F32%' or
DX_CD_5 like 'F32%' or
DX_CD_6 like 'F32%' or
DX_CD_7 like 'F32%' or
DX_CD_8 like 'F32%' or
DX_CD_9 like 'F32%' or
DX_CD_10 like 'F32%' or
DX_CD_11 like 'F32%' or
DX_CD_12 like 'F32%' or
DX_CD_13 like 'F32%' or
DX_CD_14 like 'F32%' or
DX_CD_15 like 'F32%' or
DX_CD_16 like 'F32%' or
DX_CD_17 like 'F32%' or
DX_CD_18 like 'F32%' or
DX_CD_19 like 'F32%' or
DX_CD_20 like 'F32%' or
DX_CD_21 like 'F32%' or
DX_CD_22 like 'F32%' or
DX_CD_23 like 'F32%' or
DX_CD_24 like 'F32%' or
DX_CD_25 like 'F32%';
--315240

--enc

select '2021' as fy, derv_enc, 'enc' as src_table
into work.dbo.xz_temp2
from medicaid.dbo.ENC_DX_21 where
PRIM_DX_CD like 'F32%' or
DX_CD_1 like 'F32%' or
DX_CD_2 like 'F32%' or
DX_CD_3 like 'F32%' or
DX_CD_4 like 'F32%' or
DX_CD_5 like 'F32%' or
DX_CD_6 like 'F32%' or
DX_CD_7 like 'F32%' or
DX_CD_8 like 'F32%' or
DX_CD_9 like 'F32%' or
DX_CD_10 like 'F32%' or
DX_CD_11 like 'F32%' or
DX_CD_12 like 'F32%' or
DX_CD_13 like 'F32%' or
DX_CD_14 like 'F32%' or
DX_CD_15 like 'F32%' or
DX_CD_16 like 'F32%' or
DX_CD_17 like 'F32%' or
DX_CD_18 like 'F32%' or
DX_CD_19 like 'F32%' or
DX_CD_20 like 'F32%' or
DX_CD_21 like 'F32%' or
DX_CD_22 like 'F32%' or
DX_CD_23 like 'F32%' or
DX_CD_24 like 'F32%';
--1030078


--my tables
drop table if exists work.dbo.xz_temp3;
drop table if exists work.dbo.xz_temp4;

select fy, icn, pcn
into work.dbo.xz_temp3
from work.dbo.xz_mcd_clm_dx_21
where dx like 'F32%';
--393243

select fy, icn, pcn
into work.dbo.xz_temp4
from work.dbo.xz_mcd_enc_dx_21
where dx like 'F32%';
--3053913

select a.icn as raw_icn, b.icn as my_icn
from work.dbo.xz_temp1 a left join work.dbo.xz_temp3 b
on a.icn = b.icn
where b.icn is null;

--no matches when a.icn is null

/*these icns missing from my list
200050030202119430176707
100050031202113409521512
200050030202034354349715
200050030202035459624104
200050030202113409521888
100050030202109093074343
100040030202113810262779
100031030202117423665493
100031030202118226570614
200050030202204108651432
 */

select * from medicaid.dbo.clm_dx_21 where icn = '200050030202119430176707'
select * from work.dbo.xz_mcd_clm_dx_21 where icn = '200050030202119430176707' order by dx_pos;

select a.derv_enc as raw_icn, b.icn as my_icn
from work.dbo.xz_temp2 a left join work.dbo.xz_temp4 b
on a.derv_enc = b.icn
where b.icn is null;

select count(*) from work.dbo.xz_temp2;
select count(*) from work.dbo.xz_temp4;

select top 10 derv_enc from work.dbo.xz_temp2
order by derv_enc;

select top 10 icn from work.dbo.xz_temp4
order by icn;

--no matches when b.icn is null

select * from medicaid.dbo.clm_dx_21 where icn = '200050030202119430176707'
select * from work.dbo.xz_mcd_clm_dx_21 where icn = '200050030202119430176707' order by dx_pos;

select * from medicaid.dbo.enc_dx_21 where derv_enc = '0000000450067800I63'
select * from work.dbo.xz_mcd_enc_dx_21 where icn = '0000000450067800I63' order by dx_pos;
0000000450067800I63



--DX
drop table if exists work.dbo.xz_temp1;
drop table if exists work.dbo.xz_temp2;

select '2021' as fy, icn, 'clm' as src_table
into work.dbo.xz_temp1
from medicaid.dbo.clm_dx_21 where
PRIM_DX_CD like 'F32%' or
DX_CD_1 like 'F32%' or
DX_CD_2 like 'F32%' or
DX_CD_3 like 'F32%' or
DX_CD_4 like 'F32%' or
DX_CD_5 like 'F32%' or
DX_CD_6 like 'F32%' or
DX_CD_7 like 'F32%' or
DX_CD_8 like 'F32%' or
DX_CD_9 like 'F32%' or
DX_CD_10 like 'F32%' or
DX_CD_11 like 'F32%' or
DX_CD_12 like 'F32%' or
DX_CD_13 like 'F32%' or
DX_CD_14 like 'F32%' or
DX_CD_15 like 'F32%' or
DX_CD_16 like 'F32%' or
DX_CD_17 like 'F32%' or
DX_CD_18 like 'F32%' or
DX_CD_19 like 'F32%' or
DX_CD_20 like 'F32%' or
DX_CD_21 like 'F32%' or
DX_CD_22 like 'F32%' or
DX_CD_23 like 'F32%' or
DX_CD_24 like 'F32%' or
DX_CD_25 like 'F32%';
--315240

--enc
select '2021' as fy, derv_enc as icn, 'enc' as src_table
into work.dbo.xz_temp2
from medicaid.dbo.ENC_DX_21 where
PRIM_DX_CD like 'F32%' or
DX_CD_1 like 'F32%' or
DX_CD_2 like 'F32%' or
DX_CD_3 like 'F32%' or
DX_CD_4 like 'F32%' or
DX_CD_5 like 'F32%' or
DX_CD_6 like 'F32%' or
DX_CD_7 like 'F32%' or
DX_CD_8 like 'F32%' or
DX_CD_9 like 'F32%' or
DX_CD_10 like 'F32%' or
DX_CD_11 like 'F32%' or
DX_CD_12 like 'F32%' or
DX_CD_13 like 'F32%' or
DX_CD_14 like 'F32%' or
DX_CD_15 like 'F32%' or
DX_CD_16 like 'F32%' or
DX_CD_17 like 'F32%' or
DX_CD_18 like 'F32%' or
DX_CD_19 like 'F32%' or
DX_CD_20 like 'F32%' or
DX_CD_21 like 'F32%' or
DX_CD_22 like 'F32%' or
DX_CD_23 like 'F32%' or
DX_CD_24 like 'F32%';
--1030078

drop table if exists work.dbo.xz_temp3;

select a.fy, a.icn, b.pcn, a.src_table
into work.dbo.xz_temp3
from work.dbo.xz_temp1 a inner join medicaid.dbo.clm_proc_21 b
on a.icn = b.icn
where b.pcn is not null and trim(b.pcn) != '';

insert into work.dbo.xz_temp3
select a.fy, a.icn, b.MEM_ID as pcn, a.src_table
from work.dbo.xz_temp2 a inner join medicaid.dbo.enc_proc_21 b
on a.icn = b.DERV_ENC
where b.MEM_ID is not null and trim(b.MEM_ID) != '';

select count(*) as rows, count(distinct icn) as distinct_icn,
  count(distinct pcn) as distinct_pcn
  from work.dbo.xz_temp3;
-- rows     icn     pcn
-- 1345318	1345318	263930
 
 
select count(*) as rows, count(distinct icn) as distinct_icn,
  count(distinct pcn) as distinct_pcn
  from work.dbo.xz_temp3
  group by src_table;
 
-- rows     icn     pcn
-- 315240	315240	78106
 
 select count(*) as rows, count(distinct icn) as distinct_icn,
  count(distinct pcn) as distinct_pcn
  from work.dbo.xz_temp4
  group by src_table;
 
 -- rows     icn     pcn
 -- 1030078	 1030078 197492
 
 select * from work.dbo.xz_temp3;

select count(*) from work.dbo.xz_5a_tdcj_enrl;
--1247568
--region, sid_no, sex, fy, enrlmnth, agegrp, age

xz_mcd_enrl_cy_reconciled

select * from medicaid.dbo.CLM_DX_1819_HTW
select * from medicaid.dbo.CLM_PROC_1819_HTW

drop table if exists work.dbo.xz_temp1

select '2012' as fy, icn, pcn
into work.dbo.xz_temp1
from medicaid.dbo.CLM_PROC_21
where proc_icd_cd_1 = 'F07Z5ZZ';
--F07Z5ZZ 43 rows


select distinct proc_icd_cd_1, count(*) as count from medicaid.dbo.clm_proc_21
group by proc_icd_cd_1
order by count(*) desc;

select distinct proc_icd_cd_1, count(*) as count from medicaid.dbo.clm_proc_12
group by proc_icd_cd_1
order by count(*) desc;
/*
code	count
03995  	19474
07569  	18217
09904  	17328
06400  	12405
03893  	10494
09671  	7896
09390  	7215
04516  	6452
09672  	6093
03897  	5163 */

select distinct substring(proc_icd_cd_1, 1, 1), count(*) as count from medicaid.dbo.clm_proc_12
group by substring(proc_icd_cd_1, 1, 1)
order by count(*) desc;

select proc_icd_cd_1 from medicaid.dbo.clm_proc_12
where substring(proc_icd_cd_1, 1, 1) != '0' 
	and proc_icd_cd_1 is not null
	and trim(proc_icd_cd_1) != '';

select len(proc_icd_cd_1) as length, count(*) as count from medicaid.dbo.clm_proc_12
group by len(proc_icd_cd_1)
order by count(*) desc;


select distinct proc_icd_cd_1, count(*) as count from medicaid.dbo.clm_proc_16
group by proc_icd_cd_1
order by count(*) desc;


select min(cast(from_dos as date)) as min_date, max(cast(from_dos as date)) as max_date from medicaid.dbo.CLM_DETAIL_1819_HTW

select cast(from_dos as date) as fdos from medicaid.dbo.CLM_DETAIL_1819_HTW
order by cast(from_dos as date);
--starting 2017-09-01

2001-01-01
2001-01-01
2001-01-01
2001-01-01
2001-01-01
2016-02-12
2017-09-01
2017-09-01
2017-09-01
2017-09-01
2017-09-01
2017-09-01
2017-09-01
2017-09-01
2017-09-01

select cast(from_dos as date) as fdos from medicaid.dbo.CLM_DETAIL_1819_HTW
order by cast(from_dos as date) desc;
-- 75 claims in 2019
2068-10-18
2020-05-09
2019-08-23
2019-08-15
2019-08-15
2019-08-15
2019-08-15
2019-07-31
2019-07-30
2019-07-30
2019-07-30
2019-07-23
2019-07-23
2019-07-23
2019-07-23

drop table if exists work.dbo.xz_temp1;

select '2012' as fy, icn, pcn 
into work.dbo.xz_temp1
from medicaid.dbo.clm_proc_12 where
PROC_ICD_CD_1 = '03995' or
PROC_ICD_CD_2 = '03995' or
PROC_ICD_CD_3 = '03995' or
PROC_ICD_CD_4 = '03995' or
PROC_ICD_CD_5 = '03995' or
PROC_ICD_CD_6 = '03995' or
PROC_ICD_CD_7 = '03995' or
PROC_ICD_CD_8 = '03995' or
PROC_ICD_CD_9 = '03995' or
PROC_ICD_CD_10 = '03995' or
PROC_ICD_CD_11 = '03995' or
PROC_ICD_CD_12 = '03995' or
PROC_ICD_CD_13 = '03995' or
PROC_ICD_CD_14 = '03995' or
PROC_ICD_CD_15 = '03995' or
PROC_ICD_CD_16 = '03995' or
PROC_ICD_CD_17 = '03995' or
PROC_ICD_CD_18 = '03995' or
PROC_ICD_CD_19 = '03995' or
PROC_ICD_CD_20 = '03995' or
PROC_ICD_CD_21 = '03995' or
PROC_ICD_CD_22 = '03995' or
PROC_ICD_CD_23 = '03995' or
PROC_ICD_CD_24 = '03995' or
PROC_ICD_CD_25 = '03995';

insert into work.dbo.xz_temp1
select '2013' as fy, derv_enc, mem_id
from medicaid.dbo.enc_proc_13 where
PRIM_PROC_CD = '03995' or
PROC_ICD_CD_1 = '03995' or
PROC_ICD_CD_2 = '03995' or
PROC_ICD_CD_3 = '03995' or
PROC_ICD_CD_4 = '03995' or
PROC_ICD_CD_5 = '03995' or
PROC_ICD_CD_6 = '03995' or
PROC_ICD_CD_7 = '03995' or
PROC_ICD_CD_8 = '03995' or
PROC_ICD_CD_9 = '03995' or
PROC_ICD_CD_10 = '03995' or
PROC_ICD_CD_11 = '03995' or
PROC_ICD_CD_12 = '03995' or
PROC_ICD_CD_13 = '03995' or
PROC_ICD_CD_14 = '03995' or
PROC_ICD_CD_15 = '03995' or
PROC_ICD_CD_16 = '03995' or
PROC_ICD_CD_17 = '03995' or
PROC_ICD_CD_18 = '03995' or
PROC_ICD_CD_19 = '03995' or
PROC_ICD_CD_20 = '03995' or
PROC_ICD_CD_21 = '03995' or
PROC_ICD_CD_22 = '03995' or
PROC_ICD_CD_23 = '03995' or
PROC_ICD_CD_24 = '03995';


select * from medicaid.dbo.CLM_PROC_1819_HTW




alter table work.dbo.xz_temp1
alter column

select max(len(icn)) as icn, max(len(pcn)) as pcn from medicaid.dbo.CLM_PROC_1819_HTW;
24, 9

select max(len(icn)) as icn, max(len(pcn)) as pcn from medicaid.dbo.CLM_PROC_12;
24, 9

--10E0XZZ
drop table if exists work.dbo.xz_temp4;

select '2019' as fy, icn, pcn 
into work.dbo.xz_temp4
from medicaid.dbo.clm_proc_19 where
PROC_ICD_CD_1 = '10E0XZZ' or
PROC_ICD_CD_2 = '10E0XZZ' or
PROC_ICD_CD_3 = '10E0XZZ' or
PROC_ICD_CD_4 = '10E0XZZ' or
PROC_ICD_CD_5 = '10E0XZZ' or
PROC_ICD_CD_6 = '10E0XZZ' or
PROC_ICD_CD_7 = '10E0XZZ' or
PROC_ICD_CD_8 = '10E0XZZ' or
PROC_ICD_CD_9 = '10E0XZZ' or
PROC_ICD_CD_10 = '10E0XZZ' or
PROC_ICD_CD_11 = '10E0XZZ' or
PROC_ICD_CD_12 = '10E0XZZ' or
PROC_ICD_CD_13 = '10E0XZZ' or
PROC_ICD_CD_14 = '10E0XZZ' or
PROC_ICD_CD_15 = '10E0XZZ' or
PROC_ICD_CD_16 = '10E0XZZ' or
PROC_ICD_CD_17 = '10E0XZZ' or
PROC_ICD_CD_18 = '10E0XZZ' or
PROC_ICD_CD_19 = '10E0XZZ' or
PROC_ICD_CD_20 = '10E0XZZ' or
PROC_ICD_CD_21 = '10E0XZZ' or
PROC_ICD_CD_22 = '10E0XZZ' or
PROC_ICD_CD_23 = '10E0XZZ' or
PROC_ICD_CD_24 = '10E0XZZ' or
PROC_ICD_CD_25 = '10E0XZZ';

drop table if exists work.dbo.xz_temp5;

select '2018' as fy, icn, pcn 
into work.dbo.xz_temp5
from medicaid.dbo.clm_proc_18 where
PROC_ICD_CD_1 = '10E0XZZ' or
PROC_ICD_CD_2 = '10E0XZZ' or
PROC_ICD_CD_3 = '10E0XZZ' or
PROC_ICD_CD_4 = '10E0XZZ' or
PROC_ICD_CD_5 = '10E0XZZ' or
PROC_ICD_CD_6 = '10E0XZZ' or
PROC_ICD_CD_7 = '10E0XZZ' or
PROC_ICD_CD_8 = '10E0XZZ' or
PROC_ICD_CD_9 = '10E0XZZ' or
PROC_ICD_CD_10 = '10E0XZZ' or
PROC_ICD_CD_11 = '10E0XZZ' or
PROC_ICD_CD_12 = '10E0XZZ' or
PROC_ICD_CD_13 = '10E0XZZ' or
PROC_ICD_CD_14 = '10E0XZZ' or
PROC_ICD_CD_15 = '10E0XZZ' or
PROC_ICD_CD_16 = '10E0XZZ' or
PROC_ICD_CD_17 = '10E0XZZ' or
PROC_ICD_CD_18 = '10E0XZZ' or
PROC_ICD_CD_19 = '10E0XZZ' or
PROC_ICD_CD_20 = '10E0XZZ' or
PROC_ICD_CD_21 = '10E0XZZ' or
PROC_ICD_CD_22 = '10E0XZZ' or
PROC_ICD_CD_23 = '10E0XZZ' or
PROC_ICD_CD_24 = '10E0XZZ' or
PROC_ICD_CD_25 = '10E0XZZ';

insert into work.dbo.xz_temp5
select '2018' as fy, icn, pcn 
from medicaid.dbo.CLM_PROC_1819_HTW where
PROC_ICD_CD_1 = '10E0XZZ' or
PROC_ICD_CD_2 = '10E0XZZ' or
PROC_ICD_CD_3 = '10E0XZZ' or
PROC_ICD_CD_4 = '10E0XZZ' or
PROC_ICD_CD_5 = '10E0XZZ' or
PROC_ICD_CD_6 = '10E0XZZ' or
PROC_ICD_CD_7 = '10E0XZZ' or
PROC_ICD_CD_8 = '10E0XZZ' or
PROC_ICD_CD_9 = '10E0XZZ' or
PROC_ICD_CD_10 = '10E0XZZ' or
PROC_ICD_CD_11 = '10E0XZZ' or
PROC_ICD_CD_12 = '10E0XZZ' or
PROC_ICD_CD_13 = '10E0XZZ' or
PROC_ICD_CD_14 = '10E0XZZ' or
PROC_ICD_CD_15 = '10E0XZZ' or
PROC_ICD_CD_16 = '10E0XZZ' or
PROC_ICD_CD_17 = '10E0XZZ' or
PROC_ICD_CD_18 = '10E0XZZ' or
PROC_ICD_CD_19 = '10E0XZZ' or
PROC_ICD_CD_20 = '10E0XZZ' or
PROC_ICD_CD_21 = '10E0XZZ' or
PROC_ICD_CD_22 = '10E0XZZ' or
PROC_ICD_CD_23 = '10E0XZZ' or
PROC_ICD_CD_24 = '10E0XZZ' or
PROC_ICD_CD_25 = '10E0XZZ';

--0

select a.icn from work.dbo.xz_temp4 a inner join work.dbo.xz_temp5 b on a.icn = b.icn;
--no matches

select * from work.dbo.xz_temp4;

select '2021' as fy, concat(pcn, ndc, replace(rx_fill_dt, '-', '')) as rx_id, pcn
from medicaid.dbo.chip_rx_fy21 where ndc = '00065853302';

--lots

select '2012' as fy, concat(pcn, ndc, replace(rx_fill_dt, '-', '')) as rx_id, pcn
from medicaid.dbo.chip_rx_fy12 where ndc = '00065853302';

--none

select ndc, count(*) as count from medicaid.dbo.chip_rx_fy21
group by ndc
order by count(*) desc;

ndc			count
00642009330	23459	multivitamin
00054327099	17843	flonase
59267100001	16740	covid vaccine
51672407008	15019	zyrtec
00093317431	12089	albuterol
59267100002	11745
00143988701	8682


select ndc, count(*) as count from medicaid.dbo.chip_rx_fy12
group by ndc
order by count(*) desc;

ndc			count
00085128801	51966	nasonex
00006027531	42758	singulair
60432083716	42103	bromfed dm
63402051001	38812	levalbuterol (asthma)
00006011731	15385	singulair
00006071131	15050	singulair chewable
60432083704	14856	bromfed
00065853302	14457	cipro <--this is present in 2012 and 2021, let's use it
00143988701	14266


select * from medicaid.dbo.FFS_RX_FY18_19_HTW




select a.*
into work.dbo.xz_temp2
from work.dbo.xz_temp1 a inner join
	(select client_nbr as pcn from medicaid.dbo.enrl_2021
	union all
	select client_nbr as pcn from medicaid.dbo.chip_uth_sfy2021_final
	) b on a.pcn = b.pcn;
	





select a.*
from work.dbo.xz_temp1 a left join work.dbo.xz_temp2 b
on a.rx_id = b.rx_id
where b.rx_id is null;

fy		rx_id							pcn
2021	7406633640006585330220201110	740663364
2021	5210694320006585330220201208	521069432
2021	7406633640006585330220201229	740663364
2021	6093047660006585330220201028	609304766
2021	5278630870006585330220200928	527863087
2021	6252815450006585330220201113	625281545
2021	7406633640006585330220201216	740663364
2021	7406633640006585330220200915	740663364
2021	7406633640006585330220201207	740663364
2021	7406633640006585330220201201	740663364


select * from (select client_nbr as pcn from medicaid.dbo.enrl_2021
	union all
	select client_nbr as pcn from medicaid.dbo.chip_uth_sfy2021_final
	) b
where pcn = '740663364'; --not enrolled for FY21



select PROC_CD, COUNT(*) as count from medicaid.dbo.CLM_DETAIL_12
group by PROC_CD
order by count(*) desc;

proc_cd	count
7049X  	3472186		the most common proc code is not found?
99213  	3428859		patient office visit <-- use this one
T1019  	2452572 	personal care attendant
D1351  	2294375
92507  	1958952
T2003  	1898339
36415  	1880628
85025  	1871694
250    	1794730
99214  	1678019

select PROC_CD, COUNT(*) as count from medicaid.dbo.CLM_DETAIL_21
group by PROC_CD
order by count(*) desc;

proc_cd	count
7049X  	4882345		same
T2003  	1760220		non-emergency transportation
7051X  	1374304
250    	1215548
7025X  	1014830
99232  	801280		lvl2 hospital f/u care
99214  	769142		patient office visit
97110  	762012
99213  	738506
85025  	712659

select '2012' as fy, a.icn, b.pcn
--into work.dbo.temp1
from medicaid.dbo.CLM_DETAIL_12 a inner join medicaid.dbo.CLM_PROC_12 b
on a.ICN = b.ICN
where PROC_CD = '99213' or SUB_PROC_CD = ''

select top 10 '2013' as fy, a.derv_enc, b.MEM_ID
from medicaid.dbo.ENC_DET_13 a inner join medicaid.dbo.ENC_PROC_13 b
on a.derv_enc = b.derv_enc
where PROC_CD = '99213';



select '2012' as fy, a.icn, b.pcn
--into work.dbo.temp1
from medicaid.dbo.CLM_DETAIL_1819_HTW a inner join medicaid.dbo.CLM_PROC_12 b
on a.ICN = b.ICN
where PROC_CD = '99213' or SUB_PROC_CD = ''

drop table if exists work.dbo.xz_temp1;

select '2018' as fy, a.icn, b.pcn
into work.dbo.xz_temp1
from medicaid.dbo.CLM_DETAIL_18 a inner join medicaid.dbo.CLM_PROC_18 b
on a.ICN = b.ICN
where PROC_CD = '99213' or SUB_PROC_CD = '99213';

insert into work.dbo.xz_temp1
select '2018' as fy, a.derv_enc, b.MEM_ID
from medicaid.dbo.ENC_DET_18 a inner join medicaid.dbo.ENC_PROC_18 b
on a.derv_enc = b.derv_enc
where PROC_CD = '99213';

insert into work.dbo.xz_temp1
select '2018' as fy, a.icn, b.pcn
from medicaid.dbo.CLM_DETAIL_1819_HTW a inner join medicaid.dbo.CLM_PROC_1819_HTW b
on a.ICN = b.ICN
where PROC_CD = '99213' or SUB_PROC_CD = '99213';

drop table if exists work.dbo.xz_temp2;

select distinct pcn 
into work.dbo.xz_temp2
from work.dbo.xz_temp1;

--count distinct PCNs in 2018 (clm, enc, htw)
select count(*) from work.dbo.xz_temp2;
--2973065

select * from work.dbo.xz_temp1 where icn = '100020030201817948309523';

select * from medicaid.dbo.clm_detail_18 where icn = '100020030201817948309523'
select * from medicaid.dbo.clm_proc_18 where icn = '100020030201817948309523';


select * from work.dbo.xz_temp1 where pcn = '617028738'; -- in here

select * from work.dbo.xz_temp2 where pcn = '617028738'; -- in here


select * from work.dbo.xz_temp1 where icn = '100020030201802601181231'; --not in here

select * from medicaid.dbo.clm_detail_18 where icn = '100020030201802601181231' --not in here
select * from medicaid.dbo.clm_detail_19 where icn = '100020030201802601181231' --not in here

select * from medicaid.dbo.CLM_DETAIL_1819_HTW where icn = '100020030201802601181231';




insert into work.dbo.temp1
  select '2018' as fy, a.icn, b.pcn
  from medicaid.dbo.CLM_DETAIL_1819_HTW a inner join medicaid.dbo.CLM_PROC_1819_HTW b
  on a.ICN = b.ICN
  where PROC_CD = '99213' or SUB_PROC_CD = '99213';


select count(distinct pcn) from work.dbo.xz_temp1;
--2,943,734
--383413 from clm
--2943734 after enc
