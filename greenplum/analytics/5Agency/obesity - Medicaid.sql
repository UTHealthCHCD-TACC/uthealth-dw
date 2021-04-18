
----find denominator: obese population using dx codes

create table STAGE.dbo.wc_5a_obese_dx (diag_cd varchar(20));

insert into STAGE.dbo.wc_5a_obese_dx values 
('E660'),('E661'),('E662'),('E668'),('E669'),('27800'),('27801'),('V853'),
('V8530'),('V8531'),('V8532'),('V8533'),('V8534'),('V8535'),('V8536'),('V8537'),('V8538'),
('V8539'),('V854'),('Z6830'),
('Z6831'),('Z6832'),('Z6833'),('Z6834'),('Z6835'),('Z6836'),('Z6837'),
('Z6838'),('Z6839'),('Z6841'),('Z6842'),('Z6843'),('Z6844'),('Z6845');


drop table if exists stage.dbo.wc_5a_obese_cohort;

select distinct fscyr from stage.dbo.wc_5a_obese_cohort;

---diag from claims run once for each year 2016-2019
insert into stage.dbo.wc_5a_obese_cohort
select icn, fscyr 
--into stage.dbo.wc_5a_obese_cohort
from (
select d.icn, '2019' as fscyr
from [MEDICAID].[dbo].[CLM_DX_19] d
  --join [MEDICAID].[dbo].[CLM_PROC_16] p
 --    on d.ICN = p.ICN 
where  ( d.PRIM_DX_CD in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.ADM_DX_CD in (select diag_cd from STAGE.dbo.wc_5a_obese_dx)  
       or d.DX_CD_1 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_2 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_3 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_4 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_5 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_6 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_7 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_8 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_9 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_10 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_11 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_12 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_13 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_14 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_15 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_16 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_17 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_18 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_19 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_20 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_21 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_22 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_23 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_24 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_25 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       )     
) inr; 


---diag from encoutner run once for each year 2016-2019
insert into stage.dbo.wc_5a_obese_cohort
select d.DERV_ENC, '2016' as fscyr  --, 2017, 2016
from [MEDICAID].[dbo].[ENC_DX_16] d
where  ( d.PRIM_DX_CD in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.ADM_DX_CD in (select diag_cd from STAGE.dbo.wc_5a_obese_dx)  
       or d.DX_CD_1 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_2 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_3 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_4 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_5 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_6 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_7 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_8 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_9 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_10 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_11 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_12 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_13 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_14 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_15 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_16 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_17 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_18 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_19 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_20 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_21 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_22 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_23 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       or d.DX_CD_24 in (select diag_cd from STAGE.dbo.wc_5a_obese_dx) 
       )     
; 


select distinct MCO_PROGRAM_NM from MEDICAID.dbo.LU_Contract


select distinct MCO_PROGRAM_NM from [stage].[dbo].[AGG_ENRL_MCD_YR] 



