drop table stage.dbo.wc_mdcd_tobacco_2018;


select client_nbr, min(elig_month) as fst_elig, max(elig_month) as last_elig, 
       min(zip3) as zip3, max(sex) as sex, min(age) as age , min(src) as src 
into stage.dbo.wc_mdcd_tobacco_2018_temp 
from 
(
select client_nbr
       ,substring(mailing_zip,1,3) as zip3 
       ,gender_cd as sex  
       ,age 
	   ,elig_month 
	   ,'chip' as src 
FROM MEDICAID.dbo.CHIP_UTH_SFY2018_Final
  where elig_month between 201801 and 201812
  and substring(mailing_zip,1,3) between '750' and '799'
  --and cast(age as float) >= 15.00
 union 
 select client_nbr
      ,substring(mailing_zip,1,3) as zip3
       ,gender_cd as sex 
      ,age
	  ,elig_month 
	  ,'chip'
FROM MEDICAID.dbo.CHIP_UTH_SFY2019_Final
  where elig_month between 201801 and 201812
  and substring(mailing_zip,1,3) between '750' and '799'
 -- and cast(age as float) >= 15.00
 union 
SELECT [CLIENT_NBR]
      ,substring([ZIP],1,3) as zip3
      ,[SEX]
	  ,age 
	   ,elig_date 
	   ,'enrl' as src 
  FROM [MEDICAID].[dbo].[ENRL_2018]
  where elig_date between 201801 and 201812
  and substring(zip,1,3) between '750' and '799'
   -- and cast(age as float) >= 15.00
 union 
  SELECT[CLIENT_NBR]
      ,substring([ZIP],1,3) as zip3
      ,[SEX]
	  ,age
	   ,elig_date
	   ,'enrl' as src
  FROM [MEDICAID].[dbo].[ENRL_2019]
  where elig_date between 201801 and 201812
	and substring(zip,1,3) between '750' and '799'
	--  and cast(age as float) >= 15.00
  ) inr 
  group by client_nbr;

  
delete from stage.dbo.wc_mdcd_tobacco_2018_temp where last_elig <> 201812;

select count(*) from stage.dbo.wc_mdcd_tobacco_2018_temp
--where cast(age as float) >= 15.00
--and last_elig = 201812;


---------------------------------------------------------------------------------------------------------
----proc and hcpc
---------------------------------------------------------------------------------------------------------
drop table stage.dbo.wc_mdcd_tobacco_clm_2018 ;

select distinct derv_enc 
into stage.dbo.wc_mdcd_tobacco_clm_2018
from ( 
  select derv_enc
  from medicaid.dbo.ENC_DET_18
  where proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  '1034F','4004F','4001F','G9906','G9907','G9908','G9909')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select derv_enc
  from medicaid.dbo.ENC_DET_19 d
  where proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  '1034F','4004F','4001F','G9906','G9907','G9908','G9909')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select ICN 
  from MEDICAID.dbo.CLM_DETAIL_18
  where proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  '1034F','4004F','4001F','G9906','G9907','G9908','G9909')
  and  FROM_DOS between '2018-01-01' and '2018-12-31'
union 
  select ICN
  from medicaid.dbo.CLM_DETAIL_19
  where proc_cd in( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  '1034F','4004F','4001F','G9906','G9907','G9908','G9909')
  and FROM_DOS between '2018-01-01' and '2018-12-31'  
) inr ;


---------------------------------------------------------------------------------------------------------
----Diagnosis Codes
---------------------------------------------------------------------------------------------------------
create table stage.dbo.wc_mdcd_tobacco_diag (dx_cd varchar(50));

insert into stage.dbo.wc_mdcd_tobacco_diag values
('T65222A'),('T65213D'),('T65214A'),('O99330'),('F172107'),('F172007'),('T65291D'),('T65221S'),('T65213A'),('T65221A'),('T65224D'),('O99335'),
('Z716'),('T65211A'),('T65214'),('O9933'),('O99332'),('F1721'),('O99331'),('T65294D'),('T65214S'),('T65212D'),('T65292S'),('F172010'),('F17211'),
('Z87891'),('T65291A'),('Z7169'),('T65291S'),('T6522'),('T65212A'),('F17298'),('F17228'),('F17221'),('F17293'),('F17218'),('F17290'),('T65221'),
('T65211S'),('T65292D'),('F17299'),('T65224A'),('F17201'),('T6529'),('F1722'),('T65224S'),('F17291'),('F17200'),('F17209'),('F17220'),('P042'),
('F17208'),('F17229'),('T6523'),('Z720'),('T6521'),('F17213'),('F1720'),('F1729'),('T65223S'),('O99333'),('T65211D'),('T65222S'),('T65292A'),
('T65294S'),('F17223'),('T65222D'),('F17203'),('F17219'),('T65221D'),('T65293D'),('O99334'),('T65293S'),('T65223D'),('T65214D'),('T65223A'),
('P9681'),('T65293A'),('F17210'),('T65212S'),('T65294A'),('T65213S');

insert into stage.dbo.wc_mdcd_tobacco_clm_2018
select distinct ICN
from ( 
  select d.ICN 
  from medicaid.dbo.CLM_DX_18 d 
    join MEDICAID.dbo.CLM_HEADER_18 h 
      on h.ICN = d.ICN 
     and h.HDR_FRM_DOS between '2018-01-01' and '2018-12-31'
  where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        )
union 
  select d.ICN 
  from medicaid.dbo.CLM_DX_19 d
    join MEDICAID.dbo.CLM_HEADER_19 h 
      on h.ICN = d.ICN 
     and h.HDR_FRM_DOS between '2018-01-01' and '2018-12-31'
  where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        )
union 
  select d.DERV_ENC 
  from MEDICAID.dbo.enc_dx_18 d 
    join MEDICAID.dbo.ENC_HEADER_18 h 
      on h.DERV_ENC = d.DERV_ENC 
     and h.FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        )
union 
  select d.DERV_ENC 
  from MEDICAID.dbo.enc_dx_19 d 
    join MEDICAID.dbo.ENC_HEADER_19 h 
      on h.DERV_ENC = d.DERV_ENC 
     and h.FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_tobacco_diag) 
        )
) inr ;



---------------------------------------------------------------------------------------------------------
----Members from claim ids
--------------------------------------------------------------------------------------------------------
drop table stage.dbo.wc_mdcd_tobacco_mem_2018;

select distinct mem_id 
into stage.dbo.wc_mdcd_tobacco_mem_2018
from (
		select distinct a.mem_id 
		from medicaid.dbo.enc_proc_18 a 
		where a.DERV_ENC in (select derv_enc from stage.dbo.wc_mdcd_tobacco_clm_2018) 
	union 
		select distinct a.mem_id 
		from medicaid.dbo.enc_proc_19 a 
		where a.DERV_ENC in (select derv_enc from stage.dbo.wc_mdcd_tobacco_clm_2018) 
	union 
		select a.PCN
		from MEDICAID.dbo.clm_proc_18 a 
		where a.ICN in (select derv_enc from stage.dbo.wc_mdcd_tobacco_clm_2018) 
    union 
		select a.PCN
		from MEDICAID.dbo.clm_proc_19 a 
		where a.ICN in (select derv_enc from stage.dbo.wc_mdcd_tobacco_clm_2018) 
	) inr ; 


---rx
insert into stage.dbo.wc_mdcd_tobacco_mem_2018
select distinct pcn from MEDICAID.dbo.mco_rx_fy18 where ndc in ( select ndc from stage.dbo.wc_tobacco_ndc)
;

insert into stage.dbo.wc_mdcd_tobacco_mem_2018
select distinct pcn from MEDICAID.dbo.CHIP_RX_FY18 where ndc in ( select ndc from stage.dbo.wc_tobacco_ndc)
;


-------------------------------------------------

alter table stage.dbo.wc_mdcd_tobacco_2018 add vacc_flag int default 0;


update stage.dbo.wc_mdcd_tobacco_2018 set vacc_flag = 1
  from stage.dbo.wc_mdcd_tobacco_mem_2018 b 
    where client_nbr = b.mem_id
 ;


select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_tobacco_2018;


----------------cessation rates


---------------------------------------------------------------------------------------------------------
----proc and hcpc cess
---------------------------------------------------------------------------------------------------------
drop table stage.dbo.wc_mdcd_tobacco_cess_clm_2018 ;

select distinct derv_enc 
into stage.dbo.wc_mdcd_tobacco_cess_clm_2018
from ( 
  select derv_enc
  from medicaid.dbo.ENC_DET_18
  where proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  		  '4004F','4001F')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select derv_enc
  from medicaid.dbo.ENC_DET_19 d
  where proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  		  '4004F','4001F')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select ICN 
  from MEDICAID.dbo.CLM_DETAIL_18
  where proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  		  '4004F','4001F')
  and  FROM_DOS between '2018-01-01' and '2018-12-31'
union 
  select ICN
  from medicaid.dbo.CLM_DETAIL_19
  where proc_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
				  		  '4004F','4001F')
  and FROM_DOS between '2018-01-01' and '2018-12-31'  
) inr ;


--diag cess
insert into stage.dbo.wc_mdcd_tobacco_cess_clm_2018
select distinct ICN
from ( 
  select d.ICN 
  from medicaid.dbo.CLM_DX_18 d 
    join MEDICAID.dbo.CLM_HEADER_18 h 
      on h.ICN = d.ICN 
     and h.HDR_FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 = 'Z716'
        or d.DX_CD_2 = 'Z716'
        or d.DX_CD_3 = 'Z716'
        or d.DX_CD_4 = 'Z716'
        or d.DX_CD_5 = 'Z716'
        or d.DX_CD_6 = 'Z716'
        or d.DX_CD_7 = 'Z716'
        or d.DX_CD_8 = 'Z716'
        or d.DX_CD_9 = 'Z716'
        )
union 
  select d.ICN 
  from medicaid.dbo.CLM_DX_19 d
    join MEDICAID.dbo.CLM_HEADER_19 h 
      on h.ICN = d.ICN 
     and h.HDR_FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 = 'Z716'
        or d.DX_CD_2 = 'Z716'
        or d.DX_CD_3 = 'Z716'
        or d.DX_CD_4 = 'Z716'
        or d.DX_CD_5 = 'Z716'
        or d.DX_CD_6 = 'Z716'
        or d.DX_CD_7 = 'Z716'
        or d.DX_CD_8 = 'Z716'
        or d.DX_CD_9 = 'Z716'
        )
union 
  select d.DERV_ENC 
  from MEDICAID.dbo.enc_dx_18 d 
    join MEDICAID.dbo.ENC_HEADER_18 h 
      on h.DERV_ENC = d.DERV_ENC 
     and h.FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 = 'Z716'
        or d.DX_CD_2 = 'Z716'
        or d.DX_CD_3 = 'Z716'
        or d.DX_CD_4 = 'Z716'
        or d.DX_CD_5 = 'Z716'
        or d.DX_CD_6 = 'Z716'
        or d.DX_CD_7 = 'Z716'
        or d.DX_CD_8 = 'Z716'
        or d.DX_CD_9 = 'Z716'
        )
union 
  select d.DERV_ENC 
  from MEDICAID.dbo.enc_dx_19 d 
    join MEDICAID.dbo.ENC_HEADER_19 h 
      on h.DERV_ENC = d.DERV_ENC 
     and h.FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 = 'Z716'
        or d.DX_CD_2 = 'Z716'
        or d.DX_CD_3 = 'Z716'
        or d.DX_CD_4 = 'Z716'
        or d.DX_CD_5 = 'Z716'
        or d.DX_CD_6 = 'Z716'
        or d.DX_CD_7 = 'Z716'
        or d.DX_CD_8 = 'Z716'
        or d.DX_CD_9 = 'Z716'
        )
) inr ;


select distinct mem_id 
into stage.dbo.wc_mdcd_tobacco_cess_mem_2018
from (
		select distinct a.mem_id 
		from medicaid.dbo.enc_proc_18 a 
		where a.DERV_ENC in (select derv_enc from stage.dbo.wc_mdcd_tobacco_cess_clm_2018) 
	union 
		select distinct a.mem_id 
		from medicaid.dbo.enc_proc_19 a 
		where a.DERV_ENC in (select derv_enc from stage.dbo.wc_mdcd_tobacco_cess_clm_2018) 
	union 
		select a.PCN
		from MEDICAID.dbo.clm_proc_18 a 
		where a.ICN in (select derv_enc from stage.dbo.wc_mdcd_tobacco_cess_clm_2018) 
    union 
		select a.PCN
		from MEDICAID.dbo.clm_proc_19 a 
		where a.ICN in (select derv_enc from stage.dbo.wc_mdcd_tobacco_cess_clm_2018) 
	) inr ; 


create table stage.dbo.wc_tobacco_cess_ndcs (ndc_cd varchar(50));

---rx cess
insert into stage.dbo.wc_mdcd_tobacco_cess_mem_2018
select distinct pcn from MEDICAID.dbo.mco_rx_fy18 where ndc in ( select ndc from stage.dbo.wc_tobacco_cess_ndcs)
;

insert into stage.dbo.wc_mdcd_tobacco_cess_mem_2018
select distinct pcn from MEDICAID.dbo.CHIP_RX_FY18 where ndc in ( select ndc from stage.dbo.wc_tobacco_cess_ndcs)
;



alter table stage.dbo.wc_mdcd_tobacco_2018 add vacc_cess_flag int default 0;


update stage.dbo.wc_mdcd_tobacco_2018 set vacc_cess_flag = 1
  from stage.dbo.wc_mdcd_tobacco_cess_mem_2018 b 
    where client_nbr = b.mem_id
 ;


select count(client_nbr), sum(vacc_flag) as smoker,
       sum(vacc_cess_flag ) as cess,sex
from stage.dbo.wc_mdcd_tobacco_2018
group by sex;