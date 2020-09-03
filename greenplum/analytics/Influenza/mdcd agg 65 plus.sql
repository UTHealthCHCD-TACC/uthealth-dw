/****** Script for SelectTopNRows command from SSMS  ******/

drop table stage.dbo.wc_mdcd_65plus_2017;

select client_nbr, max(sex) as sex, min(age) as age, min(zip3) as zip3 
into stage.dbo.wc_mdcd_65plus_2017
from 
(
SELECT [CLIENT_NBR]
      ,[SEX]
	  ,age 
	   ,substring(zip,1,3) as zip3
  FROM [MEDICAID].[dbo].[ENRL_2017]
  where cast(age as float) >= 65
  and elig_date >= 201701
  and substring(zip,1,3) between '750' and '799'
 union 
  SELECT[CLIENT_NBR]
      ,[SEX]
	  ,age
	   ,substring(zip,1,3) as zip3
  FROM [MEDICAID].[dbo].[ENRL_2018]
  where cast(age as float) >= 65
    and elig_date <= 201712
	and substring(zip,1,3) between '750' and '799'
  ) inr 
  group by client_nbr;


drop table stage.dbo.wc_mdcd_65plus_vacc_derv_enc_2017 ;


select distinct derv_enc 
into stage.dbo.wc_mdcd_65plus_vacc_derv_enc_2017
from ( 
  select derv_enc 
  from medicaid.dbo.ENC_det_17 d
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039')
  and d.FDOS_DT between '2017-01-01' and '2017-12-31'
union 
  select derv_enc 
  from medicaid.dbo.ENC_det_18 d
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039')
  and d.FDOS_DT between '2017-01-01' and '2017-12-31'
) inr ;


drop table stage.dbo.wc_mdcd_65plus_vacc_mem_2017;

select distinct mem_id 
into stage.dbo.wc_mdcd_65plus_vacc_mem_2017
from 
(
select distinct a.mem_id 
from medicaid.dbo.enc_proc_17 a 
where a.DERV_ENC in ( select derv_enc from stage.dbo.wc_mdcd_65plus_vacc_derv_enc_2017) 
union 
select distinct a.mem_id 
from medicaid.dbo.enc_proc_18 a 
where a.DERV_ENC in ( select derv_enc from stage.dbo.wc_mdcd_65plus_vacc_derv_enc_2017) 
) inr ; 




alter table stage.dbo.wc_mdcd_65plus_2017 add vacc_flag int default 0;


update stage.dbo.wc_mdcd_65plus_2017 set vacc_flag = 1
  from stage.dbo.wc_mdcd_65plus_vacc_mem_2017 b 
    where client_nbr = b.mem_id
 ;

select count(*), count(distinct client_nbr), sum(vacc_flag) from stage.dbo.wc_mdcd_65plus_2017;

select * from stage.dbo.wc_mdcd_65plus_2017

