/****** Script for SelectTopNRows command from SSMS  ******/

drop table stage.dbo.wc_mdcd_flu_2016;

select client_nbr, min(zip3) as zip3, max(sex) as sex, min(age) as age , min(age_Group) as age_group
into stage.dbo.wc_mdcd_flu_2016
from 
(
SELECT [CLIENT_NBR]
      ,substring([ZIP],1,3) as zip3
      ,[SEX]
	  ,age 
      ,	   case  when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	   end as age_group
	   ,elig_date 
  FROM [MEDICAID].[dbo].[ENRL_2016]
  where age < '65'
  and elig_date >= 201601
  and substring(zip,1,3) between '750' and '799'
 union 
  SELECT[CLIENT_NBR]
      ,substring([ZIP],1,3) as zip3
      ,[SEX]
	  ,age
      ,	   case  when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	   end as age_group
	   ,elig_date 
  FROM [MEDICAID].[dbo].[ENRL_2017]
  where cast(age as float) < 65
    and elig_date <= 201612
	and substring(zip,1,3) between '750' and '799'
  ) inr 
  group by client_nbr;



delete from stage.dbo.wc_mdcd_flu_2016 where age_group is null;


drop table stage.dbo.wc_mdcd_vacc_derv_enc_2016 


select distinct derv_enc 
into stage.dbo.wc_mdcd_vacc_derv_enc_2016
from ( 
  select derv_enc 
  from medicaid.dbo.ENC_det_16 d
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039')
  and d.FDOS_DT between '2016-01-01' and '2016-12-31'
union 
  select derv_enc 
  from medicaid.dbo.ENC_det_17 d
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039')
  and d.FDOS_DT between '2016-01-01' and '2016-12-31'
) inr ;


drop table stage.dbo.wc_mdcd_vacc_mem_2016

select distinct mem_id 
into stage.dbo.wc_mdcd_vacc_mem_2016
from 
(
select distinct a.mem_id 
from medicaid.dbo.enc_proc_16 a 
where a.DERV_ENC in ( select derv_enc from stage.dbo.wc_mdcd_vacc_derv_enc_2016) 
union 
select distinct a.mem_id 
from medicaid.dbo.enc_proc_17 a 
where a.DERV_ENC in ( select derv_enc from stage.dbo.wc_mdcd_vacc_derv_enc_2016) 
) inr ; 



alter table stage.dbo.wc_mdcd_flu_2016 add vacc_flag int default 0;


update stage.dbo.wc_mdcd_flu_2016 set vacc_flag = 1
  from stage.dbo.wc_mdcd_vacc_mem_2016 b 
    where client_nbr = b.mem_id
 ;

select count(*), count(distinct client_nbr), sum(vacc_flag) from stage.dbo.wc_mdcd_flu_2016;


----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2016;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2016
where sex = 'F';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2016
where sex = 'M';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag), age_group
from stage.dbo.wc_mdcd_flu_2016
group by age_group
order by age_group;


--- prevalance by zip 

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2016
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2016
where sex = 'F';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2016
where sex = 'M';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) ) as prev, count(client_nbr), sum(vacc_flag), age_group
from stage.dbo.wc_mdcd_flu_2016
group by age_group
order by age_group;
