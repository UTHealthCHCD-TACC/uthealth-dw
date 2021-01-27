/****** Medicaid Influenza 2018  ******/

drop table stage.dbo.wc_mdcd_flu_2018;


select client_nbr, min(elig_month) as fst_elig, max(elig_month) as lst_elig, 
       min(zip3) as zip3, max(sex) as sex, min(age) as age , min(age_Group) as age_group
into stage.dbo.wc_mdcd_flu_2018_temp
from 
(
select client_nbr
       ,substring(mailing_zip,1,3) as zip3 
       ,gender_cd as sex  
       ,age 
       ,case  when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	         else 7
	   end as age_group
	   ,elig_month 
from MEDICAID.dbo.CHIP_UTH_SFY2018_Final
  where elig_month between 201801 and 201812
  and substring(mailing_zip,1,3) between '750' and '799'
 union 
select client_nbr
      ,substring(mailing_zip,1,3) as zip3
      ,gender_cd as sex 
      ,age
      ,case  when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	         else 7
	   end as age_group
	  ,elig_month 
from MEDICAID.dbo.CHIP_UTH_SFY2019_Final
where elig_month between 201801 and 201812
  and substring(mailing_zip,1,3) between '750' and '799'
union 
select [CLIENT_NBR]
       ,substring([ZIP],1,3) as zip3
       ,[SEX]
	   ,age
       ,case when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	         else 7 
	   end as age_group
	  ,elig_date 
  from [MEDICAID].[dbo].[ENRL_2018]
  where elig_date between 201801 and 201812
    and substring(zip,1,3) between '750' and '799'
union 
select [CLIENT_NBR]
       ,substring([ZIP],1,3) as zip3
       ,[SEX]
	   ,age
       ,case when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	         else 7 
	   end as age_group
	  ,elig_date 
from [MEDICAID].[dbo].[ENRL_2019]
where elig_date between 201801 and 201812
    and substring(zip,1,3) between '750' and '799'
) inr 
  group by client_nbr;

--exclude anyone in texas women or duel eligible
drop table if exists stage.dbo.wc_mdcd_flu_wmde_2018
 
 select distinct client_nbr 
 into stage.dbo.wc_mdcd_flu_wmde_2018
 from ( 
 select distinct client_nbr 
 FROM MEDICAID.dbo.ENRL_2018 a
 where elig_date between 201801 and 201812 
   and ( a.SMIB <> '0'  or a.ME_CODE = 'W' )
union 
  select distinct client_nbr 
 FROM MEDICAID.dbo.ENRL_2019 a
 where elig_date between 201801 and 201812 
   and ( a.SMIB <> '0'  or a.ME_CODE = 'W' )
  ) x 
  ;
 
--texas women and duel elig
 delete from stage.dbo.wc_mdcd_flu_2018_temp where client_nbr in ( select client_nbr from stage.dbo.wc_mdcd_flu_wmde_2018 )
 
 
--get only members covered all year  
select * 
into stage.dbo.wc_mdcd_flu_2018 
from stage.dbo.wc_mdcd_flu_2018_temp a 
where a.fst_elig = '201801' 
  and a.lst_elig = '201812'
;  

delete from stage.dbo.wc_mdcd_flu_2018 where age_group is null;

delete from stage.dbo.wc_mdcd_flu_2018 where zip3 = '771';

delete from stage.dbo.wc_mdcd_flu_2018 where sex = 'U';

drop table stage.dbo.wc_mdcd_flu_2018_temp

--sanity check - should be a few million age group 1, and much less other age groups
select count(*), age_group 
from stage.dbo.wc_mdcd_flu_2018
group by age_group order by age_group ;

---get encounters
drop table stage.dbo.wc_mdcd_flu_clm_2018;

select distinct derv_enc 
into  stage.dbo.wc_mdcd_flu_clm_2018
from ( 
  select derv_enc 
  from medicaid.dbo.ENC_det_19
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select derv_enc 
  from medicaid.dbo.ENC_det_18
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select icn 
  from MEDICAID.dbo.clm_detail_19 
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
    and FROM_DOS between '2018-01-01' and '2018-12-31'
union 
  select icn 
  from MEDICAID.dbo.clm_detail_18 
  where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
    and FROM_DOS between '2018-01-01' and '2018-12-31'
) inr ;


drop table stage.dbo.wc_mdcd_flu_mem_2018;

select distinct mem_id 
into stage.dbo.wc_mdcd_flu_mem_2018
from 
(
	select distinct a.mem_id 
	from medicaid.dbo.enc_proc_19 a 
	where a.DERV_ENC in ( select derv_enc from stage.dbo.wc_mdcd_flu_clm_2018) 
union 
	select distinct a.mem_id 
	from medicaid.dbo.enc_proc_18 a 
	where a.DERV_ENC in ( select derv_enc from stage.dbo.wc_mdcd_flu_clm_2018) 
union 
	select a.PCN
	from MEDICAID.dbo.clm_proc_19 a 
	where a.ICN in (select derv_enc from stage.dbo.wc_mdcd_flu_clm_2018) 
union 
	select a.PCN
	from MEDICAID.dbo.clm_proc_18 a 
	where a.ICN in (select derv_enc from  stage.dbo.wc_mdcd_flu_clm_2018) 
) inr ; 


--apply flag
alter table stage.dbo.wc_mdcd_flu_2018 add vacc_flag int default 0;


update stage.dbo.wc_mdcd_flu_2018 set vacc_flag = 1
  from stage.dbo.wc_mdcd_flu_mem_2018 b 
    where client_nbr = b.mem_id
 ;

--validate
select count(*), count(distinct client_nbr), sum(vacc_flag), age_group
from stage.dbo.wc_mdcd_flu_2018
group by age_group order by age_group;


--agg65 plus table to be sent to greenplum 
drop table stage.dbo.wc_mdcd_flu_agg_2018 ;

select * 
into stage.dbo.wc_mdcd_flu_agg_2018
from stage.dbo.wc_mdcd_flu_2018
where age_group = 7;

delete from stage.dbo.wc_mdcd_flu_2018 where age_group = 7;

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2018;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2018
where sex = 'F';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_flu_2018
where sex = 'M';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), age_group
from stage.dbo.wc_mdcd_flu_2018
group by age_group
order by age_group;


--- prevalance by zip 
select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where sex = 'F'
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where sex = 'M'
group by zip3
order by zip3;

---by age group
select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where age_group = 1
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where age_group = 2
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where age_group = 3
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where age_group = 4
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where age_group = 5
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_flu_2018
where age_group = 6
group by zip3
order by zip3;
