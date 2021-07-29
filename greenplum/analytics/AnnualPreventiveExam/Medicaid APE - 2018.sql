/****** Medicaid Annual Prev Exam 2018 ******/

drop table stage.dbo.wc_mdcd_ape_2018;


select client_nbr, min(elig_month) as fst_elig, max(elig_month) as lst_elig, 
       min(zip3) as zip3, max(sex) as sex, min(age) as age , min(age_Group) as age_group
into stage.dbo.wc_mdcd_ape_2018_temp
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
FROM MEDICAID.dbo.CHIP_UTH_SFY2018_Final
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
FROM MEDICAID.dbo.CHIP_UTH_SFY2019_Final
  where elig_month between 201801 and 201812
  and substring(mailing_zip,1,3) between '750' and '799'
 union 
SELECT [CLIENT_NBR]
      ,substring([ZIP],1,3) as zip3
      ,[SEX]
	  ,age 
      ,case  when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	         else 7
	   end as age_group
	   ,elig_date 
  FROM [MEDICAID].[dbo].[ENRL_2018]
  where elig_date between 201801 and 201812
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
	         else 7
	   end as age_group
	   ,elig_date
  FROM [MEDICAID].[dbo].[ENRL_2019]
  where elig_date between 201801 and 201812
	and substring(zip,1,3) between '750' and '799'
  ) inr 
  group by client_nbr;

 
--exclude anyone in texas women or duel eligible
drop table if exists stage.dbo.wc_mdcd_ape_wmde_2018
 
 select distinct client_nbr 
 into stage.dbo.wc_mdcd_ape_wmde_2018
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
 delete from stage.dbo.wc_mdcd_ape_2018_temp where client_nbr in ( select client_nbr from stage.dbo.wc_mdcd_ape_wmde_2018 )
 
 
--get only members covered all year  
select * 
into stage.dbo.wc_mdcd_ape_2018
from stage.dbo.wc_mdcd_ape_2018_temp a 
where a.fst_elig = '201801' 
  and a.lst_elig = '201812'
;  

delete from stage.dbo.wc_mdcd_ape_2018 where age_group is null;

delete from stage.dbo.wc_mdcd_ape_2018 where zip3 = '771';

delete from stage.dbo.wc_mdcd_ape_2018 where sex = 'U';

select count(*), age_group from stage.dbo.wc_mdcd_ape_2018  group by age_group order by age_group;

drop table stage.dbo.wc_mdcd_ape_2018_temp

---------------------------------------------------------------------------------------------------------
----proc and hcpc
---------------------------------------------------------------------------------------------------------
drop table stage.dbo.wc_mdcd_ape_clm_2018 ;

select distinct derv_enc 
into stage.dbo.wc_mdcd_ape_clm_2018
from ( 
  select derv_enc
  from medicaid.dbo.ENC_DET_18
  where proc_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select derv_enc
  from medicaid.dbo.ENC_DET_19 d
  where proc_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
  and FDOS_DT between '2018-01-01' and '2018-12-31'
union 
  select ICN 
  from MEDICAID.dbo.CLM_DETAIL_18
  where proc_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
  and  FROM_DOS between '2018-01-01' and '2018-12-31'
union 
  select ICN
  from medicaid.dbo.CLM_DETAIL_19
  where proc_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
  and FROM_DOS between '2018-01-01' and '2018-12-31'  
) inr ;


---------------------------------------------------------------------------------------------------------
----Diagnosis Codes
---------------------------------------------------------------------------------------------------------
create table stage.dbo.wc_mdcd_ape_diag (dx_cd varchar(50));

insert into stage.dbo.wc_mdcd_ape_diag values
('Z0000'),('Z0001'),('Z00110'),('Z00111'),('Z00121'),('Z00129'),('Z003'),('Z01411'),('Z01419'),
				  ('V700'),('V700'),('V7231'),('V705'),('V703'),('V7284'),('V7285') ;


  select d.ICN 
into  stage.dbo.wc_mdcd_ape_dx_2018
  from medicaid.dbo.htw_clm_dx_18 d 
    join MEDICAID.dbo.CLM_HEADER_18 h 
      on h.ICN = d.ICN 
     and h.HDR_FRM_DOS between '2018-01-01' and '2018-12-31'
  where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        )
;

insert into  stage.dbo.wc_mdcd_ape_dx_2018
  select d.ICN 
  from medicaid.dbo.htw_clm_dx_19 d
    join MEDICAID.dbo.CLM_HEADER_19 h 
      on h.ICN = d.ICN 
     and h.HDR_FRM_DOS between '2018-01-01' and '2018-12-31'
  where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        )
;

insert into  stage.dbo.wc_mdcd_ape_dx_2018
  select d.DERV_ENC 
  from MEDICAID.dbo.enc_dx_18 d 
    join MEDICAID.dbo.ENC_HEADER_18 h 
      on h.DERV_ENC = d.DERV_ENC 
     and h.FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        )
;


insert into  stage.dbo.wc_mdcd_ape_dx_2018
  select d.DERV_ENC 
  from MEDICAID.dbo.enc_dx_19 d 
    join MEDICAID.dbo.ENC_HEADER_19 h 
      on h.DERV_ENC = d.DERV_ENC 
     and h.FRM_DOS between '2018-01-01' and '2018-12-31'
   where (   d.DX_CD_1 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_2 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_3 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_4 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_5 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_6 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_7 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_8 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        or d.DX_CD_9 in (select dx_cd from stage.dbo.wc_mdcd_ape_diag) 
        )
;



insert into stage.dbo.wc_mdcd_ape_clm_2018
select distinct ICN
from  stage.dbo.wc_mdcd_ape_dx_2018
;

---------------------------------------------------------------------------------------------------------
----Members from claim ids
--------------------------------------------------------------------------------------------------------
drop table stage.dbo.wc_mdcd_ape_mem_2018;

select distinct mem_id 
into stage.dbo.wc_mdcd_ape_mem_2018
from (
		select distinct a.mem_id 
		from medicaid.dbo.enc_proc_18 a 
		where a.DERV_ENC in (select derv_enc from stage.dbo.wc_mdcd_ape_clm_2018) 
	union 
		select distinct a.mem_id 
		from medicaid.dbo.enc_proc_19 a 
		where a.DERV_ENC in (select derv_enc from stage.dbo.wc_mdcd_ape_clm_2018) 
	union 
		select a.PCN
		from MEDICAID.dbo.clm_proc_18 a 
		where a.ICN in (select derv_enc from stage.dbo.wc_mdcd_ape_clm_2018) 
    union 
		select a.PCN
		from MEDICAID.dbo.clm_proc_19 a 
		where a.ICN in (select derv_enc from stage.dbo.wc_mdcd_ape_clm_2018) 
	) inr ; 


select * 
from  stage.dbo.wc_mdcd_ape_mem_2018

-------------------------------------------------

alter table stage.dbo.wc_mdcd_ape_2018 add vacc_flag int default 0;


update stage.dbo.wc_mdcd_ape_2018 set vacc_flag = 1
  from stage.dbo.wc_mdcd_ape_mem_2018 b 
    where client_nbr = b.mem_id
 ;

--agg table to be sent to gp
drop table stage.dbo.wc_mdcd_ape_agg_2018 ;

select * 
into stage.dbo.wc_mdcd_ape_agg_2018 
from stage.dbo.wc_mdcd_ape_2018 a 
where a.age_group = 7;

delete from stage.dbo.wc_mdcd_ape_2018 where age_group = 7;

select count(*), count(distinct client_nbr), sum(vacc_flag) from stage.dbo.wc_mdcd_ape_2018;


----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_ape_2018;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_ape_2018
where sex = 'F';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag)
from stage.dbo.wc_mdcd_ape_2018
where sex = 'M';

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), age_group
from stage.dbo.wc_mdcd_ape_2018
group by age_group
order by age_group;


--- prevalance by zip 

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where sex = 'F'
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where sex = 'M'
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where age_group = 1
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where age_group = 2
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where age_group = 3
group by zip3
order by zip3;

--4
select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where age_group = 4
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where age_group = 5
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(client_nbr) as float ) )*100 as prev, count(client_nbr), sum(vacc_flag), zip3 
from stage.dbo.wc_mdcd_ape_2018
where age_group = 6
group by zip3
order by zip3;

