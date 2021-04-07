drop table stage.dbo.wc_5a_hpv_clms;

----depression criteria	

---from claims
select pcn, cast(fst_dt as date) as fst_dt 
into stage.dbo.wc_5a_hpv_clms
from (
	select p.pcn , d.FROM_DOS as fst_dt
	from [MEDICAID].[dbo].[CLM_DETAIL_16] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_16] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('90649','90650','90651')
union 
	select p.pcn , d.FROM_DOS
	from [MEDICAID].[dbo].[CLM_DETAIL_17] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_17] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('90649','90650','90651')
union 
	select p.pcn , d.FROM_DOS
	from [MEDICAID].[dbo].[CLM_DETAIL_18] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_18] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('90649','90650','90651')
union 
	select p.pcn, d.FROM_DOS
	from [MEDICAID].[dbo].[CLM_DETAIL_19] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_19] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('90649','90650','90651')
) inr;	
	
---from encounter
insert into stage.dbo.wc_5a_hpv_clms
select distinct mem_id,  cast(fst_dt as date) as fst_dt 
from (
	select p.MEM_ID , d.FDOS_DT as fst_dt 
	from [MEDICAID].[dbo].[ENC_DET_16] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_16] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('90649','90650','90651')
union 
	select p.MEM_ID, d.FDOS_DT
	from [MEDICAID].[dbo].[ENC_DET_17] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_17] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('90649','90650','90651')
union 
	select p.MEM_ID, d.FDOS_DT
	from [MEDICAID].[dbo].[ENC_DET_18] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_18] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('90649','90650','90651')
union 
	select p.MEM_ID, d.FDOS_DT
	from [MEDICAID].[dbo].[ENC_DET_19] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_19] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('90649','90650','90651')	
) inr_enc;




----------------------------------------------------get one member per year for counting purposes

--cohort
drop table if exists stage.dbo.wc_5a_hpv_cohort
select distinct pcn, fscyr 
into stage.dbo.wc_5a_hpv_cohort
from stage.dbo.wc_5a_hpv_clms;


select pcn 
into stage.dbo.wc_5a_hpv_vacc
from (
select pcn, count(fst_dt) as cnt 
from stage.dbo.wc_5a_hpv_clms
group by pcn 
) inr 
where cnt > 1
;
			

----************************************************************************************************
----get counts for spreadsheet
------------------------------------




----overall by medicaid type
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from [stage].[dbo].[AGG_ENRL_MCD_YR] 
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( (str(a.ENRL_FY) + MCO_PROGRAM_NM), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl  a 
  left outer join stage.dbo.wc_5a_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
where  a.ENRL_FY between 2016 and 2019
  and age = 13
  and Em >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM
order by a.ENRL_FY, a.MCO_PROGRAM_NM ;



---overall dual eligible
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from [stage].[dbo].[AGG_ENRL_MCD_YR] 
                       where SMIB = 1
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( (str(a.ENRL_FY) + 'DUAL ELIGIBLE'), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join stage.dbo.wc_5a_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
where  a.ENRL_FY between 2016 and 2019
  and age = 13 
  and em >=12
  and a.SMIB = 1
group by a.ENRL_FY
order by a.ENRL_FY
;


---by age group and medicaid type
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from [stage].[dbo].[AGG_ENRL_MCD_YR] 
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( (str(a.ENRL_FY) + MCO_PROGRAM_NM  + str(a.AgeGrp) ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join stage.dbo.wc_5a_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
where  a.ENRL_FY between 2016 and 2019
  and age = 13
  and em >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM, a.AgeGrp 
order by a.ENRL_FY, a.MCO_PROGRAM_NM, a.AgeGrp ;


---by age group, gender, and medicaid type
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from [stage].[dbo].[AGG_ENRL_MCD_YR] 
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( (str(a.ENRL_FY) + MCO_PROGRAM_NM + SEX + str(a.AgeGrp) ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join stage.dbo.wc_5a_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
where  a.ENRL_FY between 2016 and 2019
  and age = 13
  and em >=12
  and sex in ('M','F')
group by a.ENRL_FY , sex, a.MCO_PROGRAM_NM, a.AgeGrp  
order by a.ENRL_FY, sex, a.MCO_PROGRAM_NM, a.AgeGrp 
;







