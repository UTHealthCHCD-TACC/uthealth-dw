--***********************************************************************************************
-----------tobacco users
--***********************************************************************************************
drop table if exists WRK.dbo.wc_TRS_tobacco_cohort_temp
select combo_ID, fscyr
into WRK.dbo.wc_TRS_tobacco_cohort_temp
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_CLM_FIN_NEW a
	       join wrk.dbo.wc_5a_smoking_dx d 
	         on ( replace(a.pri_icd9_dx_cd,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_2,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_3,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_4,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_5,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_6,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_7,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_8,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_9,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_10,'.','') = d.diag_cd 
	        )
		where a.MED_FSCYR between 2014 and 2021	       
) inrx;



insert into WRK.dbo.wc_TRS_tobacco_cohort_temp
select combo_ID, fscyr
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_BCBS_FIN_NEW a
	       join wrk.dbo.wc_5a_smoking_dx d 
	         on replace(a.diagnosis_code_1 ,'.','') = d.diag_cd 
	        or replace(a.diagnosis_code_2 ,'.','') = d.diag_cd 
	        or replace(a.diagnosis_code_3,'.','') = d.diag_cd 
	        or replace(a.diagnosis_code_4,'.','') = d.diag_cd 
	        or replace(a.diagnosis_code_5,'.','') = d.diag_cd 
		where a.MED_FSCYR between 2014 and 2021	       
) inrx;


-----get tobacco use cpt/hcpcs
insert into WRK.dbo.wc_TRS_tobacco_cohort_temp 
select combo_ID, fscyr
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_CLM_FIN_NEW a
		where a.MED_FSCYR between 2014 and 2021
		  and a.prcdr_cd  in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	               
) inrx;


insert into WRK.dbo.wc_TRS_tobacco_cohort_temp 
select combo_ID, fscyr
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_BCBS_FIN_NEW a
		where a.MED_FSCYR between 2014 and 2021	
		  and a.hcpcs_cpt_code  in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	               
) inrx;


--consolidate 
drop table if exists wrk.dbo.wc_trs_tobacco_cohort
select distinct combo_id, fscyr 
into WRK.dbo.wc_TRS_tobacco_cohort
from WRK.dbo.wc_TRS_tobacco_cohort_temp 
;


---**********************************************************************************************************
---- Tobacco Cessation 
---**********************************************************************************************************

--counselling dx and cpt/hcpcs
drop table if exists WRK.dbo.wc_TRS_tobacco_counselling_temp
select combo_id, med_FSCYR as fscyr 
into WRK.dbo.wc_TRS_tobacco_counselling_temp
		from trsers.dbo.TRS_CLM_FIN_NEW a
		where a.MED_FSCYR between 2016 and 2021
		 and (  replace(a.pri_icd9_dx_cd,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_2,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_3,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_4,'.','') = 'Z716' 
	        or replace(a.icd9_dx_cd_5,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_6,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_7,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_8,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_9,'.','') = 'Z716'
	        or replace(a.icd9_dx_cd_10,'.','') = 'Z716' 
	        or a.prcdr_cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458','4004F','4001F')	
	        )
;

insert into WRK.dbo.wc_TRS_tobacco_counselling_temp
select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_BCBS_FIN_NEW a
		where a.MED_FSCYR between 2016 and 2021 
		 and (  replace(a.diagnosis_code_1 ,'.','') = 'Z716'
	        or replace(a.diagnosis_code_2,'.','') = 'Z716'
	        or replace(a.diagnosis_code_3,'.','') = 'Z716'
	        or replace(a.diagnosis_code_4,'.','') = 'Z716' 
	        or replace(a.diagnosis_code_5 ,'.','') = 'Z716'
	        or a.hcpcs_cpt_code in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458','4004F','4001F')	
	        )
;

--counselling rx 
insert into  WRK.dbo.wc_TRS_tobacco_counselling_temp
select combo_id , FSCYR 
from TRSERS.dbo.TRS_RX_FIN_NEW
where Generic_Name like '%BUPROPION%'
  and FSCYR between 2016 and 2021;

 
 --consolidate
 drop table if exists WRK.dbo.wc_TRS_tobacco_counselling 
 select distinct combo_id, fscyr 
into WRK.dbo.wc_TRS_tobacco_counselling
 from WRK.dbo.wc_TRS_tobacco_counselling_temp
 ;

---**********************************************************************************************************
---get counts for spreadsheet--------------------------------------------------------------------------
---**********************************************************************************************************


---active vs cobra vs ret
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON
	where yearmonth in ('201608','201708','201808','201908','202008','202108')
	  and age >= 15
    )
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer 
from TRSERS.dbo.TRS_AGG_YR a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
 --tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR     
     /* 
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
     */
where a.FSCYR between 2016 and 2021 
group by a.FSCYR , stat 
union 
---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when rel = 'S' then 'E' when rel = 'D' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
--tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR     
     /* 
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
     */
where a.FSCYR between 2016 and 2021
group by a.FSCYR , rel, stat 
union
---age group active vs retiree vs cobra 
select replace( str(a.FSCYR) + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
--tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR     
     /* 
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
     */
where a.FSCYR between 2016 and 2021
group by  a.fscyr ,  stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
union 
---age group active vs retiree vs cobra / male vs female 
select replace( str(a.FSCYR) + gen + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
--tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR     
     /* 
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
     */
where  a.FSCYR between 2016 and 2021
group by  a.fscyr , gen, stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
 ;