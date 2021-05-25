--***********************************************************************************************
-----------diag to find denominator (obese population)
--***********************************************************************************************
drop table if exists WRK.dbo.wc_TRS_tobacco_cohort_temp;


-- wrk.dbo.wc_5a_smoking_dx built in ERS file 

select * from trsers.dbo.TRS_CLM_FIN_NEW;

select combo_ID, fscyr
into WRK.dbo.wc_TRS_tobacco_cohort_temp
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_CLM_FIN_NEW a
	       join wrk.dbo.wc_5a_smoking_dx d 
	         on replace(a.pri_icd9_dx_cd,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_2,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_3,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_4,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_5,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_6,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_7,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_8,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_9,'.','') = d.diag_cd 
	        or replace(a.icd9_dx_cd_10,'.','') = d.diag_cd 
		where a.MED_FSCYR between 2014 and 2019 	       
) inrx;




-----get tobacco use cpt/hcpcs
insert into WRK.dbo.wc_TRS_tobacco_cohort_temp 
select combo_ID, fscyr
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_CLM_FIN_NEW a
		where a.MED_FSCYR between 2014 and 2019 	
		  and a.prcdr_cd  in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	               
) inrx;


--consolidate 
select distinct combo_id, fscyr 
into WRK.dbo.wc_TRS_tobacco_cohort
from WRK.dbo.wc_TRS_tobacco_cohort_temp 
;


---**********************************************************************************************************
---- Tobacco Cessation 
---**********************************************************************************************************

drop table if exists WRK.dbo.wc_TRS_tobacco_counselling_temp;

--counselling dx and cpt/hcpcs
select combo_id, med_FSCYR as fscyr 
into WRK.dbo.wc_TRS_tobacco_counselling_temp
		from trsers.dbo.TRS_CLM_FIN_NEW a
		where a.MED_FSCYR between 2016 and 2019 
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


--counselling rx 
insert into into WRK.dbo.wc_TRS_tobacco_counselling_temp
select combo_id , FSCYR 
from TRSERS.dbo.TRS_RX_FIN_NEW
where Generic_Name like '%BUPROPION%'
  and FSCYR between 2016 and 2019;

 
 --consolidate
 select distinct combo_id, fscyr 
into WRK.dbo.wc_TRS_tobacco_counselling
 from WRK.dbo.wc_TRS_tobacco_counselling_temp
 ;

---**********************************************************************************************************
---get counts for spreadsheet--------------------------------------------------------------------------
---**********************************************************************************************************


--validate 1 rec per mem per year 
select count(*), count(distinct combo_id), FSCYR 
from TRSERS.dbo.TRS_AGG_YR_FIN 
group by FSCYR order by FSCYR;

select count(*), count(distinct combo_id), FSCYR 
from WRK.dbo.wc_TRS_tobacco_cohort
group by FSCYR order by FSCYR;

select count(*), count(distinct combo_id), FSCYR 
from WRK.dbo.wc_TRS_tobacco_counselling
group by FSCYR order by FSCYR;




---active vs cobra vs ret
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')
	  and age >= 15
    )
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer 
from TRSERS.dbo.TRS_AGG_YR_FIN a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
 /*--tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR   
  */   
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
where a.FSCYR between 2016 and 2019 
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')
	  and age >= 15
    )
select replace( (str(a.FSCYR) +  stat + case when rel = 'S' then 'E' when rel = 'D' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR_FIN a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
 /*--tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR   
  */   
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
where a.FSCYR between 2016 and 2019 
group by a.FSCYR , rel, stat 
order by a.FSCYR, stat, rel desc
;



---age group active vs retiree vs cobra 
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')	
	  and age >= 15
    )
select replace( str(a.FSCYR) + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR_FIN a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
 /*--tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR   
  */   
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
where a.FSCYR between 2016 and 2019 
group by  a.fscyr ,  stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
order by  a.fscyr,  stat,    case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
 ;



---age group active vs retiree vs cobra / male vs female 
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')	
	  and age >= 15
    )
select replace( str(a.FSCYR) + gen + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.combo_id) as denom, count(distinct c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR_FIN a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
 /*--tobacco use 
  left outer join WRK.dbo.wc_TRS_tobacco_cohort c
     on c.combo_id = a.combo_id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR   
  */   
 --tobacco counselling
  join WRK.dbo.wc_TRS_tobacco_cohort b
     on b.combo_id = a.combo_id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR  
  left outer join WRK.dbo.wc_TRS_tobacco_counselling c 
      on c.combo_id = a.combo_id 
     and c.fscyr = a.FSCYR   
where  a.FSCYR between 2016 and 2019 
group by  a.fscyr , gen, stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
order by  a.fscyr, a.gen, stat,    case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
 ;