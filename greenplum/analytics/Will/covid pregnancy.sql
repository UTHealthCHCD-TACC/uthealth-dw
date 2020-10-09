

select distinct replace(Code,'.','') cd, * 
into stage.dbo.wc_preg_covid 
from REF.dbo.[Value_Sets_Codes_2020] where [Value Set Name] like '%Pregnancy Diagnosis%'


select * 
from stage.dbo.wc_preg_covid
where cd = 'J208'


drop table stage.dbo.wc_preg_covid_ptid;

select distinct ptid  
into stage.dbo.wc_preg_covid_ptid
from COVID.dbo.cov_20200820_diag cd 
where DIAGNOSIS_CD in ( select cd from stage.dbo.wc_preg_covid)
 and cast(diag_date as date) >= '2020-01-01'
;


---covid positive
drop table stage.dbo.wc_preg_covid_positives;


select * 
into stage.dbo.wc_preg_covid_positives
from (
select distinct ptid, diag_date as covid_date 
from COVID.dbo.cov_20200820_diag cd 
where diagnosis_cd in ('B9729', 'J1289', 'J208', 'J22', 'J40', 'J80', 'J988')
  and cast(diag_date as date) >= '02-20-2020'
union 
select distinct ptid, diag_date as covid_date 
from COVID.dbo.cov_20200820_diag cd 
where DIAGNOSIS_CD in ('U071', 'U072', 'U073')
  and cast(diag_date as date) >= '02-01-2020' 
) inr 
;


---counts of preg women
select count(*), count(distinct ptid), age_group 
from ( 
select case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 1
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 2
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 3
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 4
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 5
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 6
            when 2020 - cast(BIRTH_YR as int) > 74 then 7
            end as age_group
            , * 
from COVID.dbo.cov_20200820_pt 
where GENDER = 'Female'
and LEN(birth_yr) = 4
and ptid in ( select ptid from stage.dbo.wc_preg_covid_positives)
and ptid in ( select ptid from stage.dbo.wc_preg_covid_ptid )
) inr 
group by age_group order by age_group 


select count(*), race
from ( 
select case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 1
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 2
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 3
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 4
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 5
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 6
            when 2020 - cast(BIRTH_YR as int) > 74 then 7
            end as age_group
            , * 
from COVID.dbo.cov_20200820_pt 
where GENDER = 'Female'
and LEN(birth_yr) = 4
--and ptid in ( select ptid from stage.dbo.wc_preg_covid_positives)
and ptid in ( select ptid from stage.dbo.wc_preg_covid_ptid )
) inr 
group by race order by race



---visit type
select cv.VISIT_TYPE, count(distinct cv.ptid) as women, count(distinct cv.ptid+cv.VISIT_START_DATE ) as admissions
from COVID.dbo.cov_20200820_vis cv
  join covid.dbo.cov_20200820_pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'
   and len(BIRTH_YR ) = 4
  join stage.dbo.wc_preg_covid_positives a 
    on a.ptid = cv.ptid 
   and a.covid_date between cv.VISIT_START_DATE and cv.VISIT_END_DATE 
 where VISIT_TYPE  in ('Inpatient','EMERGENCY PATIENT','Observation patient')
--and cp.ptid in ( select ptid from stage.dbo.wc_preg_covid_ptid )
and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
 group by VISIT_TYPE 
 order by VISIT_TYPE 
;


---visit type and discharge
select cv.VISIT_TYPE, cv.DISCHARGE_DISPOSITION , count(distinct cv.ptid) as women, count(distinct cv.ptid+cv.VISIT_START_DATE ) as admissions
from COVID.dbo.cov_20200820_vis cv
  join covid.dbo.cov_20200820_pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'
   and len(BIRTH_YR ) = 4
  join stage.dbo.wc_preg_covid_positives a 
    on a.ptid = cv.ptid 
   and a.covid_date between cv.VISIT_START_DATE and cv.VISIT_END_DATE 
 where VISIT_TYPE  in ('Inpatient','EMERGENCY PATIENT','Observation patient')
--and cp.ptid in ( select ptid from stage.dbo.wc_preg_covid_ptid )
and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
 group by VISIT_TYPE , cv.DISCHARGE_DISPOSITION 
 order by VISIT_TYPE , cv.DISCHARGE_DISPOSITION 
;


--- age group disdcharge and visit type 
select case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 1
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 2
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 3
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 4
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 5
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 6
            when 2020 - cast(BIRTH_YR as int) > 74 then 7
            end as age_group,
         cv.VISIT_TYPE, cv.DISCHARGE_DISPOSITION, 
         count(distinct cv.ptid) as women, 
         count(distinct cv.ptid+cv.VISIT_START_DATE ) as admissions
from COVID.dbo.cov_20200820_vis cv
  join covid.dbo.cov_20200820_pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'
   and len(BIRTH_YR ) = 4
  join stage.dbo.wc_preg_covid_positives a 
    on a.ptid = cv.ptid 
   and a.covid_date between cv.VISIT_START_DATE and cv.VISIT_END_DATE 
 where VISIT_TYPE  in ('Inpatient','EMERGENCY PATIENT','Observation patient')
--and cp.ptid in ( select ptid from stage.dbo.wc_preg_covid_ptid )
and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
 group by case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 1
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 2
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 3
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 4
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 5
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 6
            when 2020 - cast(BIRTH_YR as int) > 74 then 7
            end, VISIT_TYPE , cv.DISCHARGE_DISPOSITION 
 order by case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 1
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 2
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 3
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 4
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 5
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 6
            when 2020 - cast(BIRTH_YR as int) > 74 then 7
            end, VISIT_TYPE , cv.DISCHARGE_DISPOSITION 
;

