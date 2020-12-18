select distinct a.interpret 
from shared.covid_positive_20201015 a 


--build this table in SPHCDESQL and export
select distinct replace(Code,'.','') cd, * 
into stage.dbo.wc_preg_covid 
from REF.dbo.[Value_Sets_Codes_2020] where [Value Set Name] like '%Pregnancy Diagnosis%'


select * from stage.dbo.wc_preg_covid 



---pregnant women
select distinct ptid  
into STAGE.dbo.wc_cov_pregnant_ptids
from COVID.dbo.cov_20201015_diag  
where DIAGNOSIS_CD in ( select cd from stage.dbo.wc_preg_covid )
 and cast(diag_date as date) >= '2020-01-01'
;


---covid positive
drop table STAGE.dbo.wc_cov_all_covid_positives;

--select distinct PTID, min(RESULT_DATE) as covid_first_date

select distinct ptid, result_date
into stage.dbo.wc_temp
--into STAGE.dbo.wc_cov_all_covid_positives
from COVID.dbo.temp_lab_20201015_new 
where Interpret = 'Pos'

group by ptid 
;

select * --count(*) , count(distinct ptid) 
from STAGE.dbo.wc_cov_all_covid_positives

select count(*) 
from (
select count(result_date) as rd,ptid from stage.dbo.wc_temp
group by ptid
having count(result_date) > 1
)x;

select count(distinct ptid) from stage.dbo.wc_temp;


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
from COVID.dbo.cov_20201015_pt a 
where GENDER = 'Female'
and LEN(birth_yr) = 4
and ptid in ( select ptid from STAGE.dbo.wc_cov_all_covid_positives)
--and ptid in ( select ptid from STAGE.dbo.wc_cov_pregnant_ptids)
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
from opt_20201112.pt 
where GENDER = 'Female'
and length(birth_yr) = 4
and ptid in ( select ptid from g823066.wc_preg_covid_positives)
--and ptid in ( select ptid from g823066.wc_preg_covid_ptid )
) inr 
group by race order by race



---visit type
select cv.VISIT_TYPE, count(distinct cv.ptid) as women, count(distinct cv.ptid || cv.VISIT_START_DATE ) as admissions
from opt_20201112.vis cv
  join opt_20201112.pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'
   and length(BIRTH_YR ) = 4
  join g823066.wc_preg_covid_positives a 
    on a.ptid = cv.ptid 
   and a.covid_date between cv.VISIT_START_DATE and cv.VISIT_END_DATE 
 where VISIT_TYPE  in ('Inpatient','Emergency patient','Observation patient')
--and cp.ptid in ( select ptid from g823066.wc_preg_covid_ptid )
and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
 group by VISIT_TYPE 
 order by VISIT_TYPE 
;

select distinct discharge_disposition from opt_20201112.vis

select * from opt_20201112.vis where substring(discharge_disposition ,1,2) = '01'

---visit type and discharge
select cv.VISIT_TYPE, substring(discharge_disposition ,1,2) , lower(min(discharge_disposition)) as desc, count(distinct cv.ptid) as women, count(distinct cv.ptid || cv.VISIT_START_DATE ) as admissions
from opt_20201112.vis cv
  join opt_20201112.pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'
   and length(BIRTH_YR ) = 4
  join g823066.wc_preg_covid_positives a 
    on a.ptid = cv.ptid 
   and a.covid_date between cv.VISIT_START_DATE and cv.VISIT_END_DATE 
 where VISIT_TYPE  in ('Inpatient','Emergency patient','Observation patient')
--and cp.ptid in ( select ptid from g823066.wc_preg_covid_ptid )
and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
 group by VISIT_TYPE , substring(discharge_disposition ,1,2)
 order by VISIT_TYPE , substring(discharge_disposition ,1,2)
;


--- age group disdcharge and visit type 
select case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 'AGE BETWEEN 0 AND 19'
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 'AGE BETWEEN 20 AND 34'
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 'AGE BETWEEN 35 AND 44'
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 'AGE BETWEEN 45 AND 54'
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 'AGE BETWEEN 55 AND 64'
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 'AGE BETWEEN 65 AND 74'
            when 2020 - cast(BIRTH_YR as int) > 74 then 'AGE GREATER THAN 74'
            end as age_group,
         cv.VISIT_TYPE, substring(discharge_disposition ,1,2) , lower(min(discharge_disposition)) as desc,
         count(distinct cv.ptid) as women, 
         count(distinct cv.ptid||cv.VISIT_START_DATE ) as admissions
from opt_20201112.vis cv
  join opt_20201112.pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'
   and length(BIRTH_YR ) = 4
  join g823066.wc_preg_covid_positives a 
    on a.ptid = cv.ptid 
   and a.covid_date between cv.VISIT_START_DATE and cv.VISIT_END_DATE 
 where VISIT_TYPE  in ('Inpatient','Emergency patient','Observation patient')
--and cp.ptid in ( select ptid from g823066.wc_preg_covid_ptid )
and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
 group by case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 'AGE BETWEEN 0 AND 19'
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 'AGE BETWEEN 20 AND 34'
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 'AGE BETWEEN 35 AND 44'
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 'AGE BETWEEN 45 AND 54'
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 'AGE BETWEEN 55 AND 64'
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 'AGE BETWEEN 65 AND 74'
            when 2020 - cast(BIRTH_YR as int) > 74 then 'AGE GREATER THAN 74'
            end, VISIT_TYPE , substring(discharge_disposition ,1,2)
 order by case when 2020 - cast(BIRTH_YR as int) between 0 and 19 then 'AGE BETWEEN 0 AND 19'
            when 2020 - cast(BIRTH_YR as int) between 20 and 34 then 'AGE BETWEEN 20 AND 34'
            when 2020 - cast(BIRTH_YR as int) between 35 and 44 then 'AGE BETWEEN 35 AND 44'
            when 2020 - cast(BIRTH_YR as int) between 45 and 54 then 'AGE BETWEEN 45 AND 54'
            when 2020 - cast(BIRTH_YR as int) between 55 and 64 then 'AGE BETWEEN 55 AND 64'
            when 2020 - cast(BIRTH_YR as int) between 65 and 74 then 'AGE BETWEEN 65 AND 74'
            when 2020 - cast(BIRTH_YR as int) > 74 then 'AGE GREATER THAN 74'
            end, VISIT_TYPE , substring(discharge_disposition ,1,2)
;

