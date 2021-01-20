select distinct a.interpret 
from shared.covid_positive_20201015 a 


select *
from covid.dbo.temp_lab_20201015_new tln 



--build this table in SPHCDESQL and export
select distinct replace(Code,'.','') cd, * 
into stage.dbo.wc_preg_covid 
from REF.dbo.[Value_Sets_Codes_2020] where [Value Set Name] like '%Pregnancy Diagnosis%'


select * from stage.dbo.wc_preg_covid 


drop table STAGE.dbo.wc_cov_pregnant_ptids

---pregnant women
select  ptid  , min(diag_date) as preg_dt
into STAGE.dbo.wc_cov_pregnant_ptids
from COVID.dbo.cov_20201015_diag  
where DIAGNOSIS_CD in ( select cd from stage.dbo.wc_preg_covid )
 and cast(diag_date as date) >= '2020-01-01'
 group by ptid
;


---covid positive
drop table STAGE.dbo.wc_cov_all_covid_positives;

--select distinct PTID, min(RESULT_DATE) as covid_first_date

select ptid, min(result_date) as covid_first_date 
into STAGE.dbo.wc_cov_all_covid_positives
from COVID.dbo.temp_lab_20201015_new 
where Interpret = 'Pos'
group by ptid 
;

select distinct interpret from COVID.dbo.temp_lab_20201015_new 



drop table STAGE.dbo.wc_cov_all_covid_negatives;

---covid negatives
select ptid, max(result_date) as covid_neg_date
into STAGE.dbo.wc_cov_all_covid_negatives
from COVID.dbo.temp_lab_20201015_new 
where Interpret = 'Neg'
group by ptid
;

select * --count(*) , count(distinct ptid) 
from STAGE.dbo.wc_cov_all_covid_positives



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
and 2020 - cast(a.BIRTH_YR as int) between 15 and 54
and ptid in ( select ptid from STAGE.dbo.wc_cov_all_covid_positives)
and ptid in ( select ptid from STAGE.dbo.wc_cov_pregnant_ptids)
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
from COVID.dbo.cov_20201015_pt a 
where GENDER = 'Female'
and LEN(birth_yr) = 4
and 2020 - cast(a.BIRTH_YR as int) between 15 and 54
and ptid in ( select ptid from STAGE.dbo.wc_cov_all_covid_positives)
and ptid in ( select ptid from STAGE.dbo.wc_cov_pregnant_ptids)
) inr 
group by race order by race



---visit type
select cv.VISIT_TYPE, count(distinct cv.ptid) as women, count(distinct cv.ptid + cv.VISIT_START_DATE ) as admissions


select count(*) , count(distinct ptid) from STAGE.dbo.wc_cov_pregnant_ptids where ptid not in ( select ptid from COVID.dbo.cov_20201015_vis where cast(VISIT_START_DATE as date) >= '2020-01-01')

select cp.ptid, cv.visit_type, cast(cv.VISIT_START_DATE as date) as visit_start , cv.ptid + cv.VISIT_START_DATE, cp.RACE, cp.BIRTH_YR , preg.preg_dt,
       pos.covid_first_date, neg.covid_neg_date
from COVID.dbo.cov_20201015_vis cv
  join COVID.dbo.cov_20201015_pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'
   and len(BIRTH_YR ) = 4
   and 2020 - cast(cp.BIRTH_YR as int) between 15 and 54
  join STAGE.dbo.wc_cov_pregnant_ptids preg 
     on preg.ptid = cp.ptid 
  left outer join STAGE.dbo.wc_cov_all_covid_positives pos
      on pos.ptid = cp.ptid 
  left outer join  STAGE.dbo.wc_cov_all_covid_negatives neg
     on neg.ptid = cp.ptid 
 where VISIT_TYPE  in ('Inpatient','Emergency patient','Observation patient')
and cp.ptid in ( select ptid from STAGE.dbo.wc_cov_pregnant_ptids )
--and cp.ptid in ( select ptid from STAGE.dbo.wc_cov_all_covid_positives )
and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
and cast(cv.VISIT_START_DATE as date) >= '2020-01-01'
 order by cp.ptid, cast(cv.VISIT_START_DATE as date)
;


--- all pregs
select count(*), count(distinct ptid) as total_pregnant_women, count(distinct vis_id), sum(cov_cnt) as cov_cnt 
from (
select cp.ptid, cv.visit_type, cast(cv.VISIT_START_DATE as date) as visit_start , cv.ptid + cv.VISIT_START_DATE as vis_id, 
       cp.RACE, cp.BIRTH_YR , 2020 - cast(cp.BIRTH_YR as int) as age, 
       preg.preg_dt, pos.covid_first_date, neg.covid_neg_date,
       case when cv.ptid is null then 1 else 0 end as cov_cnt 
      -- into stage.dbo.wc_covid_pregnancy_study_extract
from COVID.dbo.cov_20201015_pt cp 
  join STAGE.dbo.wc_cov_pregnant_ptids preg 
      on preg.ptid = cp.ptid 
  left outer join STAGE.dbo.wc_cov_all_covid_positives pos
      on pos.ptid = cp.ptid 
  left outer join  STAGE.dbo.wc_cov_all_covid_negatives neg
      on neg.ptid = cp.ptid 
  left outer join COVID.dbo.cov_20201015_vis cv 
	  on cp.ptid  = cv.PTID 
	  and cp.ptid in ( select ptid from STAGE.dbo.wc_cov_pregnant_ptids )
	 -- and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
	  and cast(cv.VISIT_START_DATE as date) >= '2020-01-01'
where  cp.GENDER = 'Female'
   and len(BIRTH_YR ) = 4
   and 2020 - cast(cp.BIRTH_YR as int) between 15 and 54
 --order by cp.ptid, cast(cv.VISIT_START_DATE as date)
) a 
where cov_cnt = 1;


select * --count(*), INTERACTION_TYPE 
from COVID.dbo.cov_20201015_enc ce 
where cast(INTERACTION_DATE as date) >= '2020-01-01'

group by INTERACTION_TYPE 

select * from COVID.dbo.temp_lab_20201015_new where PTID = 'PT116166443'

