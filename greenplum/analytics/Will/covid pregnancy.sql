select distinct a.interpret 
from shared.covid_positive_20210128 a 


select *
from covid.dbo.temp_lab_20210128_new tln 



--build this table in SPHCDESQL and export
select distinct replace(Code,'.','') cd, * 
into stage.dbo.wc_preg_covid 
from REF.dbo.[Value_Sets_Codes_2020] where [Value Set Name] like '%Pregnancy Diagnosis%'


select * from stage.dbo.wc_preg_covid 


select * from  dev.wc_covid_preg_codeset;


drop table g823066.dbo.wc_preg_pregnant_mems;

---pregnant women
select  ptid  , min(diag_date) as preg_dt
into g823066.wc_preg_pregnant_mems
from opt_20210401.diag d 
where DIAGNOSIS_CD in ( select cd from g823066.wc_preg_codes )
 and cast(diag_date as date) >= '2020-01-01'
 group by ptid
;


---covid positive
drop table g823066.wc_preg_covid_positive_mems;

--select distinct PTID, min(RESULT_DATE) as covid_first_date


select * 
into g823066.wc_preg_covid_positive_mems
from (
select distinct ptid, diag_date as covid_date 
from opt_20210401.diag cd 
where diagnosis_cd in ('B9729', 'J1289', 'J208', 'J22', 'J40', 'J80', 'J988')
  and cast(diag_date as date) >= '02-20-2020'
union 
select distinct ptid, diag_date as covid_date 
from opt_20210401.diag cd 
where DIAGNOSIS_CD in ('U071', 'U072', 'U073')
  and cast(diag_date as date) >= '02-01-2020' 
) inr 


select ptid, min(result_date) as covid_first_date 
into STAGE.dbo.wc_cov_all_covid_positives
from COVID.dbo.temp_lab_20210128_new 
where Interpret = 'Pos'
group by ptid 
;

select distinct interpret from COVID.dbo.temp_lab_20210128_new 



drop table STAGE.dbo.wc_cov_all_covid_negatives;

---covid negatives
select ptid, max(result_date) as covid_neg_date
into STAGE.dbo.wc_cov_all_covid_negatives
from COVID.dbo.temp_lab_20210128_new 
where Interpret = 'Neg'
group by ptid
;

select * --count(*) , count(distinct ptid) 
from STAGE.dbo.wc_cov_all_covid_positives



<<<<<<< Updated upstream
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
from COVID.dbo.cov_20210128_pt 
where GENDER = 'Female'
and LEN(birth_yr) = 4
and 2020 - cast(a.BIRTH_YR as int) between 15 and 54
and ptid in ( select ptid from STAGE.dbo.wc_cov_all_covid_positives)
and ptid in ( select ptid from STAGE.dbo.wc_cov_pregnant_ptids)
) inr 
group by race order by race



---visit type
select cv.VISIT_TYPE, count(distinct cv.ptid) as women, count(distinct cv.ptid+cv.VISIT_START_DATE ) as admissions
from COVID.dbo.cov_20210128_vis cv
  join covid.dbo.cov_20210128_pt cp 
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
from COVID.dbo.cov_20210128_vis cv
  join covid.dbo.cov_20210128_pt cp 
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
from COVID.dbo.cov_20210128_vis cv
  join covid.dbo.cov_20210128_pt cp 
   on cp.ptid  = cv.PTID 
   and cp.GENDER = 'Female'

=======
>>>>>>> Stashed changes
--- all pregs
select count(*), count(distinct ptid) as total_pregnant_women, count(distinct vis_id), sum(cov_cnt) as cov_cnt 
from (


----create extract table with visit info
drop table if exists stage.dbo.wc_covid_pregnancy_study_extract

select cp.ptid, cv.visit_type, cast(cv.VISIT_START_DATE as date) as visit_start , cv.ptid + cv.VISIT_START_DATE as vis_id, 
       cp.RACE, cp.BIRTH_YR , 2020 - cast(cp.BIRTH_YR as int) as age, 
       preg.preg_dt, pos.covid_first_date, neg.covid_neg_date
       --case when cv.ptid is null then 1 else 0 end as cov_cnt 
       into stage.dbo.wc_covid_pregnancy_study_extract
from COVID.dbo.cov_20210128_pt cp 
  join STAGE.dbo.wc_cov_pregnant_ptids preg 
      on preg.ptid = cp.ptid 
  left outer join STAGE.dbo.wc_cov_all_covid_positives pos
      on pos.ptid = cp.ptid 
  left outer join  STAGE.dbo.wc_cov_all_covid_negatives neg
      on neg.ptid = cp.ptid 
  left outer join COVID.dbo.cov_20210128_vis cv 
	  on cp.ptid  = cv.PTID 
	 -- and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
	  and cast(cv.VISIT_START_DATE as date) >= '2020-01-01'
where  cp.GENDER = 'Female'
   and len(BIRTH_YR ) = 4
   and 2020 - cast(cp.BIRTH_YR as int) between 15 and 54
 --order by cp.ptid, cast(cv.VISIT_START_DATE as date)

   ) a 
where cov_cnt = 1;


----*********************************************************************
---deliveries 
----*********************************************************************
drop table if exists stage.dbo.wc_covid_deliveries_temp;

drop table if exists stage.dbo.wc_covid_deliveries;

--deliveries diag
select  PTID, cast(DIAG_DATE as date) as delivery_date
into stage.dbo.wc_covid_deliveries_temp
from COVID.dbo.cov_20210128_diag a
where a.DIAGNOSIS_CD in ('Z3800','Z3801','Z381','Z382','Z3830','Z3831','Z384','Z385','Z3861','Z3862',
'Z3863','Z3864','Z3865','Z3866','Z3868','Z3869','Z387','Z388','Z370','Z379','Z371')
and cast(diag_date as date) >= '2020-01-01'
;



---deliveries icd proc
insert into stage.dbo.wc_covid_deliveries_temp
select ptid, cast(PROC_DATE as date)
from COVID.dbo.cov_20210128_proc a 
where a.PROC_CODE in ( '10D00Z0','10D00Z1','10D00Z2','10D07Z3','10D07Z4','10D07Z5',
'10D07Z6','10D07Z7','10D07Z8','10E0XZZ''O321XX0','O322XX0','O323XX0','O329XX0','O364XX0',
'O6010X0','O6012X0','O6013X0','O6014X0','O641XX0','O642XX0','O643XX0','O641XX0')
and cast(PROC_DATE as date) >= '2020-01-01'
;


select * 
from stage.dbo.wc_covid_deliveries_temp
order by ptid

--delivieries drgs
insert into stage.dbo.wc_covid_deliveries_temp
select ptid,  cast(a.VISIT_START_DATE as date)
from COVID.dbo.cov_20210128_vis a
where a.DRG in ('370','371','372','373','374','375',
'765','766','767','768','774','775')
  and cast(a.VISIT_START_DATE as date) >= '2020-01-01'
 ;
 

---deliveries cpt/hcpc
insert into stage.dbo.wc_covid_deliveries_temp
select ptid, cast(PROC_DATE as date)
from COVID.dbo.cov_20210128_proc a 
where a.PROC_CODE in ('59400','59409','59410','59510','59514','59515','59610',
'59612','59614','59618','59620','59622')
and  cast(PROC_DATE as date) >= '2020-01-01'
;

--consolidate deliveries
select min(delivery_date) as deliv_date, ptid 
into stage.dbo.wc_covid_deliveries
from stage.dbo.wc_covid_deliveries_temp
group by ptid
;


----*********************************************************************
---outcomes
----*********************************************************************

---Acute myocardial infarction AMI
select  PTID, cast(DIAG_DATE as date) as outcome_dt, 'AMI' as outcome
into stage.dbo.wc_covid_outcomes_temp
from COVID.dbo.cov_20210128_diag a
where left(a.DIAGNOSIS_CD,3) between 'I21' and 'I22'
and cast(diag_date as date) >= '2020-01-01'
;


---Temporary tracheostomy
insert into stage.dbo.wc_covid_outcomes_temp
select ptid, cast(PROC_DATE as date) as outcome_dt, 'TT' as outcome
from COVID.dbo.cov_20210128_proc a 
where ( a.PROC_CODE like '0B110Z%' 
       or a.PROC_CODE like '0B110F%'
       or a.PROC_CODE like '0B113%'
       or a.PROC_CODE like '0B114%'
       )
and  cast(PROC_DATE as date) >= '2020-01-01'
;



---ventilation
insert into stage.dbo.wc_covid_outcomes_temp
select ptid, cast(PROC_DATE as date) as outcome_dt, 'VEN' as outcome
from COVID.dbo.cov_20210128_proc a 
where a.PROC_CODE in ('5A1935Z', '5A1945Z', '5A1955Z')
and  cast(PROC_DATE as date) >= '2020-01-01'
;




---Puerperal cerebrovascular disorders PCD
insert into stage.dbo.wc_covid_outcomes_temp
select  PTID, cast(DIAG_DATE as date) as outcome_dt, 'PCD' as outcome
from COVID.dbo.cov_20210128_diag a
where ( left(a.DIAGNOSIS_CD,3) between 'I60' and 'I68'
    or left(a.DIAGNOSIS_CD,5) between 'I9781' and 'I9782'
    or a.DIAGNOSIS_CD in ('O2251', 'O2252', 'O2253','O873')
    )
and cast(diag_date as date) >= '2020-01-01'
;

---consolidate outcomes
select ptid, outcome , min(outcome_dt) as outcome_date 
into stage.dbo.wc_covid_outcomes 
from stage.dbo.wc_covid_outcomes_temp
group by ptid, outcome; 


---emergency room visits
select distinct a.VISIT_TYPE 
from COVID.dbo.cov_20210128_vis a 

where a.VISIT_TYPE 


----outcomes file
select a.*, b.deliv_date, 
       c.outcome_date as AMI_date, 
       d.outcome_date as TT_date,
       e.outcome_date as VENT_date,
       f.outcome_date as PCD_date
       into STAGE.dbo.wc_covid_pregnancy_outcomes_extract
from STAGE.dbo.wc_cov_pregnant_ptids a
  left outer join stage.dbo.wc_covid_deliveries b 
    on a.ptid = b.ptid 
  left outer join stage.dbo.wc_covid_outcomes c 
    on a.ptid = c.ptid 
   and c.outcome = 'AMI'
    left outer join stage.dbo.wc_covid_outcomes d 
    on a.ptid = d.ptid 
   and d.outcome = 'TT'
    left outer join stage.dbo.wc_covid_outcomes e
    on a.ptid = e.ptid 
   and e.outcome = 'VEN'
    left outer join stage.dbo.wc_covid_outcomes f 
    on a.ptid = f.ptid 
   and f.outcome = 'PCD'
    ;

---validate
   select count(*), count(distinct ptid) 
   from STAGE.dbo.wc_covid_pregnancy_outcomes_extract