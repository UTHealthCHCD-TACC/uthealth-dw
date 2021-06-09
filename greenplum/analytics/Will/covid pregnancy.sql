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


drop table g823066.wc_preg_pregnant_mems;

---pregnant women
select  ptid  , min(diag_date) as preg_dt
into g823066.wc_preg_pregnant_mems
from opt_20210401.diag d 
where DIAGNOSIS_CD in ( select cd from g823066.wc_preg_codes )
 and cast(diag_date as date) >= '2020-01-01'
 group by ptid
;


---covid positive
drop table g823066.wc_preg_covid_pos_mems;


select ptid, min(result_date) as covid_first_date 
into g823066.wc_preg_covid_pos_mems
from g823066.covid_lab_test_results_20210401
where Interpret = 'Pos'
group by ptid 
;




drop table g823066.wc_preg_covid_neg_mems;

---covid negatives
select ptid, max(result_date) as covid_neg_date
into g823066.wc_preg_covid_neg_mems
from g823066.covid_lab_test_results_20210401
where Interpret = 'Neg'
group by ptid
;


----create extract table with visit info
drop table if exists g823066.wc_covid_pregnancy_study_extract

select a.ptid, a.race, a.ethnicity, a.birth_yr, 2020 - cast(a.BIRTH_YR as int) as age, a.date_of_death, preg_dt, 
       v.visit_type, cast(v.VISIT_START_DATE as date) as visit_start , v.ptid::text || v.VISIT_START_DATE::text as vis_id, 
       c.covid_first_date, d.covid_neg_date
      into g823066.wc_covid_pregnancy_study_extract
from opt_20210401.pt a 
   join g823066.wc_preg_pregnant_mems b 
      on a.ptid = b.ptid 
  left outer join  g823066.wc_preg_covid_pos_mems c 
      on a.ptid = c.ptid 
  left outer join  g823066.wc_preg_covid_neg_mems d 
      on a.ptid = d.ptid 
  left outer join opt_20210401.vis v 
	  on a.ptid  = v.PTID 
	 -- and cv.DISCHARGE_DISPOSITION not in ('INVALID VALUE','NOT RECORDED')
	  and cast(v.VISIT_START_DATE as date) >= '2020-01-01'
where  a.GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(a.BIRTH_YR as int) between 15 and 54
 order by a.ptid, cast(v.VISIT_START_DATE as date)
;


select count(*), count(distinct ptid) from g823066.wc_preg_pregnant_mems

select count(*), count(distinct a.ptid) 
from opt_20210401.pt a 
   join g823066.wc_preg_pregnant_mems b 
      on a.ptid = b.ptid 
where  a.GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(a.BIRTH_YR as int) between 15 and 54

----*********************************************************************
---NEW 4/28/21 Comorbid conditions
----*********************************************************************

drop table g823066.wc_preg_comorbid;


--placenta accreta spectrum
select distinct b.ptid, 'placentra accreta' as cond
into g823066.wc_preg_comorbid
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O43213','O43223','O43233')
;

--pulm hypert
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'pulm hypertension' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('I270','I272')
;

--chronic renal dis 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'chronic renal disease' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in 
('O2683', 'I12', 'I13', 'N03','N04','N05', 'N07', 'N08', 'N111', 'N118', 'N119', 'N18', 'N250', 'N251', 'N2581', 'N2589', 'N259', 'N269')
;


--bleeding disorder
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'bleed disorder' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('D66', 'D67', 'D680','D681','D682','D683','D684','D685','D686', 'D69')
;


--cardiac disease 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'cardiac disease' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('I05','I06','I07','I08','I09', 'I11','I12','I13', 'I15', 'I16', 'I20',
		'I25', 'I278', 'I5022', 'I5023', 'I5032', 'I5033', 'I5042', 'I5043', 'I50812', 'I50813', 'O9941', 'O9942', 
         'Q20','Q21','Q22','Q23','Q24','I30','I31','I32','I33','I34','I35','I36','I37','I38','I39','I40','I41',
         'I44','I45''I46','I47','I48','I49')
;


--hiv
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'hiv aids' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O987','B20')
;


--placenta previa 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'placenta previa' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O4403', 'O4413', 'O4423', 'O4433')
;


--preclampsia
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'preclamp' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O141', 'O142', 'O11')
;


--anemia
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'anemia' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O9901', 'O9902', 'D50', 'D55', 'D56', 'D58', 'D59', 'D571', 'D5720', 'D573', 'D5740', 'D5780')
;

--twins
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'twins' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O30', 'O31','Z372','Z373','Z374','Z375','Z376','Z377')
;


--placental abruption
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'placental abruption' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O45')
;

--preterm birth 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'preterm birth' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('Z3A20','Z3A21','Z3A22','Z3A23','Z3A24','Z3A25','Z3A36')
;

--GI disease 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'gi disease' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('K50','K51','K52', 'K70','K71','K72','K73','K74','K75','K76','K77', 'K80','K82','K83','K83', 
                       'K85','K86','K87', 'K94', 'K95', 'O266')
;


--preclamp unsevere
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'preclamp without' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O13', 'O140', 'O149')
;

--asthma
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'asthma' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O995', 'J4521', 'J4522', 'J4531', 'J4532', 'J454', 'J455', 'J45901', 'J45902')
;


--substance abuse
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'substance abuse' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('F10','F11','F12','F13','F14','F15','F16','F17','F18','F19', 'O9931', 'O9932')
;


--autoimmune
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'autoimmune' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('M30','M31','M32','M33','M34','M35','M36')
;

--hypertension
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'chronic hypertension' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O10','O11','I10')
;

--diabetes
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'diabetes' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('E08','E09','E10','E11','E12','E13', 'O240', 'O241', 'O243', 'O248', 'O249', 'Z794')
;


--neuromuscular 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'nueromuscular' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('G40','G70')
;


--major mental health
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'mental health' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('F06', 'F20','F21','F22','F23','F24','F25', 'F28','F29','F30','F31','F32','F33','F34', 'F39', 'F400', 'F41', 'F43', 'F53', 'F60')
;


--thyrotoxicosis
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'thyro' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('E05')
;

--bmi 40 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'bmi40' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('Z684')
;

--previous cesarean
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'prev cesarean' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O3421')
;


--gestational diabetes 
insert into g823066.wc_preg_comorbid
select distinct b.ptid, 'gest diabetes' as cond
from opt_20210401.diag a
   join g823066.wc_preg_pregnant_mems b
     on a.ptid = b.ptid 
    and a.diag_date between b.preg_dt - interval '12 months' and b.preg_dt 
where diagnosis_cd in ('O244')
;

---over 35
insert into g823066.wc_preg_comorbid
select distinct a.ptid, 'age35plus' as cond
from opt_20210401.pt a 
  join g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
     and 2020- case when length(birth_yr) > 4 then 1932 else a.birth_yr::int2 end >= 35
where length(birth_yr) = 4
;


select a.* 
into g823066.wc_preg_comorbid_extract
from g823066.wc_preg_comorbid a
  join opt_20210401.pt b
      on a.ptid = b.ptid 
where  b.GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54

------------
---- /end comorbid /
------------

----*********************************************************************
---outcomes
----*********************************************************************

----****
---deliveries 
drop table if exists g823066.wc_covid_deliveries_temp;

drop table if exists g823066.wc_covid_deliveries;

--deliveries diag
select  PTID, cast(DIAG_DATE as date) as delivery_date, 
        case when a.diagnosis_cd in ('10D00Z0','10D00Z1','10D00Z2','Z3801','Z3864','Z3866','Z3869') then 'C' 
        else 'D' end as deliv_type 
into g823066.wc_covid_deliveries_temp
from opt_20210401.diag a
where a.DIAGNOSIS_CD in ('10D00Z0','10D00Z1','10D00Z2','Z3801','Z3864','Z3866','Z3869','Z370','Z379','Z371','O321XX0',
'O322XX0','O323XX0','O329XX0','O364XX0','O6010X0','O6012X0','O6013X0','O6014X0','O641XX0','O642XX0','O643XX0','10D07Z3',
'10D07Z4','10D07Z5','10D07Z6','10D07Z7','10D07Z8','10E0XZZ','Z3800','Z381','Z382','Z3830','Z3831','Z384','Z385','Z3861',
'Z3862','Z3863','Z3865','Z3868','Z387','Z388')
and cast(diag_date as date) >= '2020-01-01'
;


---deliveries icd proc
insert into g823066.wc_covid_deliveries_temp
select ptid, cast(PROC_DATE as date), 
        case when a.proc_code in ('10D00Z0','10D00Z1','10D00Z2','Z3801','Z3864','Z3866','Z3869') then 'C' 
        else 'D' end as deliv_type 
from opt_20210401.proc a 
where a.PROC_CODE in ('10D00Z0','10D00Z1','10D00Z2','Z3801','Z3864','Z3866','Z3869','Z370','Z379','Z371','O321XX0',
'O322XX0','O323XX0','O329XX0','O364XX0','O6010X0','O6012X0','O6013X0','O6014X0','O641XX0','O642XX0','O643XX0','10D07Z3',
'10D07Z4','10D07Z5','10D07Z6','10D07Z7','10D07Z8','10E0XZZ','Z3800','Z381','Z382','Z3830','Z3831','Z384','Z385','Z3861',
'Z3862','Z3863','Z3865','Z3868','Z387','Z388')
and cast(PROC_DATE as date) >= '2020-01-01'
;


--delivieries drgs
insert into g823066.wc_covid_deliveries_temp
select ptid,  cast(a.VISIT_START_DATE as date), 
        case when a.drg in ('370','371','765','766','783','784','786','787','785','788') then 'C' 
        else 'D' end as deliv_type 
from opt_20210401.vis a
where a.DRG in ('370','371','372','373','374','375',
'765','766','767','768','774','775''796','797','798',
'805','806','807','783','784','786','787','785','788')
  and cast(a.VISIT_START_DATE as date) >= '2020-01-01'
 ;
 

---deliveries cpt/hcpc
insert into g823066.wc_covid_deliveries_temp
select ptid, cast(PROC_DATE as date), 
        case when a.PROC_CODE in ('59510','59514','59515','59610','59612','59614','59618','59620','59622') then 'C' 
        else 'D' end as deliv_type 
from opt_20210401.proc a 
where a.PROC_CODE in ('59400','59409','59410',
'59510','59514','59515','59610','59612','59614','59618','59620','59622')
and  cast(PROC_DATE as date) >= '2020-01-01'
;

--consolidate deliveries
select a.ptid, min(a.deliv_type) as deliv_type, a.delivery_date 
into g823066.wc_covid_deliveries
from g823066.wc_covid_deliveries_temp a 
  join 
(
select ptid, min(delivery_date) as delivery_date--count(*), count(distinct ptid) 
from g823066.wc_covid_deliveries_temp
group by ptid
) inr
 on inr.ptid = a.ptid 
 and inr.delivery_date = a.delivery_date
group by a.ptid, a.delivery_date 
;


---Acute myocardial infarction AMI
select  PTID, cast(DIAG_DATE as date) as outcome_dt, 'AMI' as outcome
into g823066.wc_covid_outcomes_temp
from opt_20210401.diag a
where left(a.DIAGNOSIS_CD,3) between 'I21' and 'I22'
and cast(diag_date as date) >= '2020-01-01'
;


---Temporary tracheostomy
insert into g823066.wc_covid_outcomes_temp
select ptid, cast(PROC_DATE as date) as outcome_dt, 'TT' as outcome
from opt_20210401.proc a
where ( a.PROC_CODE like '0B110Z%' 
       or a.PROC_CODE like '0B110F%'
       or a.PROC_CODE like '0B113%'
       or a.PROC_CODE like '0B114%'
       )
and  cast(PROC_DATE as date) >= '2020-01-01'
;



---ventilation
insert into g823066.wc_covid_outcomes_temp
select ptid, cast(PROC_DATE as date) as outcome_dt, 'VEN' as outcome
from opt_20210401.proc a
where a.PROC_CODE in ('5A1935Z', '5A1945Z', '5A1955Z')
and  cast(PROC_DATE as date) >= '2020-01-01'
;




---Puerperal cerebrovascular disorders PCD
insert into g823066.wc_covid_outcomes_temp
select  PTID, cast(DIAG_DATE as date) as outcome_dt, 'PCD' as outcome
from opt_20210401.diag a
where ( left(a.DIAGNOSIS_CD,3) between 'I60' and 'I68'
    or left(a.DIAGNOSIS_CD,5) between 'I9781' and 'I9782'
    or a.DIAGNOSIS_CD in ('O2251', 'O2252', 'O2253','O873')
    )
and cast(diag_date as date) >= '2020-01-01'
;

---consolidate outcomes
select ptid, outcome , min(outcome_dt) as outcome_date 
into g823066.wc_covid_outcomes
from g823066.wc_covid_outcomes_temp
group by ptid, outcome; 


drop table g823066.wc_covid_pregnancy_outcomes_extract;


----outcomes file
select a.*, b.delivery_date, b.deliv_type,
       c.outcome_date as AMI_date, 
       d.outcome_date as TT_date,
       e.outcome_date as VENT_date,
       f.outcome_date as PCD_date
      into g823066.wc_covid_pregnancy_outcomes_extract
from g823066.wc_preg_pregnant_mems a 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
  left outer join g823066.wc_covid_deliveries b 
    on a.ptid = b.ptid 
  left outer join g823066.wc_covid_outcomes c 
    on a.ptid = c.ptid 
   and c.outcome = 'AMI'
    left outer join g823066.wc_covid_outcomes d 
    on a.ptid = d.ptid 
   and d.outcome = 'TT'
    left outer join g823066.wc_covid_outcomes e
    on a.ptid = e.ptid 
   and e.outcome = 'VEN'
    left outer join g823066.wc_covid_outcomes f 
    on a.ptid = f.ptid 
   and f.outcome = 'PCD'
    ;

---validate
   select count(*), count(distinct ptid) 
   from g823066.wc_covid_pregnancy_outcomes_extract
   
   
---- new 5/26  full claims pull for 1/1/2020 and forward 
   
   ---carearea 
 select a.* 
 into g823066.wc_covid_preg_carearea_extract
 from opt_20210401.carearea a 
 join  g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
where a.carearea_date >= '2020-01-01'
   ;
   
  ---- diag 
  select a.* 
 into g823066.wc_covid_preg_diag_extract
 from opt_20210401.diag a 
 join  g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
where a.diag_date >= '2020-01-01'
;


  --- enc 
    select a.* 
 into g823066.wc_covid_preg_enc_extract
 from opt_20210401.enc a 
 join  g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
where a.interaction_date >= '2020-01-01'
;
  
  --- lab 
    select a.* 
 into g823066.wc_covid_preg_lab_extract
 from opt_20210401.lab a 
 join  g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
where a.collected_date >= '2020-01-01'
;
  
  --- obs 
    select a.* 
 into g823066.wc_covid_preg_obs_extract
 from opt_20210401.obs a 
 join  g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
where a.obs_date >= '2020-01-01'
;

  ---- rx presc 
   select a.* 
 into g823066.wc_covid_preg_rx_presc_extract
 from opt_20210401.rx_presc a 
 join  g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
where a.rxdate >= '2020-01-01'
; 
  
  --- vis 
    select a.* 
 into g823066.wc_covid_preg_vis_extract
 from opt_20210401.vis a 
 join  g823066.wc_preg_pregnant_mems b 
     on a.ptid = b.ptid 
  join opt_20210401.pt p
      on a.ptid = p.ptid 
   and GENDER = 'Female'
   and length(BIRTH_YR ) = 4
   and 2020 - cast(BIRTH_YR as int) between 15 and 54
where a.visit_start_date >= '2020-01-01'
;
  