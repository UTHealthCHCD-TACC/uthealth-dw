drop table stage.dbo.wc_cov20201210_inpatient_temp;

---get inpatient admits within 14 days of covid positive diagnosis
SELECT v.*,l.result_date
into stage.dbo.wc_cov20201210_inpatient_temp
from 
(SELECT DISTINCT  [PTID]
      ,concat(PTID,[VISIT_START_DATE],[VISIT_END_DATE]) VSID
      ,[VISIT_TYPE]
      ,[VISIT_START_DATE]  
      ,[VISIT_END_DATE]
      ,[DISCHARGE_DISPOSITION]    
  FROM [COVID].[dbo].[cov_20201210_vis] where [VISIT_TYPE] = 'inpatient'   AND year([VISIT_START_DATE]) = 2020) v,
(SELECt DISTINCT PTID,RESULT_DATE
 from covid.[dbo].[temp_lab_20201210_new] 
 where interpret ='Pos') l 
where (l.RESULT_DATE BETWEEN [VISIT_START_DATE] AND [VISIT_END_DATE]
OR datediff(day,RESULT_DATE,[VISIT_START_DATE]) between 0 and 14 ) 
  AND l.PTID=v.PTID 
;


select count(distinct a.PTID), case when Interpret = 'Pos' then 'Y' else 'N' end as resul, month(cast(RESULT_DATE as date)) as mon 
from covid.[dbo].[temp_lab_20201210_new] a 
  left outer join stage.dbo.wc_perla_temp b 
     on a.PTID = b.ptid 
  where b.ptid is null 
group by  month(cast(RESULT_DATE as date)) , case when Interpret = 'Pos' then 'Y' else 'N' end
order by  month(cast(RESULT_DATE as date)) , case when Interpret = 'Pos' then 'Y' else 'N' end
;


select count(distinct ptid) from stage.dbo.wc_cov20201210_chd_cohort;

select interpret, count(a.ptid), month(cast(a.RESULT_DATE as date) ) as mnth, 
       count(distinct a.ptid) as dist
from  covid.[dbo].[temp_lab_20201210_new] a
   join  stage.dbo.wc_cov20201210_chd_cohort b 
      on a.PTID = b.ptid 
 where cast(a.RESULT_DATE as date) >= '2020-01-01'
   and a.Interpret is not null 
group by interpret, month(cast(a.RESULT_DATE as date) ) 
order by month(cast(a.RESULT_DATE as date) ), a.Interpret ;


select ptid, visit_type, min(visit_start_date) as visit_start_date, min(visit_end_date) as visit_end_date
into  stage.dbo.wc_cov20201210_inpatient
from stage.dbo.wc_cov20201210_inpatient_temp
group by ptid, visit_type
;

select count(*), count(distinct ptid) , month(cast(visit_start_date as date) )
from stage.dbo.wc_cov20201210_inpatient
group by month(cast(visit_start_date as date) )
order by month(cast(visit_start_date as date) );
;


create table stage.dbo.wc_perla_ptids (patid varchar(25));


select count(*), count(distinct ptid) , month(cast(visit_start_date as date) )
from COVID.[dbo].[cov_20201210_pt]
group by month(cast(visit_start_date as date) )
order by month(cast(visit_start_date as date) );



select inr.ptid, inr.visit_type, inr.visit_start_date, inr.visit_end_date 
into stage.dbo.wc_cov20201210_chd_cohort
from ( 
select a.*, 
case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age
from stage.dbo.wc_cov20201210_inpatient a 
  join COVID.[dbo].[cov_20201210_pt] b 
     on a.ptid = b.ptid 
  left outer join stage.dbo.wc_perla_ptids c 
     on a.ptid = c.patid 
where c.patid is null 
) inr
where age >= 18
;

select count(*), count(distinct ptid) , month(cast(visit_start_date as date) )
from stage.dbo.wc_cov20201210_chd_cohort
   group by month(cast(visit_start_date as date) )
order by month(cast(visit_start_date as date) );  
     
---counts by age group and ethnicity                                   
select count(distinct ptid) as pat, case when race = 'African American' then 'Black' 
                                         when race = 'Asian' then 'Asian' 
                                         when race = 'Other/Unknown' then 'Race Other/Unknown & Ethnicity Unknown'
                                         when race = 'Caucasian' then      case when ETHNICITY = 'Hispanic' then 'Hispanic' 
                                                                                when ETHNICITY = 'Unknown' then 'White Ethnicity Unknown'
                                                                                when ETHNICITY = 'Not Hispanic' then 'Non-Hispanic White' end 
                                         end as race_eth                                                                                                                    
/*select count(distinct ptid) as pat,                                         
case when age between 18 and 49 then 1 
              when age between 50 and 59 then 2 
              when age between 60 and 69 then 3 
              when age >= 70 then 4 end as agegroup
              */
from (
SELECT p.PTID,DATE_OF_DEATH,
	   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age,
       substring(gender,1,1) Gen,
	   race,region,division,ethnicity 
from COVID.[dbo].[cov_20201210_pt] p 
   join stage.dbo.wc_cov20201210_chd_cohort v   
       on v.ptid = p.ptid  
) inr 
/*group by case when age between 18 and 49 then 1 
              when age between 50 and 59 then 2 
              when age between 60 and 69 then 3 
              when age >= 70 then 4 end 
; */
group by case when race = 'African American' then 'Black' 
                                         when race = 'Asian' then 'Asian' 
                                         when race = 'Other/Unknown' then 'Race Other/Unknown & Ethnicity Unknown'
                                         when race = 'Caucasian' then      case when ETHNICITY = 'Hispanic' then 'Hispanic' 
                                                                                when ETHNICITY = 'Unknown' then 'White Ethnicity Unknown'
                                                                                when ETHNICITY = 'Not Hispanic' then 'Non-Hispanic White' end 
                                         end
;


----by gender                              
select  --avg(cast(age as float)), stdev(age)  
count(distinct ptid), mnth  --gen -- mnth, region
from (
SELECT p.PTID,DATE_OF_DEATH,
	   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age,
       substring(gender,1,1) Gen,
	   race,region,division,ethnicity , 
	   month(cast(v.VISIT_START_DATE as date) ) as mnth
from COVID.[dbo].[cov_20201210_pt] p 
   join  stage.dbo.wc_cov20201210_chd_cohort v   
       on v.ptid = p.ptid  
) inr 
group by mnth --region -- gen 
--group by mnth,region
--order by region, mnth --gen 
;

---get insurance type  com > medicaid > medicare > other 
select INSURANCE_TYPE, ptid 
into stage.dbo.wc_covid_20201210_ins
from covid.dbo.cov_20201210_ins a  
where a.INSURANCE_DATE = ( select max(INSURANCE_DATE) from covid.dbo.cov_20201210_ins b where a.ptid = b.ptid );




select distinct insurance_type from stage.dbo.wc_covid_20201210_ins


drop table if exists stage.dbo.wc_covid_20201210_ins_final ;

select distinct ptid, insurance_type 
into stage.dbo.wc_covid_20201210_ins_final 
from stage.dbo.wc_covid_20201210_ins 
where insurance_type = 'Commercial' 
;

insert into stage.dbo.wc_covid_20201210_ins_final 
select distinct a.ptid, a. insurance_type 
from stage.dbo.wc_covid_20201210_ins a 
left outer join  stage.dbo.wc_covid_20201210_ins_final b  
    on b.ptid = a.ptid 
where b.ptid is null 
and a.insurance_type = 'Medicaid' 
;


insert into stage.dbo.wc_covid_20201210_ins_final 
select distinct a.ptid, a. insurance_type 
from stage.dbo.wc_covid_20201210_ins a 
left outer join  stage.dbo.wc_covid_20201210_ins_final b  
    on b.ptid = a.ptid 
where b.ptid is null 
and a.insurance_type = 'Medicare' 
;


insert into stage.dbo.wc_covid_20201210_ins_final 
select distinct a.ptid, a. insurance_type 
from stage.dbo.wc_covid_20201210_ins a 
left outer join  stage.dbo.wc_covid_20201210_ins_final b  
    on b.ptid = a.ptid 
where b.ptid is null 
and a.insurance_type = 'Uninsured' 
;

insert into stage.dbo.wc_covid_20201210_ins_final 
select distinct a.ptid, 'Other'
from stage.dbo.wc_covid_20201210_ins a 
left outer join  stage.dbo.wc_covid_20201210_ins_final b  
    on b.ptid = a.ptid 
where b.ptid is null 
;


select count(ptid), count(distinct ptid), insurance_type 
from stage.dbo.wc_covid_20201210_ins_final 
group by insurance_type
;

----by ins                              
select count(distinct ptid), insurance_type 
from (
SELECT p.PTID,DATE_OF_DEATH,
	   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age,
       substring(gender,1,1) Gen,
	   race,region,division,ethnicity, 
	   x.insurance_type
from COVID.[dbo].[cov_20201210_pt] p 
   join stage.dbo.wc_cov20201210_chd_cohort v   
       on v.ptid = p.ptid  
   left outer join stage.dbo.wc_covid_20201210_ins_final  x
       on x.ptid = p.ptid 
) inr 
group by insurance_type ;


----death                          
select count(distinct ptid)
from (
SELECT p.PTID,case when DATE_OF_DEATH = '' then null else datefromparts(left(DATE_OF_DEATH,4) ,substring(DATE_OF_DEATH,5,2) ,'01') end as deathdt,
	   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age,
       substring(gender,1,1) Gen,
	   race,region,division,ethnicity, 
	   cast(v.VISIT_START_DATE as date) as visit_start_date
from COVID.[dbo].[cov_20201210_pt] p 
   join stage.dbo.wc_cov20201210_chd_cohort v   
       on v.ptid = p.ptid    
) inr 
where deathdt is not null 
 --and month(deathdt) between month(visit_start_date) and month(visit_start_date) + 1 --during month 
 --and month(deathdt) between month(visit_start_date) and month(visit_start_date) + 2 --during month or next 
and month(deathdt) >= month(visit_start_date)  --any time after  
;




---smoking**************************************************
select a.ptid, OBS_TYPE, OBS_RESULT, cast(a.OBS_DATE as date) obs_date
into stage.dbo.wc_covid_smoking
from COVID.dbo.cov_20201210_obs a
where OBS_TYPE like '%SMOKE%' 
and cast(OBS_DATE as date) >= '2020-01-01'
;

---smoking 
select ptid, --count(distinct ptid), 
       case when OBS_RESULT = 'Never smoked' then 'Never'
            when OBS_RESULT in ('Not currently smoking', 'Previously smoked') then 'Former'
            when OBS_RESULT = 'Current smoker' then 'Current'
            when  OBS_RESULT in ('Unknown smoking status' ,'Other smoking status') then 'Missing' end
           into stage.dbo.wc_smoking_prior
from ( 
	select ptid, min(obs_result) as obs_result 
	from (
	SELECT p.PTID,case when DATE_OF_DEATH = '' then null else datefromparts(left(DATE_OF_DEATH,4) ,substring(DATE_OF_DEATH,5,2) ,'01') end as deathdt,
		   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
		   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
		   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	       END  age,
	       substring(gender,1,1) Gen,
		   race,region,division,ethnicity, 
		   cast(v.VISIT_START_DATE as date) as visit_start_date,
		   s.OBS_RESULT 
	from COVID.[dbo].[cov_20201210_pt] p 
	   join stage.dbo.wc_cov20201210_chd_cohort v   
	       on v.ptid = p.ptid 
	    join stage.dbo.wc_covid_smoking s 
	      on s.ptid = p.ptid 
	     and cast(s.OBS_DATE as date) between cast(v.VISIT_START_DATE as date) and cast(v.VISIT_END_DATE as date)
	) inr 
group by ptid 
) inrx
;


select s.ptid, s.obs_result, cast(s.obs_date as date) as obs_date 
into stage.dbo.wc_covid_smoking_work
from stage.dbo.wc_covid_smoking s  
  join stage.dbo.wc_cov20201210_chd_cohort v
     on v.ptid = s.ptid 
  ;

select inr2.ptid, obs_result 
into stage.dbo.wc_smoking_final_perla

select count(inr2.ptid), count(distinct inr2.ptid) ,case when OBS_RESULT = 'Never smoked' then 'Never'
            when OBS_RESULT in ('Not currently smoking', 'Previously smoked') then 'Former'
            when OBS_RESULT = 'Current smoker' then 'Current'
            when  OBS_RESULT in ('Unknown smoking status' ,'Other smoking status') then 'Missing' end
from
(
	select inr.ptid, inr.visit_start_date
	from ( 
		select v.ptid, min(obs_result) as obs_result, v.visit_start_date
		from stage.dbo.wc_cov20201210_chd_cohort v   
		   left outer join stage.dbo.wc_covid_smoking_work p
			       on v.ptid = p.ptid
			      and cast(p.OBS_DATE as date) between cast(v.VISIT_START_DATE as date) and cast(v.VISIT_END_DATE as date)
	    group by v.ptid, v.visit_start_date
	  ) inr 
	where ( OBS_RESULT in ('Unknown smoking status' ,'Other smoking status')
	       or obs_result is null )
	       
) inr2
left outer join stage.dbo.wc_covid_smoking_work s
		       on s.ptid = inr2.ptid
		      and cast(s.OBS_DATE as date) < cast(inr2.VISIT_START_DATE as date)    		      
group by       case when OBS_RESULT = 'Never smoked' then 'Never'
            when OBS_RESULT in ('Not currently smoking', 'Previously smoked') then 'Former'
            when OBS_RESULT = 'Current smoker' then 'Current'
            when  OBS_RESULT in ('Unknown smoking status' ,'Other smoking status') then 'Missing' end
;




---comorbid conditions***************************************************************************************
drop table if exists stage.dbo.wc_cov_comorbid;


----update 4/30/2021 include poa flag, if poa = 1 then comorbid, if poa = 0 then confounder
---double check icu admits vs vent support

--obese
select ptid, 'obese' as cond, min(cd.Diag_date) as cond_dt
into stage.dbo.wc_cov_comorbid_obese
from COVID.dbo.cov_20201210_diag cd 
where ( cd.DIAGNOSIS_CD like 'E66%' or DIAGNOSIS_CD = '2780' )
group by ptid
;



---chf
select ptid, 'chf' as cond, min(cd.Diag_date) as cond_dt
 into stage.dbo.wc_cov_comorbid 
from COVID.dbo.cov_20201210_diag cd 
where cd.DIAGNOSIS_CD like '428%' 
	or DIAGNOSIS_CD between 'I425' and 'I42999'
	or DIAGNOSIS_CD like 'I43%'
	or DIAGNOSIS_CD like 'I50%'
	or DIAGNOSIS_CD in ( '39891','40201','40211','40291','40401','40403','40411','40413','40491','40493',
						 'I099','I110','I130','I132','I255','I420','P290')
group by ptid
;


--mi
insert into stage.dbo.wc_cov_comorbid 
select ptid, 'mi' as cond, min(cd.Diag_date) as cond_dt
from COVID.dbo.cov_20201210_diag cd 
where ( cd.DIAGNOSIS_CD like '410%'
       or DIAGNOSIS_CD like '412%'
       or DIAGNOSIS_CD like 'I21%'
       or DIAGNOSIS_CD like 'I22%'
       or DIAGNOSIS_CD = 'I252'
		)
group by ptid
;


delete from stage.dbo.wc_cov_comorbid where cond = 'aa';

--aa
insert into stage.dbo.wc_cov_comorbid 
select ptid, 'aa' as cond, min(cd.Diag_date) as cond_dt
from COVID.dbo.cov_20201210_diag cd 
where ( DIAGNOSIS_CD between '4262' and '42653'
       or DIAGNOSIS_CD between '4266' and '42689'
       or DIAGNOSIS_CD between 'I441' and 'I44399'
       or DIAGNOSIS_CD between 'I47' and 'I49999'
       or DIAGNOSIS_CD in ('42610','42611','42613','4270','4272','42731','42760','4279','7850','V450',
							'V533','I456','I459','T821','Z450','Z950')	
							--,'R000','R001','R008'
       )
group by ptid
;


--rf
insert into stage.dbo.wc_cov_comorbid 
select ptid, 'rf' as cond, min(cd.Diag_date) as cond_dt
from COVID.dbo.cov_20201210_diag cd 
where cd.DIAGNOSIS_CD in ('51881','51882','51883','51884','J9600','J9601','J9602','J9610','J9611','J9612','J9620','J9621','J9622','J9690','J9691','J9692')
group by ptid
;




--stroke
insert into stage.dbo.wc_cov_comorbid 
select ptid, 'str' as cond, min(cd.Diag_date) as cond_dt
from COVID.dbo.cov_20201210_diag cd 
where ( cd.DIAGNOSIS_CD between '430' and '43899'
       or DIAGNOSIS_CD between 'G45' and 'G46999'
     or cd.DIAGNOSIS_CD between 'I60' and 'I69999'
     or DIAGNOSIS_CD = 'H340'
     )
group by ptid
;


--aph
insert into stage.dbo.wc_cov_comorbid 
select ptid, 'aph' as cond, min(cd.Diag_date) as cond_dt
from COVID.dbo.cov_20201210_diag cd 
where cd.DIAGNOSIS_CD in ( '4160','4168','4169','I270','I272','I2720','I2721','I2722','I2723','I2724','I2729')
group by ptid
;


--vt
insert into stage.dbo.wc_cov_comorbid 
select ptid, 'vt' as cond, min(cd.Diag_date) as cond_dt
from COVID.dbo.cov_20201210_diag cd 
where cd.DIAGNOSIS_CD in ('4150','67300','452 ','4530','4510','67100','0882','I269','I260','I801','I828','I829','O223 ','I822',
						  'I820','I802','I81','O082','I823','O871','I809','I821','I808','I803')
group by ptid
;


---- rerun this for occuring during hospitalization 
--same for obesity

----comorb         
select count(distinct ptid), cond
from (
SELECT p.PTID,case when DATE_OF_DEATH = '' then null else datefromparts(left(DATE_OF_DEATH,4) ,substring(DATE_OF_DEATH,5,2) ,'01') end as deathdt,
	   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age,
       substring(gender,1,1) Gen,
	   race,region,division,ethnicity, 
	   cast(v.VISIT_START_DATE as date) as visit_start_date,
	   c.cond
from COVID.[dbo].[cov_20201210_pt] p 
   join stage.dbo.wc_cov20201210_chd_cohort v   
       on v.ptid = p.ptid    
   /*join stage.dbo.wc_cov_comorbid c  
     on c.ptid = p.ptid 
    and cast(c.cond_dt as date) <= cast(v.VISIT_END_DATE as date) 
    */
   -- obese only
    join stage.dbo.wc_cov_comorbid_obese c  
     on c.ptid = p.ptid 
     and cast(c.cond_dt as date) between ( dateadd(year,-1,cast(v.VISIT_START_DATE as date) ) ) and cast(v.VISIT_END_DATE as date) 
) inr 
group by cond
 ;

--chf, obese, mi, aa, rf, st, aph, vt

select distinct cond from stage.dbo.wc_cov_comorbid 



---vent support
drop table stage.dbo.wc_cov_vent;

select distinct ptid, cast(PROC_date as date) as prc_dt
into stage.dbo.wc_cov_vent
from covid.dbo.cov_20201210_proc 
where PROC_DESC
in ('VENT MGMT ADULT SUBQ', 'VENT MGMT HOSP/OBV INIT', 'VENT SETTINGS', 
'VENTILATOR 1ST DAY', 'VENTILATOR CHECK', 'VENTILATOR FLOWSHEET (DISCRETE)', 'VENTILATOR SUBSEQUENT DAY') 
;


insert into stage.dbo.wc_cov_vent
select distinct p.ptid, p.PROC_DATE 
from covid.dbo.cov_20201210_proc p 
where (  p.PROC_CODE like '5A093%'
       or  p.PROC_CODE like '5A094%'
       or  p.PROC_CODE like '5A095%'
       or p.PROC_CODE in ('5A1945Z','5A1935Z','5A1955Z')
       or p.PROC_CODE in ('94002','94003','31500','0410')
      );

     
--vent
select count(distinct ptid)
from (
SELECT p.PTID,case when DATE_OF_DEATH = '' then null else datefromparts(left(DATE_OF_DEATH,4) ,substring(DATE_OF_DEATH,5,2) ,'01') end as deathdt,
	   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age,
       substring(gender,1,1) Gen,
	   race,region,division,ethnicity, 
	   cast(v.VISIT_START_DATE as date) as visit_start_date
from COVID.[dbo].[cov_20201210_pt] p 
   join stage.dbo.wc_cov20201210_chd_cohort v   
       on v.ptid = p.ptid    
   join  stage.dbo.wc_cov_vent x 
     on x.ptid = p.ptid 
    and x.prc_dt between cast(v.VISIT_START_DATE as date) and cast(v.VISIT_END_DATE as date)
) inr 
;

create table stage.dbo.wc_perla_ptids_vis (patid varchar(25), visit_start date, visit_end date);

--- perla vent counts
select count(distinct patid) 
from ( 
  select patid 
  from  stage.dbo.wc_perla_ptids_vis a 
     join stage.dbo.wc_cov_vent b 
        on a.patid = b.ptid 
       and b.prc_dt between a.visit_start and a.visit_end 
) inr;

--- 
select count(distinct patid) 
from ( 
  select patid 
  from  stage.dbo.wc_perla_ptids_vis a 
     join stage.dbo.wc_covid_care_icu b 
        on a.patid = b.ptid 
       and b.care_date between a.visit_start and a.visit_end 
) inr;







--avg los
select avg(los) 
from ( 
select  cast( datediff(day,cast(v.VISIT_START_DATE as date),cast(v.VISIT_END_DATE as date))  as float  )as los
from  stage.dbo.wc_cov20201210_chd_cohort v
) X;


--los extract for analysis  
drop table  stage.dbo.wc_perla_los_extract;

select v.*, b.GENDER,	   case when ltrim(rtrim(date_of_death)) ='' THEN 2020 - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 
	   then cast(substring(BIRTH_YR,1,4) as int) else NULL end
	   ELSE cast(substring(date_of_death,1,4) as int) - case when isnumeric( substring(BIRTH_YR,1,4) ) = 1 then cast(substring(BIRTH_YR,1,4) as int) else NULL end
       END  age,
       cast( datediff(day,cast(v.VISIT_START_DATE as date),cast(v.VISIT_END_DATE as date))  as float  )as los
into stage.dbo.wc_perla_los_extract
from  stage.dbo.wc_cov20201210_chd_cohort v
  join COVID.[dbo].[cov_20201210_pt] b 
    on b.ptid = v.ptid 

 
--icu
select distinct ptid, cast(a.CAREAREA_DATE as date) as care_date
into stage.dbo.wc_covid_care_icu
from covid.dbo.cov_20201210_carearea a 
where a.CAREAREA = 'CRITICAL CARE UNIT (CCU) / INTENSIVE CARE UNIT (ICU)';

drop table stage.dbo.wc_covid_care_icu


select *
from covid.dbo.cov_20201210_carearea 
where carearea like '%ICU%'


--icu
select count(distinct patid)
from (
SELECT v.patid
from   stage.dbo.wc_perla_ptids v -- stage.dbo.wc_cov20201210_chd_cohort v        
   join stage.dbo.wc_covid_care_icu x 
      on x.ptid = v.patid 
    -- and x.care_date between cast(v.VISIT_START_DATE as date) and cast(v.VISIT_END_DATE as date)
) inr 
;




----- new code 6/29/2021   get distribution of CHD overall 

select distinct diagnosis_cd 
from covid.dbo.cov_20201210_diag 
where diagnosis_cd like 'Q20%';



select d.ptid,DIAGNOSIS_CD
into stage.dbo.wc_perla_temp
from covid.dbo.cov_20201210_diag d 
where d.DIAGNOSIS_CD in ('Q200','Q201','Q203','Q205','Q210','Q211','Q212','Q213','Q220',
						 'Q221','Q224','Q225','Q230','Q234','Q250','Q251','Q262')
group by ptid, DIAGNOSIS_CD


select count(*), count(distinct ptid), diagnosis_cd 
from stage.dbo.wc_perla_temp
group by diagnosis_cd;



