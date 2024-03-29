---dell med school
--optum zip 2018 12 months CE 
--national age 21-64
drop table dev.wc_dell_cohort_diags;


select uth_member_id, 
       case when diag_cd in ('F3110','F3111','F3112','F3113','F312','F3130','F3131','F3132','F314','F315','F3160','F3161','F3162',
                             'F3163','F3164','F3170','F3171','F3172','F3173','F3174','F3175','F3176','F3177','F3178','F3181') then 'bipolar'
            when diag_cd in ('F330','F331','F332','F333','F3340','F3341','F3342','F338','F339') then 'depression'
       end as cnd
  into dev.wc_dell_cohort_diags
from data_warehouse.claim_diag 
where extract(year from from_date_of_service) = 2018
and data_source = 'optd'
and diag_cd 
in ('F3110','F3111','F3112','F3113','F312','F3130','F3131','F3132','F314','F315','F3160','F3161','F3162',
    'F3163','F3164','F3170','F3171','F3172','F3173','F3174','F3175','F3176','F3177','F3178','F3181',
    'F330','F331','F332','F333','F3340','F3341','F3342','F338','F339')
;





select count(*), count(distinct uth_member_id), cnd 
from dev.wc_dell_cohort_diags 
group by cnd ;



---
drop table dev.wc_dell_med_study_table

--demographics
select a.year, a.uth_member_id , a.age_derived , a.gender_cd, a.state, null as race, a.bus_cd, 0 as depression, 0 as bipolar,
      0 as hypertension, 0 as diabetes_uncomplicated, 0 as diabetes_complicated, 0 as peripheral_vascular_disorder,
      0 as obesity, 0 as fluid_conditions, 0 as congestive_heart_failure, 0 as weight_loss, 0 as cardiac_arrhythmia,
      0 as valvular_disease, 0 as pulmonary_circulation_disorder, 0 as chronic_pulmonary_disease, 0 as tumor_without_metastasis, 
      0 as metastatic_cancer
into dev.wc_dell_med_study_table
from data_warehouse.member_enrollment_yearly a
where data_source = 'optd'
  and age_derived between 21 and 64 
  and total_enrolled_months = 12 
  and a.year = 2018
  and uth_member_id in ( select uth_member_id from dev.wc_dell_cohort_diags)
;



update dev.wc_dell_med_study_table a set depression = 1 
where exists ( select 1 from dev.wc_dell_cohort_diags b where b.uth_member_id = a.uth_member_id and b.cnd = 'depression')
;


update dev.wc_dell_med_study_table a set bipolar = 1 
where exists ( select 1 from dev.wc_dell_cohort_diags b where b.uth_member_id = a.uth_member_id and b.cnd = 'bipolar')
;

--validate
select count(*), count(distinct uth_member_id) 
from dev.wc_dell_med_study_table


---comorbidities
drop table dev.wc_dell_med_comorbidities;

 --hypertension
 select a.uth_member_id, 'hypertension' as comorb
 into dev.wc_dell_med_comorbidities
 from data_warehouse.claim_diag a
where data_source = 'optd' and extract(year from from_date_of_service) = 2018
 and ( a.diag_cd like 'I10%'
 		 or a.diag_cd like 'I11%'
 		 or a.diag_cd like 'I12%'
 		 or a.diag_cd like 'I13%'
  		 or a.diag_cd like 'I15%'
  		)
;


--diabetes, uncomplicated
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'diabetesU' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and extract(year from from_date_of_service) = 2018
 and  a.diag_cd in ('E100','E101','E109','E110','E111','E119','E120','E121','E129','E130','E131','E139','E140','E141','E149')
;

---diabetes, complicated
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'diabetesC' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and extract(year from from_date_of_service) = 2018
 and  ( a.diag_cd between 'E102' and 'E108'
		or a.diag_cd between 'E112' and 'E118'
		or a.diag_cd between 'E122' and 'E128'
		or a.diag_cd between 'E132' and 'E138'
		or a.diag_cd between 'E142' and 'E148'
);
	

--periph vascular disorders
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'PVD' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and extract(year from from_date_of_service) = 2018
 and (  a.diag_cd in	('I731','I738','I739','I771','I790','I792','K551','K558','K559','Z958','Z959') 
   or a.diag_cd like 'I70%'
   or a.diag_cd like 'I71%'
);	
	
--obesity
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'obesity' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and extract(year from from_date_of_service) = 2018
 and  a.diag_cd like 'E66%';

--fluid/electrolyte
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'fluid' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd = 'E222'
   or a.diag_cd like 'E86%'
   or a.diag_cd like 'E87%'
);
   
--CHF   
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'CHF' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd in ('I099', 'I110', 'I130', 'I132', 'I255', 'I420', 'P290')  
		or a.diag_cd between 'I425' and 'I429'
		or a.diag_cd like 'I43%'
		or a.diag_cd like 'I50%'
);
   
--weight loss
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'weight loss' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd in ('R634','R64')
   or a.diag_cd like 'E40%'
   or a.diag_cd like 'E41%'
   or a.diag_cd like 'E42%'
   or a.diag_cd like 'E43%'
   or a.diag_cd like 'E44%'
   or a.diag_cd like 'E45%'
   or a.diag_cd like 'E46%'
);

--cardiac arrhythmias
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'arrhythmia' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and  ( a.diag_cd between 'I441' and 'I443'
		or a.diag_cd in ('I456','I459', 'R000', 'R001', 'R008', 'T821', 'Z450', 'Z950')
		or a.diag_cd like 'I47%'
		or a.diag_cd like 'I48%'
		or a.diag_cd like 'I49%'
	);


--valvular disease
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'valvular' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and  ( a.diag_cd between 'Z952' and 'Z954'
       or a.diag_cd between 'Q230' and 'Q233'
       or a.diag_cd in ('A520','I091','I098')
       or a.diag_cd like 'I05%'
       or a.diag_cd like 'I06%'
       or a.diag_cd like 'I07%'
       or a.diag_cd like 'I08%'
       or a.diag_cd like 'I34%'
       or a.diag_cd like 'I35%'
       or a.diag_cd like 'I36%'
       or a.diag_cd like 'I37%'
       or a.diag_cd like 'I38%'
       or a.diag_cd like 'I39%'
);


--pulmonary circulation disorders
insert into dev.wc_dell_med_comorbidities
select  a.uth_member_id, 'PCD' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd in ('I280', 'I288', 'I289')
		or a.diag_cd like 'I26%'
		or a.diag_cd like 'I27%'
      );


--chronic pulmonary disease
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'chronic pulmonary' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd in ('I27.8', 'I27.9','J68.4', 'J70.1', 'J70.3')
   or substring(diag_cd,1,3) between 'J40' and 'J47'
   or substring(diag_cd,1,3) between 'J60' and 'J67'
 );


--solid tumor w/out metastasis
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'tumor' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( substring(diag_cd,1,3) between 'C00' and 'C26'
or substring(diag_cd,1,3) between 'C30' and 'C34'
or substring(diag_cd,1,3) between 'C37' and 'C41' 
or substring(diag_cd,1,3) between 'C45' and 'C58'
or substring(diag_cd,1,3) between 'C60' and 'C76'
or substring(diag_cd,1,3) = 'C97'
or substring(diag_cd,1,3) = 'C43'
);

--metastatic cancer
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'meta' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and  substring(diag_cd,1,3) between 'C77' and 'C80'
;


-----***************** New Comorbidities 3/10/2022

--hypothyroidism  
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'hypothyroidism' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and (a.diag_cd in ('E890') 
     or a.diag_cd like 'E00% '
	or a.diag_cd like 'E01%'
	or a.diag_cd like 'E02%'
	or a.diag_cd like 'E03%'
 )
;


--renalfailure
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'renalfailure' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and (a.diag_cd in ('N250','Z490','Z491','Z492','Z940','Z992','I120','I131') 
     or a.diag_cd like 'N18%'
     or a.diag_cd like 'N19%'
 )
;


--liverdisease
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'liverdisease' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and (a.diag_cd in ('I864','I982','K711','K713','K714','K715','K717','K760','K762','K763','K764','K765','K766','K767','K768','K769','Z944') 
    or a.diag_cd like 'B18%'
	or a.diag_cd like 'I85%'
	or a.diag_cd like 'K70%'
	or a.diag_cd like 'K72%'
	or a.diag_cd like 'K73%'
	or a.diag_cd like 'K74%'
 )
;

--pepticulcers
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'pepticulcers' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and (a.diag_cd in ('K257','K259','K267','K269','K277','K279','K287','K289' ) )
;


--HIVAIDS
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'HIVAIDS' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd like 'B20%'
	or a.diag_cd like 'B21%'
	or a.diag_cd like 'B22%'
	or a.diag_cd like 'B24%'
 )
;


--lymphoma
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'lymphoma' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and (a.diag_cd in ('C900','C902') 
     or a.diag_cd like 'C81%'
	or a.diag_cd like 'C82%'
	or a.diag_cd like 'C83%'
	or a.diag_cd like 'C84%'
	or a.diag_cd like 'C85%'
	or a.diag_cd like 'C88%'
	or a.diag_cd like 'C96%'
 )
;


--rheumatoid arthritis RA
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'RA' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and (a.diag_cd in ('M469','M468','M461','M313','M312','M311','M310','M123','M120','L943','L941','L940') 
     or a.diag_cd like 'M45%'
	or a.diag_cd like 'M35%'
	or a.diag_cd like 'M34%'
	or a.diag_cd like 'M33%'
	or a.diag_cd like 'M32%'
	or a.diag_cd like 'M30%'
	or a.diag_cd like 'M08%'
	or a.diag_cd like 'M06%'
	or a.diag_cd like 'M05%'
 )
;

--coagulopathy
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'coagulopathy' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and (a.diag_cd in ('D65','D66','D67','D691','D693','D694','D695','D696') 
     or a.diag_cd like 'D68%'
 )
;


--bloodlossanemia
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'bloodlossanemia' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd = 'D500'
 )
;


--deficiencyanemia
insert into dev.wc_dell_med_comorbidities
select a.uth_member_id, 'deficiencyanemia' as comorb
 from data_warehouse.claim_diag a
where data_source = 'optd' and  extract(year from from_date_of_service) = 2018
 and ( a.diag_cd in ('D508','D509') 
     or a.diag_cd like 'D51%'
     or a.diag_cd like 'D52%'
     or a.diag_cd like 'D53%'
 )
;

--- End New Comorbidities 3/10/2022 ***********

   
analyze dev.wc_dell_med_comorbidities;

select count(*), comorb 
from dev.wc_dell_med_comorbidities  
group by comorb ;



---exclusions 
drop table dev.wc_dell_med_exclusions;

--new exclusions added 2/1/2022
insert  into dev.wc_dell_med_exclusions
select distinct a.uth_member_id ,'diag' as exclusion_rsn
from data_warehouse.claim_diag a 
   join dev.wc_dellmed_new_exclusion_cds b 
      on b.cd_value = a.diag_cd 
where data_source = 'optd' 
   and extract(year from from_date_of_service) = 2018 
;   
   
select distinct a.uth_member_id ,'diag' as exclusion_rsn
into dev.wc_dell_med_exclusions
from data_warehouse.claim_diag a 
where data_source = 'optd' 
and extract(year from from_date_of_service) = 2018 
   and ( substring(a.diag_cd,1,3) between 'G10' and 'G13'
   or substring(a.diag_cd,1,3) between 'G20' and 'G22'
   or substring(a.diag_cd,1,3) between 'G35' and 'G37'
   or substring(a.diag_cd,1,4) in ('G254', 'G255','G312', 'G318', 'G319','G931','G394','R470')
   or substring(a.diag_cd,1,3) in ('G32','G40', 'G41', 'R56')
   )
;



--place of service exclusions
insert into  dev.wc_dell_med_exclusions
select distinct uth_member_id, 'pos' as exclusion_rsn
from optum_zip.confinement c 
  join data_warehouse.dim_uth_member_id b 
    on b.member_id_src = c.patid::text 
where c.year = 2018 
 and ( 
      ( pos in ('21','27','28','31','32','35','65') and c.los >= 180 ) 
     or pos in ('34','54') 
     );

select count(distinct uth_member_id) from dev.wc_dell_med_exclusions; _test;
    
     
--overall table extract (THIS IS THE FINAL TABLE TO BE CONVERTED TO SAS)
drop table if exists dev.wc_dellmed_overall;

select u.member_id_src, a.uth_member_id, a.gender_cd, a.age_derived, b.rc, 0 as depression, 0 as bipolar,
      0 as hypertension, 0 as diabetes_uncomplicated, 0 as diabetes_complicated, 0 as peripheral_vascular_disorder,
      0 as obesity, 0 as fluid_conditions, 0 as congestive_heart_failure, 0 as weight_loss, 0 as cardiac_arrhythmia,
      0 as valvular_disease, 0 as pulmonary_circulation_disorder, 0 as chronic_pulmonary_disease, 0 as tumor_without_metastasis, 
      0 as metastatic_cancer,
      ---new comorbs added 3/10/2022
      0 as HIVAIDS, 0 as RA, 0 as bloodlossanemia, 0 as coagulopathy, 0 as deficiencyanemia,
      0 as hypothyroidism, 0 as liverdisease, 0 as lymphoma, 0 as pepticulcers, 0 as renalfailure
into dev.wc_dellmed_overall
from data_warehouse.member_enrollment_yearly a
  join dev.wc_dm_race_temp b 
    on a.uth_member_id = b.uth_member_id 
  join data_warehouse.dim_uth_member_id u 
    on u.uth_member_id = a.uth_member_id
where a.data_source = 'optd'
  and age_derived between 21 and 64 
  and total_enrolled_months = 12 
  and a.year = 2018
  and a.uth_member_id not in ( select uth_member_id from dev.wc_dell_med_exclusions )
  ;

 
update dev.wc_dellmed_overall a set depression = 1 
where exists ( select 1 from dev.wc_dell_cohort_diags b where b.uth_member_id = a.uth_member_id and b.cnd = 'depression')
;


update dev.wc_dellmed_overall a set bipolar = 1 
where exists ( select 1 from dev.wc_dell_cohort_diags b where b.uth_member_id = a.uth_member_id and b.cnd = 'bipolar')
;
 
 
update dev.wc_dellmed_overall a set hypertension = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'hypertension' )
;

update dev.wc_dellmed_overall a set diabetes_uncomplicated = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'diabetesU' )
;

update dev.wc_dellmed_overall a set diabetes_complicated = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'diabetesC' )
;

update dev.wc_dellmed_overall a set peripheral_vascular_disorder = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'PVD' )
;

update dev.wc_dellmed_overall a set obesity = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'obesity' )
;

update dev.wc_dellmed_overall a set fluid_conditions = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'fluid' )
;

update dev.wc_dellmed_overall a set congestive_heart_failure = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'CHF' )
;

update dev.wc_dellmed_overall a set weight_loss = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'weight loss' )
;

update dev.wc_dellmed_overall a set cardiac_arrhythmia = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'arrhythmia' )
;

update dev.wc_dellmed_overall a set valvular_disease = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'valvular' )
;

update dev.wc_dellmed_overall a set pulmonary_circulation_disorder = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'PCD' )
;

update dev.wc_dellmed_overall a set chronic_pulmonary_disease = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'chronic pulmonary' )
;

update dev.wc_dellmed_overall a set tumor_without_metastasis = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'tumor' )
;

update dev.wc_dellmed_overall a set metastatic_cancer = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'meta' )
;

---new comorbds 3/11/2022
 
update dev.wc_dellmed_overall a set HIVAIDS = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'HIVAIDS' )
;

update dev.wc_dellmed_overall a set RA = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'RA' )
;

update dev.wc_dellmed_overall a set bloodlossanemia = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'bloodlossanemia' )
;

update dev.wc_dellmed_overall a set coagulopathy = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'coagulopathy' )
;

update dev.wc_dellmed_overall a set deficiencyanemia = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'deficiencyanemia' )
;

update dev.wc_dellmed_overall a set hypothyroidism = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'hypothyroidism' )
;

update dev.wc_dellmed_overall a set liverdisease = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'liverdisease' )
;

update dev.wc_dellmed_overall a set lymphoma = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'lymphoma' )
;

update dev.wc_dellmed_overall a set pepticulcers = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'pepticulcers' )
;

update dev.wc_dellmed_overall a set renalfailure = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'renalfailure' )
;

----
      
      
select count(uth_member_id) 
from (
	select uth_member_id , 
	       sum(a.cardiac_arrhythmia + a.chronic_pulmonary_disease + a.congestive_heart_failure + a.diabetes_complicated + 
	           a.diabetes_uncomplicated + a.fluid_conditions + a.hypertension + a.metastatic_cancer + a.obesity + 
	           a.peripheral_vascular_disorder + a.pulmonary_circulation_disorder + a.tumor_without_metastasis + a.valvular_disease 
	           + a.weight_loss ) as cond_count
	from dev.wc_dellmed_overall a
	group by uth_member_id
) x 
where cond_count >0; 
 
 
select * 
from dev.wc_dellmed_overall  ;



---------------------------------------------------------------------------------------------------------


-------------- OLD CODE BELOW 

 
----------------------------------------------------------------------------

---(DEPRECATED) table for extract
drop table if exists dev.wc_dell_med_study_table_extract;

select b.member_id_src, a.*
into dev.wc_dell_med_study_table_extract
from dev.wc_dell_med_study_table a 
   join data_warehouse.dim_uth_member_id b 
     on a.uth_member_id = b.uth_member_id 
where a.uth_member_id not in ( select uth_member_id from dev.wc_dell_med_exclusions )
;




update dev.wc_dell_med_study_table a set hypertension = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'hypertension' )
;

update dev.wc_dell_med_study_table a set diabetes_uncomplicated = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'diabetesU' )
;

update dev.wc_dell_med_study_table a set diabetes_complicated = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'diabetesC' )
;

update dev.wc_dell_med_study_table a set peripheral_vascular_disorder = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'PVD' )
;

update dev.wc_dell_med_study_table a set obesity = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'obesity' )
;

update dev.wc_dell_med_study_table a set fluid_conditions = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'fluid' )
;

update dev.wc_dell_med_study_table a set congestive_heart_failure = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'CHF' )
;

update dev.wc_dell_med_study_table a set weight_loss = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'weight loss' )
;

update dev.wc_dell_med_study_table a set cardiac_arrhythmia = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'arrhythmia' )
;

update dev.wc_dell_med_study_table a set valvular_disease = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'valvular' )
;

update dev.wc_dell_med_study_table a set pulmonary_circulation_disorder = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'PCD' )
;

update dev.wc_dell_med_study_table a set chronic_pulmonary_disease = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'chronic pulmonary' )
;

update dev.wc_dell_med_study_table a set tumor_without_metastasis = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'tumor' )
;

update dev.wc_dell_med_study_table a set metastatic_cancer = 1 
where exists ( select 1 from dev.wc_dell_med_comorbidities b where b.uth_member_id = a.uth_member_id and comorb = 'meta' )
;


---validate before extract
select count(*), count(distinct uth_member_id)
from dev.wc_dell_med_study_table a 


select * from dev.wc_dell_med_study_table

