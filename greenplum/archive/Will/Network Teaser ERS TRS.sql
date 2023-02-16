--Member Counts Overall 
with cohort_cte as ( 
					select 'TRS' as data_source, combo_id , case when gen = 'F' then 'AAF' else 'AAM' end as gender_cd, enrlmnth, plnm as plan_type,
					case when age between 0 and 19 then 'Age Group 1'
							    when age between 20 and 34 then 'Age Group 2' 
								when age between 35 and 44 then 'Age Group 3'
								when age between 45 and 54 then 'Age Group 4'
								when age between 55 and 64 then 'Age Group 5'
								when age between 65 and 74 then 'Age Group 6'
								when age >= 75 then 'Age Group 7' end as age_group 
					from  TRSERS.dbo.TRS_AGG_CY 
					where CY = 2019					  
					union 
					select 'ERS' as data_source, ID,case when gen = 'F' then 'AAF' else 'AAM' end as gender_cd, enrlmnth, 'OTH' as plnm,
					case when age between 0 and 19 then 'Age Group 1'
							    when age between 20 and 34 then 'Age Group 2' 
								when age between 35 and 44 then 'Age Group 3'
								when age between 45 and 54 then 'Age Group 4'
								when age between 55 and 64 then 'Age Group 5'
								when age between 65 and 74 then 'Age Group 6'
								when age >= 75 then 'Age Group 7' end as age_group
					from TRSERS.dbo.ERS_AGG_CY 
					where CY = 2019
                  )
select * 
from 
( 
	select data_source, 'AAATotal' as measure, plan_type, count(distinct combo_id) as mem_count, sum(enrlmnth) / 12 as my 
	from cohort_cte group by data_source, plan_type 
union 
	select data_source, gender_cd, plan_type, count(distinct combo_id), sum(enrlmnth) / 12 as my 
	from cohort_cte group by data_source, gender_cd	, plan_type
union 
	select data_source, age_group , plan_type, count(distinct combo_id), sum(enrlmnth) / 12 as my 
	from cohort_cte group by data_source, age_group	, plan_type
) inr 
order by  data_source,measure;



select * from TRSERS.dbo.TRS_CLM_FIN_NEW  ;

select * from TRSERS.dbo.ERS_BCBSMedCLM;