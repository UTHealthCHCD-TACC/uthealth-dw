--cost and details
drop table if exists dev.wc_tease_opioid_details;
 select uth_member_id , p.ndc, p.total_charge_amount, p.total_allowed_amount, p.total_paid_amount , p.uth_script_id , p.script_id, p.days_supply 
   into dev.wc_tease_opioid_details
 from data_warehouse.pharmacy_claims p
   join dev.wc_tease_ndc n 
     on p.ndc like '%' || n.ndc || '%' 
 where data_source in ('optz','mcrt','mdcd','truv')
   and p."year"  = 2019 
 ;

---
drop table wrk.dbo.wc_trsers_opiod;

select ID, 
		productid as ndc, 
		cast(DaysSupply as int) as DaysSupply, 
		cast(UsualCustomaryBillingAmount as float) as ChargeAmount, 
		cast(TotalAmountBilled as float) as AllowedAmount, 
		cast(PatientPayAmount as float) as PaidAmount, 
		PrescriberSpecialtyCode ,   
		PrescriptionReferenceNumber as script_id,
	    cast(FirstDateofService as date) as fill_date
into wrk.dbo.wc_trsers_opiod
from TRSERS.dbo.ERS_UHC_RX a 
   join wrk.dbo.temp_wc_ndc b 
     on a.ProductID like '%' + b.ndc + '%'
where substring(yrmnth,1,4) = '2019'
;

insert into wrk.dbo.wc_trsers_opiod
select combo_id, 
	   Service_ID, 
	   try_cast(Days_Supply as int) as ds, 
	   cast(Usual_Customary_Charge as float) as ca, 
	   cast(Net_Amount_Due as float) as aa, 
	   cast([Total Amount_Paid_by_all_Sources] as float) as pa, 
	   '' spec_code, 
	   RX_NUMBER,
	   cast(Date_of_Service as date) as fill_date 
from TRSERS.dbo.TRS_RX_FIN_NEW a 
   join wrk.dbo.temp_wc_ndc b 
     on a.Service_ID  like '%' + b.ndc + '%'
where substring(yearmonth,1,4) = '2019' 
;


select distinct id into wrk.dbo.wc_trsers_opioid_one
from wrk.dbo.wc_trsers_opiod
;


select id, min(fill_date) as first_fill, max(fill_date) as last_fill
into wrk.dbo.wc_teaser_fills
from wrk.dbo.wc_trsers_opiod
group by id 
;

---Step Three 
select *
into wrk.dbo.wc_tease_multiple_scripts
from (
	select id, count(distinct script_id) as scripts, sum(ChargeAmount) as charge, sum(AllowedAmount) as allowed, sum(PaidAmount) as paid
	from wrk.dbo.wc_trsers_opiod
	group by id 
	) x where scripts > 1
;


---Step Four
select * 
into wrk.dbo.wc_tease_30days
from (  	select id, count(distinct script_id) as scripts, sum(dayssupply) as days_supply, sum(AllowedAmount) as allowed, sum(PaidAmount) as paid
	from wrk.dbo.wc_trsers_opiod
	group by id 

) x where days_supply >= 30 


---Step Seven 
select * 
into wrk.dbo.wc_tease_90days
from (  	select id, count(distinct script_id) as scripts, sum(dayssupply) as days_supply, sum(AllowedAmount) as allowed, sum(PaidAmount) as paid
	from wrk.dbo.wc_trsers_opiod
	group by id 

) x where days_supply >= 90 
;

--Step Two 
select distinct combo_id 
into wrk.dbo.wc_trsers_dx_use
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) = '2019' 
and ( 	 REPLACE(a.pri_icd9_dx_cd,'.','') like 'F11%'
	       or REPLACE(a.icd9_dx_cd_2,'.','') like 'F11%' 
	       or REPLACE(a.icd9_dx_cd_3,'.','') like 'F11%'
	       or REPLACE(a.icd9_dx_cd_4,'.','') like 'F11%' 
	       or REPLACE(a.icd9_dx_cd_5,'.','') like 'F11%' 		
	       or REPLACE(a.icd9_dx_cd_6,'.','') like 'F11%'
	       or REPLACE(a.icd9_dx_cd_7,'.','') like 'F11%' 
	       or REPLACE(a.icd9_dx_cd_8,'.','') like 'F11%' 
	       or REPLACE(a.icd9_dx_cd_9,'.','') like 'F11%' 
	       or REPLACE(a.icd9_dx_cd_10,'.','') like 'F11%' 
	       )    	       
;

insert into wrk.dbo.wc_trsers_dx_use 
	select distinct id
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) = '2019'
	  and ( 
	         replace(a.DiagnosisCode1,'.','') like 'F11%'
	         or replace(a.DiagnosisCode2,'.','') like 'F11%'
	         or replace(a.DiagnosisCode3,'.','') like 'F11%'
	         or replace(a.DiagnosisCode4,'.','') like 'F11%'
	         or replace(a.DiagnosisCode5,'.','') like 'F11%'
	       )
; 

--Steps One and Two 
with cohort_cte as ( 
					select 'TRS' as data_source, combo_id , gen, enrlmnth, case when age between 0 and 19 then '1'
							    when age between 20 and 34 then '2' 
								when age between 35 and 44 then '3'
								when age between 45 and 54 then '4'
								when age between 55 and 64 then '5'
								when age between 65 and 74 then '6'
								when age >= 75 then '7' end as age_group 
					from  TRSERS.dbo.TRS_AGG_CY 
					where CY = 2019
					  and AGE >= 18					  
					union 
					select 'ERS' as data_source, ID, gen, enrlmnth, case when age between 0 and 19 then '1'
							    when age between 20 and 34 then '2' 
								when age between 35 and 44 then '3'
								when age between 45 and 54 then '4'
								when age between 55 and 64 then '5'
								when age between 65 and 74 then '6'
								when age >= 75 then '7' end as age_group
					from TRSERS.dbo.ERS_AGG_CY 
					where CY = 2019
					  and AGE >= 18
                  ), 
           measure_cte as ( 
           	                 select * 
           	                 --from wrk.dbo.wc_trsers_opioid_one
                           )
 select * 
 from  
 (           
	select data_source, 
		   'atotal' as measure,
	       count(a.combo_id) as members, 
	       sum(enrlmnth) / 12 as MY, 
	       count(b.id) as opioid_prescribed
	from cohort_cte a 
	  join wrk.dbo.wc_trsers_dx_use u 
	    on u.combo_id = a.combo_id
	  left outer join wrk.dbo.wc_trsers_opioid_one b 
	     on a.combo_id = b.id 
	 where b.id is null 
	group by data_source 
union 
	select data_source, 
		   gen,
	       count(a.combo_id) as members, 
	       sum(enrlmnth) / 12 as MY, 
	       count(b.id) as opioid_prescribed
	from cohort_cte a 
	  join wrk.dbo.wc_trsers_dx_use u 
	    on u.combo_id = a.combo_id
	  left outer join wrk.dbo.wc_trsers_opioid_one b 
	     on a.combo_id = b.id 
	  where b.id is null 
	group by data_source, gen 
union 
	select data_source, 
		   age_group,
	       count(a.combo_id) as members, 
	       sum(enrlmnth) / 12 as MY, 
	       count(b.id) as opioid_prescribed
	from cohort_cte a 
	  join wrk.dbo.wc_trsers_dx_use u 
	    on u.combo_id = a.combo_id
	  left outer join wrk.dbo.wc_trsers_opioid_one b 
	     on a.combo_id = b.id 
	  where b.id is null 
	group by data_source , age_group 
) inr 
order by data_source, measure asc
;
  


--Steps Three
with cohort_cte as ( 
					select 'TRS' as data_source, combo_id , gen, enrlmnth, case when age between 0 and 19 then '1'
							    when age between 20 and 34 then '2' 
								when age between 35 and 44 then '3'
								when age between 45 and 54 then '4'
								when age between 55 and 64 then '5'
								when age between 65 and 74 then '6'
								when age >= 75 then '7' end as age_group 
					from  TRSERS.dbo.TRS_AGG_CY 
					where CY = 2019
					  and AGE >= 18					  
					union 
					select 'ERS' as data_source, ID, gen, enrlmnth, case when age between 0 and 19 then '1'
							    when age between 20 and 34 then '2' 
								when age between 35 and 44 then '3'
								when age between 45 and 54 then '4'
								when age between 55 and 64 then '5'
								when age between 65 and 74 then '6'
								when age >= 75 then '7' end as age_group
					from TRSERS.dbo.ERS_AGG_CY 
					where CY = 2019
					  and AGE >= 18
                  ), 
           measure_cte as ( 
           	                 select * 
           	               --  from wrk.dbo.wc_tease_30days
           	                 from wrk.dbo.wc_tease_multiple_scripts
                           )
 select * 
 from  
 (           
	select data_source, 
		   'atotal' as measure,
	       count(a.combo_id) as members, 
	       sum(scripts) as unique_scripts,
	       cast( sum(scripts) as float)	 / count(a.combo_id) as avg
	       ,sum(charge) as chg ,       sum(allowed) as allowed,	       sum(paid) as paid
	from cohort_cte a 
	  join measure_cte b 
	    on b.id = a.combo_id 
	  join wrk.dbo.wc_teaser_fills c 
	    on c.id = a.combo_id 
	 where not exists ( select 1 
	                    from wrk.dbo.wc_teaser_conditions d 
	                    where d.combo_id = a.combo_id 
	                    and d.dos between dateadd(day,-30,c.first_fill) and dateadd(day,30,c.last_fill) )
	group by data_source 
union 
	select data_source, 
		   gen,
	       count(a.combo_id) as members, 
	       sum(scripts) as unique_scripts,
	       cast( sum(scripts) as float)	       / count(a.combo_id)  as avg 
  ,sum(charge) as chg ,       sum(allowed) as allowed,	       sum(paid) as paid
	from cohort_cte a 
	  join measure_cte b 
	    on b.id = a.combo_id 
	  	  join wrk.dbo.wc_teaser_fills c 
	    on c.id = a.combo_id 
	 where not exists ( select 1 
	                    from wrk.dbo.wc_teaser_conditions d 
	                    where d.combo_id = a.combo_id 
	                    and d.dos between dateadd(day,-30,c.first_fill) and dateadd(day,30,c.last_fill) )  
	group by data_source, gen 
union 
	select data_source, 
		   age_group,
	       count(a.combo_id) as members, 
	       sum(scripts) as unique_scripts,
	       cast(sum(scripts) as float) / count(a.combo_id) as avg
	         ,sum(charge) as chg ,       sum(allowed) as allowed,	       sum(paid) as paid
	from cohort_cte a 
	  join measure_cte b 
	    on b.id = a.combo_id 
	   	  join wrk.dbo.wc_teaser_fills c 
	    on c.id = a.combo_id 
	 where not exists ( select 1 
	                    from wrk.dbo.wc_teaser_conditions d 
	                    where d.combo_id = a.combo_id 
	                    and d.dos between dateadd(day,-30,c.first_fill) and dateadd(day,30,c.last_fill) ) 
	group by data_source , age_group 
) inr 
order by data_source, measure asc
;

---Step 7 and 8 
---opioid dependence
select distinct combo_id
into wrk.dbo.wc_teaser_dependence
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) =  '2019'
and ( 	      substring(REPLACE(a.pri_icd9_dx_cd,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_2,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_3,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_4,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_5,'.',''),1,4) between 'F111' and 'F112'	
	       or substring(REPLACE(a.icd9_dx_cd_6,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_7,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_8,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_9,'.',''),1,4) between 'F111' and 'F112'
	       or substring(REPLACE(a.icd9_dx_cd_10,'.',''),1,4) between 'F111' and 'F112'
	       )  
 ;
 
insert into wrk.dbo.wc_teaser_dependence
select distinct id 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) = '2019'
	  and ( 
	             substring(replace(a.DiagnosisCode1,'.',''),1,4) between 'F111' and 'F112'
	         or substring(replace(a.DiagnosisCode2,'.',''),1,4)  between 'F111' and 'F112'
	         or substring(replace(a.DiagnosisCode3,'.',''),1,4)   between 'F111' and 'F112'
	         or substring(replace(a.DiagnosisCode4,'.',''),1,4)  between 'F111' and 'F112'
	         or substring(replace(a.DiagnosisCode5,'.',''),1,4)  between 'F111' and 'F112'
	       )
;  
 
---opioid use
select distinct combo_id
into wrk.dbo.wc_teaser_use 
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) = '2019'
and ( 	 REPLACE(a.pri_icd9_dx_cd,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_2,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_3,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_4,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_5,'.','') like 'F119%'	
	       or REPLACE(a.icd9_dx_cd_6,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_7,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_8,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_9,'.','') like 'F119%'
	       or REPLACE(a.icd9_dx_cd_10,'.','') like 'F119%'
	       )    	       
  ;


 
 insert into wrk.dbo.wc_teaser_use
	select distinct id
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) = '2019'
	  and ( 
	         replace(a.DiagnosisCode1,'.','')  like 'F119%'
	         or replace(a.DiagnosisCode2,'.','') like 'F119%'
	         or replace(a.DiagnosisCode3,'.','')  like 'F119%'
	         or replace(a.DiagnosisCode4,'.','')  like 'F119%'
	         or replace(a.DiagnosisCode5,'.','') like 'F119%'
	       )
;  
 
 --Steps Seven and Eight 
with cohort_cte as ( 
					select 'TRS' as data_source, combo_id , gen, enrlmnth, case when age between 0 and 19 then '1'
							    when age between 20 and 34 then '2' 
								when age between 35 and 44 then '3'
								when age between 45 and 54 then '4'
								when age between 55 and 64 then '5'
								when age between 65 and 74 then '6'
								when age >= 75 then '7' end as age_group 
					from  TRSERS.dbo.TRS_AGG_CY 
					where CY = 2019
					  and AGE >= 18					  
					union 
					select 'ERS' as data_source, ID, gen, enrlmnth, case when age between 0 and 19 then '1'
							    when age between 20 and 34 then '2' 
								when age between 35 and 44 then '3'
								when age between 45 and 54 then '4'
								when age between 55 and 64 then '5'
								when age between 65 and 74 then '6'
								when age >= 75 then '7' end as age_group
					from TRSERS.dbo.ERS_AGG_CY 
					where CY = 2019
					  and AGE >= 18
                  ), 
           measure_cte as ( 
           	                 select * 
           	                 --from wrk.dbo.wc_tease_90days       
           	                 from wrk.dbo.wc_trsers_opioid_one   	                
                           )
 select * 
 from  
 (           
	select data_source, 
		   'atotal' as measure,
	       count(a.combo_id) as members, 
	       count(c.combo_id) as dep,
	       count(d.combo_id) as u 
	from cohort_cte a 
	  join measure_cte b 
	    on b.id = a.combo_id 
	  left outer join wrk.dbo.wc_teaser_dependence c 
	    on c.combo_id = a.combo_id 
	  left outer join wrk.dbo.wc_teaser_use d 
	    on d.combo_id = a.combo_id 
	group by data_source 
union 
	select data_source, 
		   gen,
	       count(a.combo_id) as members, 
	       count(c.combo_id) as dep,
	       count(d.combo_id) as u
	from cohort_cte a 
	  join measure_cte b 
	    on b.id = a.combo_id 
	  left outer join wrk.dbo.wc_teaser_dependence c 
	    on c.combo_id = a.combo_id 
	  left outer join wrk.dbo.wc_teaser_use d 
	    on d.combo_id = a.combo_id 
	group by data_source, gen 
union 
	select data_source, 
		   age_group,
	       count(a.combo_id) as members, 
	       count(c.combo_id) as dep,
	       count(d.combo_id) as u
	from cohort_cte a
	  join measure_cte b 
	    on b.id = a.combo_id 
	  left outer join wrk.dbo.wc_teaser_dependence c 
	    on c.combo_id = a.combo_id 
	  left outer join wrk.dbo.wc_teaser_use d 
	    on d.combo_id = a.combo_id 
	group by data_source , age_group 
) inr 
order by data_source, measure asc
;
