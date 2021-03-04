drop table stage.dbo.wc_5a_depression_clms

----depression criteria	

---from claims
select pcn, fscyr 
into stage.dbo.wc_5a_depression_clms
from (
	select p.pcn, '2016' as fscyr 
	from [MEDICAID].[dbo].[CLM_DETAIL_16] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_16] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.pcn, '2017' as fscyr 
	from [MEDICAID].[dbo].[CLM_DETAIL_17] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_17] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.pcn, '2018' as fscyr 
	from [MEDICAID].[dbo].[CLM_DETAIL_18] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_18] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.pcn, '2019' as fscyr 
	from [MEDICAID].[dbo].[CLM_DETAIL_19] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_19] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
) inr;	
	
---from encounter
insert into stage.dbo.wc_5a_depression_clms
select mem_id, fscyr 
from (
	select p.MEM_ID , '2016' as fscyr 
	from [MEDICAID].[dbo].[ENC_DET_16] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_16] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.MEM_ID , '2017' as fscyr 
	from [MEDICAID].[dbo].[ENC_DET_17] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_17] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.MEM_ID , '2018' as fscyr 
	from [MEDICAID].[dbo].[ENC_DET_18] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_18] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.MEM_ID , '2019' as fscyr 
	from [MEDICAID].[dbo].[ENC_DET_19] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_19] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')	
) inr_enc;

---confirm this dx does not exist prior to 2019 data 
select * from [MEDICAID].[dbo].[CLM_DX_18] where PRIM_DX_CD like 'Z133%';
	
---z133 criteria clms
insert into stage.dbo.wc_5a_depression_clms
select p.PCN, '2019' as fscyr
from [MEDICAID].[dbo].[CLM_DX_19] d
  join [MEDICAID].[dbo].[CLM_PROC_19] p
     on d.ICN = p.ICN 
where  ( d.PRIM_DX_CD like 'Z133%' or d.ADM_DX_CD like 'Z133%' or d.DX_CD_1 like 'Z133%' or 	d.DX_CD_2 like 'Z133%' or
			d.DX_CD_3 like 'Z133%' or d.DX_CD_4 like 'Z133%' or	d.DX_CD_5 like 'Z133%' or	d.DX_CD_6 like 'Z133%' or
			d.DX_CD_7 like 'Z133%' or d.DX_CD_8 like 'Z133%' or	d.DX_CD_9 like 'Z133%' or	d.DX_CD_10 like 'Z133%' or
			d.DX_CD_11 like 'Z133%' or d.DX_CD_12 like 'Z133%' or d.DX_CD_13 like 'Z133%' or d.DX_CD_14 like 'Z133%' or
			d.DX_CD_15 like 'Z133%' or d.DX_CD_16 like 'Z133%' or d.DX_CD_17 like 'Z133%' or d.DX_CD_18 like 'Z133%' or
			d.DX_CD_19 like 'Z133%' or d.DX_CD_20 like 'Z133%' or d.DX_CD_21 like 'Z133%' or d.DX_CD_22 like 'Z133%' or
			d.DX_CD_23 like 'Z133%' or d.DX_CD_24 like 'Z133%'  or	d.DX_CD_25 like 'Z133%'
); 
			
---z133 criteria
insert into stage.dbo.wc_5a_depression_clms
select p.MEM_ID , '2019' as fscyr
from [MEDICAID].[dbo].[enc_DX_19] d
  join [MEDICAID].[dbo].[enc_PROC_19] p
     on d.DERV_ENC = p.DERV_ENC 
  and ( d.PRIM_DX_CD like 'Z133%' or d.ADM_DX_CD like 'Z133%' or d.DX_CD_1 like 'Z133%' or 	d.DX_CD_2 like 'Z133%' or
			d.DX_CD_3 like 'Z133%' or d.DX_CD_4 like 'Z133%' or	d.DX_CD_5 like 'Z133%' or	d.DX_CD_6 like 'Z133%' or
			d.DX_CD_7 like 'Z133%' or d.DX_CD_8 like 'Z133%' or	d.DX_CD_9 like 'Z133%' or	d.DX_CD_10 like 'Z133%' or
			d.DX_CD_11 like 'Z133%' or d.DX_CD_12 like 'Z133%' or d.DX_CD_13 like 'Z133%' or d.DX_CD_14 like 'Z133%' or
			d.DX_CD_15 like 'Z133%' or d.DX_CD_16 like 'Z133%' or d.DX_CD_17 like 'Z133%' or d.DX_CD_18 like 'Z133%' or
			d.DX_CD_19 like 'Z133%' or d.DX_CD_20 like 'Z133%' or d.DX_CD_21 like 'Z133%' or d.DX_CD_22 like 'Z133%' or
			d.DX_CD_23 like 'Z133%' or d.DX_CD_24 like 'Z133%'  
); 

------ Exclusions *******************************************************************************************************************
---------------------


---clms
insert into stage.dbo.wc_5a_depression_exclusions
select p.PCN, '2019' as fscyr
--into stage.dbo.wc_5a_depression_exclusions
from [MEDICAID].[dbo].[CLM_DX_19] d
  join [MEDICAID].[dbo].[CLM_PROC_19] p
     on d.ICN = p.ICN 
where (   d.PRIM_DX_CD in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
       		or  d.PRIM_DX_CD like '296%' or (left(d.PRIM_DX_CD,3) between 'F31' and 'F34')
       or d.ADM_DX_CD in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.ADM_DX_CD like '296%' or (left(d.ADM_DX_CD,3) between 'F31' and 'F34')
       or d.DX_CD_1 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_1 like '296%' or (left(d.DX_CD_1,3) between 'F31' and 'F34')
       or d.DX_CD_2 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_2 like '296%' or (left(d.DX_CD_2,3) between 'F31' and 'F34')      		 
       or d.DX_CD_3 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_3 like '296%' or (left(d.DX_CD_3,3) between 'F31' and 'F34')
       or d.DX_CD_4 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_4 like '296%' or (left(d.DX_CD_4,3) between 'F31' and 'F34')
       or d.DX_CD_5 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_5 like '296%' or (left(d.DX_CD_5,3) between 'F31' and 'F34')
       or d.DX_CD_6 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_6 like '296%' or (left(d.DX_CD_6,3) between 'F31' and 'F34')
       or d.DX_CD_7 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_7 like '296%' or (left(d.DX_CD_7,3) between 'F31' and 'F34')
       or d.DX_CD_8 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_8 like '296%' or (left(d.DX_CD_8,3) between 'F31' and 'F34')
       or d.DX_CD_9 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_9 like '296%' or (left(d.DX_CD_9,3) between 'F31' and 'F34')
       or d.DX_CD_10 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_10 like '296%' or (left(d.DX_CD_10,3) between 'F31' and 'F34')      	
       or d.DX_CD_11 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_11 like '296%' or (left(d.DX_CD_11,3) between 'F31' and 'F34') 
       or d.DX_CD_12 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_12 like '296%' or (left(d.DX_CD_12,3) between 'F31' and 'F34') 
       or d.DX_CD_13 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_13 like '296%' or (left(d.DX_CD_13,3) between 'F31' and 'F34') 
       or d.DX_CD_14 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_14 like '296%' or (left(d.DX_CD_14,3) between 'F31' and 'F34') 
       or d.DX_CD_15 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_15 like '296%' or (left(d.DX_CD_15,3) between 'F31' and 'F34') 
       or d.DX_CD_16 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_16 like '296%' or (left(d.DX_CD_16,3) between 'F31' and 'F34') 
       or d.DX_CD_17 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_17 like '296%' or (left(d.DX_CD_17,3) between 'F31' and 'F34') 
       or d.DX_CD_18 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_18 like '296%' or (left(d.DX_CD_18,3) between 'F31' and 'F34') 
       or d.DX_CD_19 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_19 like '296%' or (left(d.DX_CD_19,3) between 'F31' and 'F34') 
       or d.DX_CD_20 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_20 like '296%' or (left(d.DX_CD_20,3) between 'F31' and 'F34')     
       or d.DX_CD_21 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_21 like '296%' or (left(d.DX_CD_21,3) between 'F31' and 'F34')  
       or d.DX_CD_22 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_22 like '296%' or (left(d.DX_CD_22,3) between 'F31' and 'F34')  
       or d.DX_CD_23 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_23 like '296%' or (left(d.DX_CD_23,3) between 'F31' and 'F34')  
       or d.DX_CD_24 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_24 like '296%' or (left(d.DX_CD_24,3) between 'F31' and 'F34')  
       or d.DX_CD_25 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_25 like '296%' or (left(d.DX_CD_25,3) between 'F31' and 'F34')        		 
);



---enc  !!! Must run once for each year changing table names !!! 
insert into stage.dbo.wc_5a_depression_exclusions
select p.MEM_ID , '2016' as fscyr
--into stage.dbo.wc_5a_depression_exclusions
from [MEDICAID].[dbo].[enc_DX_16] d
  join [MEDICAID].[dbo].[enc_PROC_16] p
     on d.DERV_ENC = p.DERV_ENC 
where (   d.PRIM_DX_CD in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
       		or  d.PRIM_DX_CD like '296%' or (left(d.PRIM_DX_CD,3) between 'F31' and 'F34')
       or d.ADM_DX_CD in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.ADM_DX_CD like '296%' or (left(d.ADM_DX_CD,3) between 'F31' and 'F34')
       or d.DX_CD_1 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_1 like '296%' or (left(d.DX_CD_1,3) between 'F31' and 'F34')
       or d.DX_CD_2 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_2 like '296%' or (left(d.DX_CD_2,3) between 'F31' and 'F34')      		 
       or d.DX_CD_3 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_3 like '296%' or (left(d.DX_CD_3,3) between 'F31' and 'F34')
       or d.DX_CD_4 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_4 like '296%' or (left(d.DX_CD_4,3) between 'F31' and 'F34')
       or d.DX_CD_5 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_5 like '296%' or (left(d.DX_CD_5,3) between 'F31' and 'F34')
       or d.DX_CD_6 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_6 like '296%' or (left(d.DX_CD_6,3) between 'F31' and 'F34')
       or d.DX_CD_7 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_7 like '296%' or (left(d.DX_CD_7,3) between 'F31' and 'F34')
       or d.DX_CD_8 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_8 like '296%' or (left(d.DX_CD_8,3) between 'F31' and 'F34')
       or d.DX_CD_9 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_9 like '296%' or (left(d.DX_CD_9,3) between 'F31' and 'F34')
       or d.DX_CD_10 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_10 like '296%' or (left(d.DX_CD_10,3) between 'F31' and 'F34')      	
       or d.DX_CD_11 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_11 like '296%' or (left(d.DX_CD_11,3) between 'F31' and 'F34') 
       or d.DX_CD_12 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_12 like '296%' or (left(d.DX_CD_12,3) between 'F31' and 'F34') 
       or d.DX_CD_13 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_13 like '296%' or (left(d.DX_CD_13,3) between 'F31' and 'F34') 
       or d.DX_CD_14 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_14 like '296%' or (left(d.DX_CD_14,3) between 'F31' and 'F34') 
       or d.DX_CD_15 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_15 like '296%' or (left(d.DX_CD_15,3) between 'F31' and 'F34') 
       or d.DX_CD_16 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_16 like '296%' or (left(d.DX_CD_16,3) between 'F31' and 'F34') 
       or d.DX_CD_17 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_17 like '296%' or (left(d.DX_CD_17,3) between 'F31' and 'F34') 
       or d.DX_CD_18 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_18 like '296%' or (left(d.DX_CD_18,3) between 'F31' and 'F34') 
       or d.DX_CD_19 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_19 like '296%' or (left(d.DX_CD_19,3) between 'F31' and 'F34') 
       or d.DX_CD_20 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_20 like '296%' or (left(d.DX_CD_20,3) between 'F31' and 'F34')     
       or d.DX_CD_21 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_21 like '296%' or (left(d.DX_CD_21,3) between 'F31' and 'F34')  
       or d.DX_CD_22 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_22 like '296%' or (left(d.DX_CD_22,3) between 'F31' and 'F34')  
       or d.DX_CD_23 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_23 like '296%' or (left(d.DX_CD_23,3) between 'F31' and 'F34')  
       or d.DX_CD_24 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
      		 or  d.DX_CD_24 like '296%' or (left(d.DX_CD_24,3) between 'F31' and 'F34')     		 
);


----------------------------------------------------get one member per year for counting purposes

--cohort
drop table if exists stage.dbo.wc_5a_depression_cohort
select distinct pcn, fscyr 
into stage.dbo.wc_5a_depression_cohort
from stage.dbo.wc_5a_depression_clms;
			
--excl
drop table if exists stage.dbo.wc_5a_depression_excl
select distinct pcn, fscyr
into stage.dbo.wc_5a_depression_excl
from stage.dbo.wc_5a_depression_exclusions
;

----************************************************************************************************
----get counts for spreadsheet
------------------------------------




----overall by medicaid type
select replace( (str(a.ENRL_FY) + MCO_PROGRAM_NM), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from [stage].[dbo].[AGG_ENRL_MCD_YR] a 
  left outer join stage.dbo.wc_5a_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.fscyr = a.ENRL_FY 
  left outer join stage.dbo.wc_5a_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.fscyr = a.ENRL_FY 
where c.pcn is null 
  and a.ENRL_FY between 2016 and 2019
  and age >= 18 
  and ENRL_MONTHS >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM
order by a.ENRL_FY, a.MCO_PROGRAM_NM ;


---overall dual eligible
select replace( (str(a.ENRL_FY) + 'DUAL ELIGIBLE'), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from [stage].[dbo].[AGG_ENRL_MCD_YR] a 
  left outer join stage.dbo.wc_5a_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.fscyr = a.ENRL_FY 
  left outer join stage.dbo.wc_5a_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.fscyr = a.ENRL_FY 
where c.pcn is null 
  and a.ENRL_FY between 2016 and 2019
  and age >= 18 
  and ENRL_MONTHS >=12
  and a.SMIB = 1
group by a.ENRL_FY
order by a.ENRL_FY


---by age group and medicaid type
select replace( (str(a.ENRL_FY) + MCO_PROGRAM_NM  + str(a.AgeGrp) ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from [stage].[dbo].[AGG_ENRL_MCD_YR] a 
  left outer join stage.dbo.wc_5a_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.fscyr = a.ENRL_FY 
  left outer join stage.dbo.wc_5a_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.fscyr = a.ENRL_FY 
where c.pcn is null 
  and a.ENRL_FY between 2016 and 2019
  and age >= 18 
  and ENRL_MONTHS >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM, a.AgeGrp 
order by a.ENRL_FY, a.MCO_PROGRAM_NM, a.AgeGrp ;


---by age group, gender, and medicaid type
select replace( (str(a.ENRL_FY) + MCO_PROGRAM_NM + SEX + str(a.AgeGrp) ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from [stage].[dbo].[AGG_ENRL_MCD_YR] a 
  left outer join stage.dbo.wc_5a_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.fscyr = a.ENRL_FY 
  left outer join stage.dbo.wc_5a_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.fscyr = a.ENRL_FY 
where c.pcn is null 
  and a.ENRL_FY between 2016 and 2019
  and age >= 18 
  and ENRL_MONTHS >=12
  and sex ='M'
group by a.ENRL_FY , sex, a.MCO_PROGRAM_NM, a.AgeGrp  
order by a.ENRL_FY, sex, a.MCO_PROGRAM_NM, a.AgeGrp ;



select * from [stage].[dbo].[AGG_ENRL_MCD_YR]




