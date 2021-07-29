drop table stage.dbo.wc_HoT_depression_clms

----depression criteria	

--verify cohort to later use in cte
select count(client_nbr) as mems, ENRL_CY 
from (
select client_nbr, enrl_cy, 
       sum(ENRL_MONTHS) as em, 
       min(sex) as sex, 
       min(age) as age, 
       min (zip3) as zip3
from cnd.dbo.AGG_ENRL_Medicaid_CY1219 a
group by CLIENT_NBR, a.ENRL_CY ) inr 
where inr.em >=12 
  and age >= 18
	--and zip3 between '750' and '799'
group by ENRL_CY 
;





---inclusion from claims tables
select pcn, cal_year  
into stage.dbo.wc_HoT_depression_clms
from (
	select p.pcn, year(d.FROM_DOS) as cal_year  
	from [MEDICAID].[dbo].[CLM_DETAIL_16] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_16] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.pcn, year(d.FROM_DOS) as cal_year  
	from [MEDICAID].[dbo].[CLM_DETAIL_17] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_17] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.pcn, year(d.FROM_DOS) as cal_year 
	from [MEDICAID].[dbo].[CLM_DETAIL_18] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_18] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')	
union 
	select p.pcn, year(d.FROM_DOS) as cal_year 
	from [MEDICAID].[dbo].[CLM_DETAIL_19] d
	   	  join [MEDICAID].[dbo].[CLM_PROC_19] p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
) inr;	
	
---inclusions from encounter
insert into stage.dbo.wc_HoT_depression_clms
select mem_id, cal_year
from (
	select p.MEM_ID , year(d.FDOS_DT) as cal_year 
	from [MEDICAID].[dbo].[ENC_DET_16] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_16] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.MEM_ID , year(d.FDOS_DT) as cal_year 
	from [MEDICAID].[dbo].[ENC_DET_17] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_17] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.MEM_ID , year(d.FDOS_DT) as cal_year 
	from [MEDICAID].[dbo].[ENC_DET_18] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_18] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select p.MEM_ID , year(d.FDOS_DT) as cal_year 
	from [MEDICAID].[dbo].[ENC_DET_19] d
	   	  join [MEDICAID].[dbo].[ENC_PROC_19] p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')	
) inr_enc;

---confirm this dx does not exist prior to 2019 data 
select * from [MEDICAID].[dbo].[CLM_DX_18] where PRIM_DX_CD like 'Z133%';
	
---z133 criteria clms
insert into stage.dbo.wc_HoT_depression_clms
select p.PCN, year(h.HDR_FRM_DOS) as cal_year
from [MEDICAID].[dbo].[CLM_DX_19] d
  join [MEDICAID].[dbo].[CLM_PROC_19] p
     on d.ICN = p.ICN 
  join medicaid.dbo.CLM_HEADER_19 h 
     on h.ICN = d.ICN 
where  ( d.PRIM_DX_CD like 'Z133%' or d.ADM_DX_CD like 'Z133%' or d.DX_CD_1 like 'Z133%' or 	d.DX_CD_2 like 'Z133%' or
			d.DX_CD_3 like 'Z133%' or d.DX_CD_4 like 'Z133%' or	d.DX_CD_5 like 'Z133%' or	d.DX_CD_6 like 'Z133%' or
			d.DX_CD_7 like 'Z133%' or d.DX_CD_8 like 'Z133%' or	d.DX_CD_9 like 'Z133%' or	d.DX_CD_10 like 'Z133%' or
			d.DX_CD_11 like 'Z133%' or d.DX_CD_12 like 'Z133%' or d.DX_CD_13 like 'Z133%' or d.DX_CD_14 like 'Z133%' or
			d.DX_CD_15 like 'Z133%' or d.DX_CD_16 like 'Z133%' or d.DX_CD_17 like 'Z133%' or d.DX_CD_18 like 'Z133%' or
			d.DX_CD_19 like 'Z133%' or d.DX_CD_20 like 'Z133%' or d.DX_CD_21 like 'Z133%' or d.DX_CD_22 like 'Z133%' or
			d.DX_CD_23 like 'Z133%' or d.DX_CD_24 like 'Z133%'  or	d.DX_CD_25 like 'Z133%'
); 
			
---z133 criteria encounters
insert into stage.dbo.wc_HoT_depression_clms
select p.MEM_ID , year(h.FRM_DOS) as cal_year
from [MEDICAID].[dbo].[enc_DX_19] d
  join [MEDICAID].[dbo].[enc_PROC_19] p
     on d.DERV_ENC = p.DERV_ENC 
  join [MEDICAID].dbo.ENC_HEADER_19 h 
     on h.DERV_ENC = d.DERV_ENC 
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
insert into stage.dbo.wc_HoT_depression_exclusions
select p.PCN, year(h.HDR_FRM_DOS) as cal_year
--into stage.dbo.wc_HoT_depression_exclusions
from [MEDICAID].[dbo].[CLM_DX_15] d
  join [MEDICAID].[dbo].[CLM_PROC_15] p
     on d.ICN = p.ICN 
  join MEDICAID.dbo.CLM_HEADER_15 h 
    on h.ICN = d.ICN 
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
insert into stage.dbo.wc_HoT_depression_exclusions
select p.MEM_ID ,year(h.FRM_DOS) as cal_year 
--into stage.dbo.wc_HoT_depression_exclusions
from [MEDICAID].[dbo].[enc_DX_19] d
  join [MEDICAID].[dbo].[enc_PROC_19] p
     on d.DERV_ENC = p.DERV_ENC 
  join MEDICAID.dbo.ENC_HEADER_19 h
     on h.DERV_ENC = d.DERV_ENC 
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
drop table if exists stage.dbo.wc_HoT_depression_cohort
select distinct pcn, cal_year 
into stage.dbo.wc_HoT_depression_cohort
from stage.dbo.wc_HoT_depression_clms;
			
--excl
drop table if exists stage.dbo.wc_HoT_depression_excl
select distinct pcn, cal_year 
into stage.dbo.wc_HoT_depression_excl
from stage.dbo.wc_HoT_depression_exclusions
;

----************************************************************************************************
----get counts for spreadsheet
------------------------------------

--total and total by gender
with cte_mcd_enrl as (  select client_nbr, enrl_cy, 
						       sum(ENRL_MONTHS) as em, 
						       min(sex) as sex, 
						       min(age) as age, 
						       min(zip3) as zip
						from cnd.dbo.AGG_ENRL_Medicaid_CY1219 a
						where zip3 between '750' and '799'
						  and age >= 18
						group by CLIENT_NBR, a.ENRL_CY 
					  )
select ENRL_CY, --a.sex, 
       ( count(b.pcn)*1.00 / count(a.client_nbr) )*100 as prev 
from cte_mcd_enrl  a 
  left outer join stage.dbo.wc_HoT_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.cal_year  = a.ENRL_CY 
  left outer join stage.dbo.wc_HoT_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.cal_year = a.ENRL_CY 
where c.pcn is null 
  and a.ENRL_CY = 2017
  and Em >=12
group by a.ENRL_CY--, a.sex 
;

--total by age group 
with cte_mcd_enrl as (  select client_nbr, enrl_cy, 
						       sum(ENRL_MONTHS) as em, 
						       min(sex) as sex, 
						       min(age) as age, 
						       min(zip3) as zip
						from cnd.dbo.AGG_ENRL_Medicaid_CY1219 a
						where zip3 between '750' and '799'
						  and age >= 18
						group by CLIENT_NBR, a.ENRL_CY 
					  )
select ENRL_CY, case  when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	         else 7
	   end as age_group,
       ( count(b.pcn)*1.00 / count(a.client_nbr) )*100 as prev 
from cte_mcd_enrl  a 
  left outer join stage.dbo.wc_HoT_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.cal_year  = a.ENRL_CY 
  left outer join stage.dbo.wc_HoT_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.cal_year = a.ENRL_CY 
where c.pcn is null 
  and a.ENRL_CY = 2017
  and Em >=12
group by a.ENRL_CY ,case  when cast(age as float) between 0  and 17.99 then 1 
	         when cast(age as float) between 18 and 29.99 then 2
	         when cast(age as float) between 30 and 39.99 then 3
	         when cast(age as float) between 40 and 49.99 then 4
	         when cast(age as float) between 50 and 59.99 then 5 
	         when cast(age as float) between 60 and 64.99 then 6 
	         else 7
	   end
;



----prev by zip
with cte_mcd_enrl as (  select client_nbr, enrl_cy, 
						       sum(ENRL_MONTHS) as em, 
						       min(sex) as sex, 
						       min(age) as age, 
						       min(zip3) as zip
						from cnd.dbo.AGG_ENRL_Medicaid_CY1219 a
						where zip3 between '750' and '799'
						  and age >= 18
						group by CLIENT_NBR, a.ENRL_CY 
					  )
select ENRL_CY, zip,
       ( count(b.pcn)*1.00 / count(a.client_nbr) )*100 as prev 
from cte_mcd_enrl  a 
  left outer join stage.dbo.wc_HoT_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.cal_year  = a.ENRL_CY 
  left outer join stage.dbo.wc_HoT_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.cal_year = a.ENRL_CY 
where c.pcn is null 
  and a.ENRL_CY = 2017
  and Em >=12
group by a.ENRL_CY, zip 
order by a.ENRL_CY, zip 
;



----prev by zip and gender
with cte_mcd_enrl as (  select client_nbr, enrl_cy, 
						       sum(ENRL_MONTHS) as em, 
						       min(sex) as sex, 
						       min(age) as age, 
						       min(zip3) as zip
						from cnd.dbo.AGG_ENRL_Medicaid_CY1219 a
						where zip3 between '750' and '799'
						  and age >= 18
						group by CLIENT_NBR, a.ENRL_CY 
					  )
select ENRL_CY, zip,
       (count(b.pcn)*1.00 / count(a.client_nbr)  )*100 as prev 
from cte_mcd_enrl  a 
  left outer join stage.dbo.wc_HoT_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.cal_year  = a.ENRL_CY 
  left outer join stage.dbo.wc_HoT_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.cal_year = a.ENRL_CY 
where c.pcn is null 
  and a.ENRL_CY = 2017
  and Em >=12
  and sex = 'M'
 -- and sex = 'F'
group by a.ENRL_CY, zip 
order by a.ENRL_CY, zip 
;


----prev by zip and age grp
with cte_mcd_enrl as (  select client_nbr, enrl_cy, 
						       sum(ENRL_MONTHS) as em, 
						       min(sex) as sex, 
						       min(age) as age, 
						       min(zip3) as zip
						from cnd.dbo.AGG_ENRL_Medicaid_CY1219 a
						where zip3 between '750' and '799'
						  and age >= 18
						group by CLIENT_NBR, a.ENRL_CY 
					  )
select ENRL_CY, zip,
       (count(b.pcn)*1.00 / count(a.client_nbr) )*100 as prev 
from cte_mcd_enrl  a 
  left outer join stage.dbo.wc_HoT_depression_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.cal_year  = a.ENRL_CY 
  left outer join stage.dbo.wc_HoT_depression_excl c 
     on c.pcn = a.CLIENT_NBR 
    and c.cal_year = a.ENRL_CY 
where c.pcn is null 
  and a.ENRL_CY = 2017
  and Em >=12
  --and age between 0 and 17
-- and age between 18 and 29
 --and age between 30 and 39
 -- and age between 40 and 49
-- and age between 50 and 59
-- and age between 60 and 64
	 and age >= 65
group by a.ENRL_CY, zip 
order by a.ENRL_CY, zip 
;