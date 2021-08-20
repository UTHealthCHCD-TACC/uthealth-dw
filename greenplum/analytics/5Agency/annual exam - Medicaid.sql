drop table dev.wc_5a_mdcd_annual_clms


---from claims
select  p.pcn, d.year_fy 
into dev.wc_5a_mdcd_annual_clms
from medicaid.clm_detail d
   	  join medicaid.clm_proc p
	     on d.ICN = p.ICN 
where d.PROC_CD in ('99381','99382','99383','99384','99385','99386','99387','99391',
							 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' )   
  and d.year_fy between 2016 and 2020
;
	
---from encounter
insert into dev.wc_5a_mdcd_annual_clms
select p.mem_id , d.year_fy
from medicaid.enc_det d
   	  join medicaid.enc_proc p
	     on d.derv_enc = p.derv_enc 
where d.proc_cd in ('99381','99382','99383','99384','99385','99386','99387','99391',
							 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' )   
  and d.year_fy between 2016 and 2020
;

---dx
---clms
insert into dev.wc_5a_mdcd_annual_clms
select p.PCN, d.year_fy 
from medicaid.clm_dx d
  join medicaid.clm_proc p
     on d.ICN = p.ICN 
where (   d.PRIM_DX_CD in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.ADM_DX_CD in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_1 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')     	
       or d.DX_CD_2 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_3 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_4 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_5 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_6 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_7 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_8 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_9 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_10 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')   	
       or d.DX_CD_11 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_12 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_13 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_14 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_15 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_16 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_17 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_18 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_19 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_20 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_21 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_22 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_23 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419') 
       or d.DX_CD_24 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_25 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')  		 
);



---enc  
insert into dev.wc_5a_mdcd_annual_clms
select p.MEM_ID , d.year_fy
from medicaid.enc_dx d
  join medicaid.enc_proc p
     on d.DERV_ENC = p.DERV_ENC 
where (   d.PRIM_DX_CD in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.ADM_DX_CD in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_1 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')     	
       or d.DX_CD_2 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_3 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_4 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_5 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_6 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_7 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_8 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_9 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_10 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')   	
       or d.DX_CD_11 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_12 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_13 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_14 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_15 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_16 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_17 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_18 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_19 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_20 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_21 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_22 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
       or d.DX_CD_23 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419') 
       or d.DX_CD_24 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
);



--cohort
drop table if exists dev.wc_5a_mdcd_annual_cohort;
select distinct pcn, year_fy 
into dev.wc_5a_mdcd_annual_cohort
from dev.wc_5a_mdcd_annual_clms;
			

----************************************************************************************************
----get counts for spreadsheet
------------------------------------


----overall by medicaid type
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from medicaid.agg_enrl_mdcd_fscyr 
                       where smib = '0'
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( ((a.ENRL_FY::text) || MCO_PROGRAM_NM), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl  a 
  left outer join dev.wc_5a_mdcd_annual_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.year_fy = a.ENRL_FY 
where  a.ENRL_FY between 2016 and 2020
  and Em >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM
order by a.ENRL_FY, a.MCO_PROGRAM_NM 
;



---overall dual eligible
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from medicaid.agg_enrl_mdcd_fscyr
                       where SMIB = '1'
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( (a.ENRL_FY::text || 'DUAL ELIGIBLE'), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join dev.wc_5a_mdcd_annual_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.year_fy = a.ENRL_FY 
where a.ENRL_FY between 2016 and 2020
  and em >=12
group by a.ENRL_FY
order by a.ENRL_FY
;


---by age group and medicaid type
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from medicaid.agg_enrl_mdcd_fscyr
                       where smib = '0'
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( (a.ENRL_FY::text ||  MCO_PROGRAM_NM  || a.AgeGrp), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join dev.wc_5a_mdcd_annual_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.year_fy = a.ENRL_FY 
where  a.ENRL_FY between 2016 and 2020
  and em >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM, a.AgeGrp 
order by a.ENRL_FY, a.MCO_PROGRAM_NM, a.AgeGrp ;


---by age group, gender, and medicaid type
with cte_mcd_enrl as ( select client_nbr, enrl_fy, sum(ENRL_MONTHS) as em, 
                              min(MCO_PROGRAM_NM) as MCO_PROGRAM_NM, min(sex) as sex, min(age) as age, min(smib) as smib, min(AgeGrp) as agegrp
                       from medicaid.agg_enrl_mdcd_fscyr
                       where smib = '0'
                       group by CLIENT_NBR, ENRL_FY ) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM || SEX || a.AgeGrp::text) , ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join dev.wc_5a_mdcd_annual_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.year_fy = a.ENRL_FY 
where  a.ENRL_FY between 2016 and 2020
  and em >=12
  and sex in ('M','F')
group by a.ENRL_FY , sex, a.MCO_PROGRAM_NM, a.AgeGrp  
order by a.ENRL_FY, sex, a.MCO_PROGRAM_NM, a.AgeGrp 
;







