drop table dev.wc_5a_mdcd_influenza_clms


---from claims
select  p.pcn, d.year_fy 
into dev.wc_5a_mdcd_influenza_clms
from medicaid.clm_detail d
   	  join medicaid.clm_proc p
	     on d.ICN = p.ICN 
where  ( 
	         d.proc_cd in  ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
            or d.PROC_CD like 'Q203%'
          ) 
  and d.year_fy between 2016 and 2020
;
	
---from encounter
insert into dev.wc_5a_mdcd_influenza_clms
select p.mem_id , d.year_fy
from medicaid.enc_det d
   	  join medicaid.enc_proc p
	     on d.derv_enc = p.derv_enc 
where ( 
	         d.proc_cd in  ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
            or d.PROC_CD like 'Q203%'
          ) 
  and d.year_fy between 2016 and 2020
;



--cohort
drop table if exists dev.wc_5a_mdcd_influenza_cohort;
select distinct pcn, year_fy 
into dev.wc_5a_mdcd_influenza_cohort
from dev.wc_5a_mdcd_influenza_clms;
			

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
  left outer join dev.wc_5a_mdcd_influenza_cohort b 
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
  left outer join dev.wc_5a_mdcd_influenza_cohort b 
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
  left outer join dev.wc_5a_mdcd_influenza_cohort b 
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
  left outer join dev.wc_5a_mdcd_influenza_cohort b 
     on b.pcn = a.CLIENT_NBR 
    and b.year_fy = a.ENRL_FY 
where  a.ENRL_FY between 2016 and 2020
  and em >=12
  and sex in ('M','F')
group by a.ENRL_FY , sex, a.MCO_PROGRAM_NM, a.AgeGrp  
order by a.ENRL_FY, sex, a.MCO_PROGRAM_NM, a.AgeGrp 
;







