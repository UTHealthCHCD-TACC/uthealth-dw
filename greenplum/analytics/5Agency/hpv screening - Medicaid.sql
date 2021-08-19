drop table dev.wc_5a_mdcd_hpv_clms;


---hpv vaccs from claims
select count(distinct icn) as clm, pcn, year_fy
into dev.wc_5a_mdcd_hpv_clms
from (
	select p.pcn , p.icn, p.year_fy
	from medicaid.clm_detail d
	   	  join medicaid.clm_proc p
		     on d.ICN = p.ICN 
	where d.PROC_CD in ('90649','90650','90651')
) x
group by pcn, year_fy;
	

---hpv vaccs from encounter
insert into dev.wc_5a_mdcd_hpv_clms
select count(distinct derv_enc) as clm, mem_id, year_fy
from (
	select p.MEM_ID , p.derv_enc , p.year_fy  
	from medicaid.ENC_DET d
	   	  join medicaid.ENC_PROC p
		     on d.DERV_ENC = p.DERV_ENC 
	where d.PROC_CD in ('90649','90650','90651')
) x 
group by mem_id, year_fy;


---consolidate hpv vaccs
drop table if exists dev.wc_5a_mdcd_hpv_vacc;
select sum(clm) as hpv_vacc_cnt, pcn, min(year_fy) as first_year 
into dev.wc_5a_mdcd_hpv_vacc
from dev.wc_5a_mdcd_hpv_clms 
group by pcn;


select *--count(*), count(distinct pcn)
from dev.wc_5a_mdcd_hpv_vacc;


----************************************************************************************************
----get counts for spreadsheet
------------------------------------

----overall by medicaid type
with cte_mcd_enrl as (    select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp,
                                 sum (enrl_months) as em 
						   from medicaid.agg_enrl_mdcd_fscyr 
						   where smib = '0'
						     and age = 13
   							group by client_nbr, enrl_fy  ) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl  a 
  left outer join dev.wc_5a_mdcd_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
    and b.hpv_vacc_cnt > 1 
    and b.first_year <= a.enrl_fy
where  a.ENRL_FY between 2016 and 2020
  and em >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM
order by a.ENRL_FY, a.MCO_PROGRAM_NM ;



---overall dual eligible
with cte_mcd_enrl as (    select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp,
                                 sum (enrl_months) as em 
						   from medicaid.agg_enrl_mdcd_fscyr 
						   where smib = '1'
						     and age = 13
   							group by client_nbr, enrl_fy  ) 
select replace( (a.ENRL_FY::text || 'DUAL ELIGIBLE'), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join dev.wc_5a_mdcd_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
    and b.hpv_vacc_cnt > 1 
    and b.first_year <= a.enrl_fy
where  a.ENRL_FY between 2016 and 2020
  and em >=12
group by a.ENRL_FY
order by a.ENRL_FY
;


---by age group and medicaid type
with cte_mcd_enrl as (    select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp,
                                 sum (enrl_months) as em 
						   from medicaid.agg_enrl_mdcd_fscyr 
						   where smib = '0'
						     and age = 13
   							group by client_nbr, enrl_fy  ) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM  || a.AgeGrp::text ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join dev.wc_5a_mdcd_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
    and b.hpv_vacc_cnt > 1 
    and b.first_year <= a.enrl_fy
where  a.ENRL_FY between 2016 and 2020
  and em >=12
group by a.ENRL_FY , a.MCO_PROGRAM_NM, a.AgeGrp 
order by a.ENRL_FY, a.MCO_PROGRAM_NM, a.AgeGrp ;


---by age group, gender, and medicaid type
with cte_mcd_enrl as (    select client_nbr, enrl_fy , min(mco_program_nm) as mco_program_nm, min(sex) as sex, min(agegrp) as agegrp,
                                 sum (enrl_months) as em 
						   from medicaid.agg_enrl_mdcd_fscyr 
						   where smib = '0'
						     and age = 13
   group by client_nbr, enrl_fy  ) 
select replace( (a.ENRL_FY::text || MCO_PROGRAM_NM  || SEX || a.AgeGrp::text ), ' ','' )  as nv,
      count(a.CLIENT_NBR) as uniq_den, count(b.pcn) as num
from cte_mcd_enrl a 
  left outer join dev.wc_5a_mdcd_hpv_vacc b 
     on b.pcn = a.CLIENT_NBR 
    and b.hpv_vacc_cnt > 1 
    and b.first_year <= a.enrl_fy
where  a.ENRL_FY between 2016 and 2020
  and em >=12
  and sex in ('M','F')
group by a.ENRL_FY , sex, a.MCO_PROGRAM_NM, a.AgeGrp  
order by a.ENRL_FY, sex, a.MCO_PROGRAM_NM, a.AgeGrp 
;





