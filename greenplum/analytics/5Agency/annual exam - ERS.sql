

-----get annual use cpt/hcpcs
drop table if exists WRK.dbo.wc_ers_annual_cohort_temp
select ID, fscyr
into WRK.dbo.wc_ers_annual_cohort_temp
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and  a.HCPCSCPTCode in ('99381','99382','99383','99384','99385','99386','99387','99391',
							 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' )        	      	       
union  
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2020
	  and  a.HCPCSCPTCode in ('99381','99382','99383','99384','99385','99386','99387','99391',
							 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' )      
) inrx;



---diags
insert into WRK.dbo.wc_ers_annual_cohort_temp
select ID, FSCYR 
from 
(
	select id, MED_FSCYR as fscyr
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and ( 
	         a.DiagnosisCode1 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or a.DiagnosisCode2 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or a.DiagnosisCode3 in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419') 
	      )
	union 
	select id, FSCYR 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2020
	  and ( 
	         replace(a.DiagnosisCode1,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or replace(a.DiagnosisCode2,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or replace(a.DiagnosisCode3,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or replace(a.DiagnosisCode4,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or replace(a.DiagnosisCode5,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')   
	      )
) inr 
;

--consolidate
drop table if exists wrk.dbo.wc_ers_annual_clms
select distinct id, fscyr 
into wrk.dbo.wc_ers_annual_clms
from WRK.dbo.wc_ers_annual_cohort_temp
;




---active vs cobra vs ret
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.id) as denom, count(c.id) as numer 
from TRSERS.dbo.ERS_AGG_YR a 
  left outer join WRK.dbo.wc_ers_annual_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where  a.FSCYR between 2016 and 2020
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when typ = 'SELF' then 'E' when typ = 'DEP' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.id) as denom, count(c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
  left outer join WRK.dbo.wc_ers_annual_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where  a.FSCYR between 2016 and 2020 
  and a.enrlmnth = 12 
  --and stat in ('A','R')
group by a.FSCYR , typ, stat 
order by a.FSCYR, stat, typ desc--, stat 
;



---age group active vs retiree vs cobra 
select replace( str(a.FSCYR) + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.id) as denom, count(c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
  left outer join WRK.dbo.wc_ers_annual_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where a.FSCYR between 2016 and 2020
  and enrlmnth = 12 
group by  a.fscyr ,  stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
order by  a.fscyr,  stat,    case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
 ;



---age group active vs retiree vs cobra / male vs female 
select replace( str(a.FSCYR) + gen + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.id) as denom, count(c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
  left outer join WRK.dbo.wc_ers_annual_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where  a.FSCYR between 2016 and 2020
  and enrlmnth = 12 
group by  a.fscyr , gen, stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
order by  a.fscyr, a.gen, stat,    case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
 ;









