

-----get flu use cpt/hcpcs
drop table if exists WRK.dbo.wc_ers_flu_clms
select distinct ID, fscyr
into WRK.dbo.wc_ers_flu_clms
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and (  a.HCPCSCPTCode in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
            or a.HCPCSCPTCode like 'Q203%'      	 )     	       
union  
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2021
	  and (  a.HCPCSCPTCode in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
            or a.HCPCSCPTCode like 'Q203%'      	 )   
) inrx;



---active vs cobra vs ret
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.id) as denom, count(c.id) as numer 
from TRSERS.dbo.ERS_AGG_YR a 
  left outer join WRK.dbo.wc_ers_flu_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where  a.FSCYR between 2016 and 2021
  and a.enrlmnth = 12
group by a.FSCYR , stat 
--order by a.FSCYR , stat 
union 
---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when typ = 'SELF' then 'E' when typ = 'DEP' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.id) as denom, count(c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
  left outer join WRK.dbo.wc_ers_flu_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where  a.FSCYR between 2016 and 2021 
  and a.enrlmnth = 12 
  --and stat in ('A','R')
group by a.FSCYR , typ, stat 
--order by a.FSCYR, stat, typ desc--, stat 
union 
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
  left outer join WRK.dbo.wc_ers_flu_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where a.FSCYR between 2016 and 2021
  and enrlmnth = 12 
group by  a.fscyr ,  stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
union 
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
  left outer join WRK.dbo.wc_ers_flu_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where  a.FSCYR between 2016 and 2021
  and enrlmnth = 12 
group by  a.fscyr , gen, stat,   case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end

 ;









