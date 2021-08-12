


drop table if exists WRK.dbo.wc_trs_flu_clms
select distinct combo_ID, elig_FSCYR 
into WRK.dbo.wc_trs_flu_clms
from 
(
	select combo_id, ELIG_FSCYR 
	from trsers.dbo.TRS_CLM_FIN_NEW a
	where a.MED_FSCYR between 2016 and 2020
	  and ( 
	         a.prcdr_cd in  ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
            or a.prcdr_cd like 'Q203%'
          ) 
) inr; 



select count(*), count(distinct combo_id), FSCYR  from TRSERS.dbo.TRS_AGG_YR where enrlmnth = 12
group by FSCYR order by FSCYR 

---active vs cobra vs ret
select replace( (str(a.FSCYR ) +  stat), ' ','' ) as nv, count(distinct a.combo_id) as denom, count(c.combo_id) as numer  
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_flu_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2020
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when rel = 'S' then 'E' when rel = 'D' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_flu_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2020
  and a.enrlmnth = 12
group by a.FSCYR , rel, stat 
order by a.FSCYR , stat 
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
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_flu_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2020
  and a.enrlmnth = 12
  and stat <> ''
  and age between 0 and 150 
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
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_flu_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2020
  and a.enrlmnth = 12
  and a.gen <> ''
  and stat <> '' 
  and age between 0 and 150
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
