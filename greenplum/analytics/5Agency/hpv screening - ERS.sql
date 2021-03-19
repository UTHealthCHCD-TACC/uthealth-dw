---hpv
drop table if exists WRK.dbo.wc_ers_hpv_clms;

select distinct ID, FSCYR 
into WRK.dbo.wc_ers_hpv_clms
from 
(
	select id, MED_FSCYR as fscyr
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and a.HCPCSCPTCode in ('90649','90650','90651')
union 
	select id, FSCYR 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2019 
 and a.HCPCSCPTCode in ('90649','90650','90651')
) inr 
;





---get denominators for spreadsheet--------------------------------------------------------------------------

---active vs cobra vs ret
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.id) as denom, count(c.id) as numer 
from TRSERS.dbo.ERS_AGG_YR a 
  left outer join WRK.dbo.wc_ers_hpv_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where a.FSCYR between 2016 and 2019 
  and a.age = 13
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when typ = 'SELF' then 'E' when typ = 'DEP' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.id) as denom, count(c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
  left join WRK.dbo.wc_ers_hpv_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr
  left outer join WRK.dbo.wc_ers_hpv_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where b.id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18
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
  left join WRK.dbo.wc_ers_hpv_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_hpv_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where b.id is null 
  and a.FSCYR between 2016 and 2019 
  and a.AGE >= 18
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
  left join WRK.dbo.wc_ers_hpv_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_hpv_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where b.id is null 
  and a.FSCYR between 2016 and 2019 
  and a.AGE >= 18
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















-----count of depressed (numerator) --------------------------------
select count(*), count(distinct id) as uniq, fscyr 
from WRK.dbo.wc_ers_hpv_clms
group by fscyr;

---active vs cobra vs ret
select count(*),count(distinct a.id) as uniq, stat, a.FSCYR 
from TRSERS.dbo.ERS_AGG_YR a 
   join WRK.dbo.wc_ers_hpv_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
  left join WRK.dbo.wc_ers_hpv_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
where b.id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18 
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
select count(distinct a.id) as uniq, a.typ, a.FSCYR 
from TRSERS.dbo.ERS_AGG_YR a 
   join WRK.dbo.wc_ers_hpv_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
  left join WRK.dbo.wc_ers_hpv_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
where b.id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18
  and a.enrlmnth = 12 
  --and stat = 'A'  
  and stat = 'R'
group by a.FSCYR , typ
order by a.FSCYR , typ desc
;




---age group active vs retiree vs cobra / male vs female 
select count(distinct a.id) as uniq,
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end as age_group,
       a.fscyr       
from TRSERS.dbo.ERS_AGG_YR a 
   join WRK.dbo.wc_ers_hpv_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
  left join WRK.dbo.wc_ers_hpv_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
where b.id is null 
  and a.FSCYR between 2016 and 2019 
  and a.AGE >= 18
  and enrlmnth = 12 
  and stat = 'A'  --status A or R
  --and GEN = 'F'  --gender M or F 
group by  a.fscyr ,     case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
order by  a.fscyr,      case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end
 ;





---active vs cobra vs ret
select a.FSCYR, stat, count(distinct a.id) as uniq
from TRSERS.dbo.ERS_AGG_YR a 
  left join WRK.dbo.wc_ers_hpv_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
where b.id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18 
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;

