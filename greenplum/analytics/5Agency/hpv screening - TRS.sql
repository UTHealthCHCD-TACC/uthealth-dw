---hpv
drop table if exists WRK.dbo.wc_trs_hpv_clms;

select count(distinct src_clm_id) as clms, combo_id, FSCYR
into WRK.dbo.wc_trs_hpv_clms
from 
(
	select a.combo_id, a.MED_FSCYR as fscyr, a.src_clm_id 
	from trsers.dbo.TRS_CLM_FIN_NEW a
	where a.MED_FSCYR between 2015 and 2019
	  and a.prcdr_cd in ('90649','90650','90651')
	  
) inr 
group by combo_id, FSCYR
;



--consolidate
drop table wrk.dbo.wc_trs_hpv_vacc;

select min(fscyr) as hpv_start, sum(clms) as cnt, combo_id 
into wrk.dbo.wc_trs_hpv_vacc
from WRK.dbo.wc_trs_hpv_clms
group by combo_id;



---get counts for spreadsheet--------------------------------------------------------------------------
---confirmed 1 record per mem per yr
select count(*), count(distinct combo_id), fscyr 
from TRSERS.dbo.TRS_AGG_YR_FIN 
group by fscyr
order by  fscyr;


---active vs cobra vs ret
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.combo_id) as denom, count(c.combo_id) as numer 
from TRSERS.dbo.TRS_AGG_YR_FIN a
  left outer join WRK.dbo.wc_trs_hpv_vacc c 
      on a.combo_id = c.combo_id 
     and c.cnt > 1 
     and c.hpv_start <= a.FSCYR 
where a.FSCYR between 2016 and 2019 
  and a.age = 13
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when rel = 'S' then 'E' when rel = 'D' then 'D' else 'X' end  ), ' ','' ) as nv, 
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR_FIN a
  left outer join WRK.dbo.wc_trs_hpv_vacc c 
      on a.combo_id = c.combo_id 
     and c.cnt > 1 
     and c.hpv_start <= a.FSCYR       
where a.FSCYR between 2016 and 2019 
  and a.age = 13
  and a.enrlmnth = 12 
group by a.FSCYR , rel, stat
order by a.FSCYR, stat, rel desc
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
from TRSERS.dbo.TRS_AGG_YR_FIN a
  left outer join WRK.dbo.wc_trs_hpv_vacc c 
      on a.combo_id = c.combo_id 
     and c.cnt > 1 
     and c.hpv_start <= a.FSCYR       
where  a.FSCYR between 2016 and 2019 
  and a.AGE = 13
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
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR_FIN a
  left outer join WRK.dbo.wc_trs_hpv_vacc c 
      on a.combo_id = c.combo_id 
     and c.cnt > 1 
     and c.hpv_start <= a.FSCYR       
where  a.FSCYR between 2016 and 2019 
  and a.AGE = 13
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

---- /end 
