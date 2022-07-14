---depression
drop table if exists WRK.dbo.wc_ers_depression_clms
select distinct ID, FSCYR 
into WRK.dbo.wc_ers_depression_clms
from 
(
	select id, MED_FSCYR as fscyr
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and ( 
	         a.HCPCSCPTCode in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161') 
	      or a.DiagnosisCode1 like 'Z133%'
	      or a.DiagnosisCode2 like 'Z133%'
	      or a.DiagnosisCode3 like 'Z133%'	 )
	union 
	select id, FSCYR 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2021
	  and ( 
	         a.HCPCSCPTCode in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161') 
	      or a.DiagnosisCode1 like 'Z133%'
	      or a.DiagnosisCode2 like 'Z133%'
	      or a.DiagnosisCode3 like 'Z133%'
	      or a.DiagnosisCode4 like 'Z133%'
	      or a.DiagnosisCode5 like 'Z133%'    )
) inr 
;



---exclusions
drop table if exists WRK.dbo.wc_ers_depression_exclusions
select distinct ID, fscyr
into WRK.dbo.wc_ers_depression_exclusions
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr , DiagnosisCode1 
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and ( 
	          a.DiagnosisCode1 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
	       or a.DiagnosisCode2 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
	       or a.DiagnosisCode3 in ('F0151','F4321','F4323','F530','F531','O906','O99340','O99341','O99342','O99343','O99345')
	       or (left(a.DiagnosisCode1,3) between 'F31' and 'F34') or a.DiagnosisCode1 like '296%'	
	       or (left(a.DiagnosisCode2,3) between 'F31' and 'F34') or a.DiagnosisCode2 like '296%'	       
	       or (left(a.DiagnosisCode3,3) between 'F31' and 'F34') or a.DiagnosisCode3 like '296%'	       
	       )
union      
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR , DiagnosisCode1 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2021
	  and ( 
	          a.DiagnosisCode1 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.DiagnosisCode2 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.DiagnosisCode3 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.DiagnosisCode4 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.DiagnosisCode5 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or (left(a.DiagnosisCode1,3) between 'F31' and 'F34') or a.DiagnosisCode1 like '296%'	
	       or (left(a.DiagnosisCode2,3) between 'F31' and 'F34') or a.DiagnosisCode2 like '296%'	       
	       or (left(a.DiagnosisCode3,3) between 'F31' and 'F34') or a.DiagnosisCode3 like '296%'	  
	       or (left(a.DiagnosisCode4,3) between 'F31' and 'F34') or a.DiagnosisCode4 like '296%'	     
	       or (left(a.DiagnosisCode5,3) between 'F31' and 'F34') or a.DiagnosisCode5 like '296%'	     
	       )
) inrx;



---get denominators for spreadsheet--------------------------------------------------------------------------

---active vs cobra vs ret
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.id) as denom, count(c.id) as numer 
from TRSERS.dbo.ERS_AGG_YR a 
  left join WRK.dbo.wc_ers_depression_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_depression_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where b.id is null 
  and a.FSCYR between 2016 and 2021
  and a.age >= 18 
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when typ = 'SELF' then 'E' when typ = 'DEP' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.id) as denom, count(c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
  left join WRK.dbo.wc_ers_depression_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr
  left outer join WRK.dbo.wc_ers_depression_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where b.id is null 
  and a.FSCYR between 2016 and 2021 
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
  left join WRK.dbo.wc_ers_depression_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_depression_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where b.id is null 
  and a.FSCYR between 2016 and 2021
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
  left join WRK.dbo.wc_ers_depression_exclusions b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_depression_clms c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
where b.id is null 
  and a.FSCYR between 2016 and 2021
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




