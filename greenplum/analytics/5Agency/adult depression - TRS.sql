

select * 
from TRSERS.dbo.TRS_CLM_FIN_NEW tcfn where yearmonth = '201810'


---depression
drop table if exists WRK.dbo.wc_trs_depression_clms
select distinct combo_ID, elig_FSCYR 
into WRK.dbo.wc_trs_depression_clms
from 
(
	select combo_id, ELIG_FSCYR 
	from trsers.dbo.TRS_CLM_FIN_NEW a
	where a.MED_FSCYR between 2016 and 2019
	  and ( 
	         a.prcdr_cd in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161') 
	      or a.pri_icd9_dx_cd  like 'Z133%'
	      or a.icd9_dx_cd_2 like 'Z133%'
	      or a.icd9_dx_cd_3 like 'Z133%'
	      or a.icd9_dx_cd_4 like 'Z133%'
	      or a.icd9_dx_cd_5 like 'Z133%'
	      or a.icd9_dx_cd_6 like 'Z133%'
	      or a.icd9_dx_cd_7 like 'Z133%'
	      or a.icd9_dx_cd_8 like 'Z133%'
	      or a.icd9_dx_cd_9 like 'Z133%'
	      or a.icd9_dx_cd_10 like 'Z133%')
) inr 
;



---exclusions
drop table if exists WRK.dbo.wc_trs_depression_exclusions
select distinct combo_id, ELIG_FSCYR 
into WRK.dbo.wc_trs_depression_exclusions
from (
	    select combo_id, ELIG_FSCYR 
		from trsers.dbo.TRS_CLM_FIN_NEW a
		where a.MED_FSCYR between 2016 and 2019
	       and (   a.pri_icd9_dx_cd in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_2 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_3 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_4 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_5 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_6 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_7 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_8 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_9 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or a.icd9_dx_cd_10 in ('F01.51','F43.21','F43.23','F53.0','F53.1','O90.6','O99.340','O99.341','O99.342','O99.343','O99.345')
	       or (left(a.pri_icd9_dx_cd,3) between 'F31' and 'F34') or a.pri_icd9_dx_cd like '296%'	
	       or (left(a.icd9_dx_cd_2,3) between 'F31' and 'F34') or a.icd9_dx_cd_2 like '296%'
	       or (left(a.icd9_dx_cd_3,3) between 'F31' and 'F34') or a.icd9_dx_cd_3 like '296%'	
	       or (left(a.icd9_dx_cd_4,3) between 'F31' and 'F34') or a.icd9_dx_cd_4 like '296%'	
	       or (left(a.icd9_dx_cd_5,3) between 'F31' and 'F34') or a.icd9_dx_cd_5 like '296%'	
	       or (left(a.icd9_dx_cd_6,3) between 'F31' and 'F34') or a.icd9_dx_cd_6 like '296%'	
	       or (left(a.icd9_dx_cd_7,3) between 'F31' and 'F34') or a.icd9_dx_cd_7 like '296%'	
	       or (left(a.icd9_dx_cd_8,3) between 'F31' and 'F34') or a.icd9_dx_cd_8 like '296%'	
	       or (left(a.icd9_dx_cd_9,3) between 'F31' and 'F34') or a.icd9_dx_cd_9 like '296%'	
	       or (left(a.icd9_dx_cd_10,3) between 'F31' and 'F34') or a.icd9_dx_cd_10 like '296%'	     
	       )
) inrx;



---get denominators for spreadsheet--------------------------------------------------------------------------

---active vs cobra vs ret
select replace( (str(a.FSCYR ) +  stat), ' ','' ) as nv, count(distinct a.combo_id) as denom, count(c.combo_id) as numer  
from TRSERS.dbo.TRS_AGG_YR_FIN a
  left join WRK.dbo.wc_trs_depression_exclusions b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.ELIG_FSCYR 
  left outer join WRK.dbo.wc_trs_depression_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where b.combo_id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18 
  and a.enrlmnth = 12
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;


select distinct stat from TRSERS.dbo.TRS_AGG_YR_FIN 


select count(combo_id), count(distinct combo_id) 
from trsers.dbo.TRS_AGG_YR_FIN 
group by FSCYR 

---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when rel = 'S' then 'E' when rel = 'D' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR_FIN a
  left join WRK.dbo.wc_trs_depression_exclusions b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.ELIG_FSCYR 
  left outer join WRK.dbo.wc_trs_depression_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where b.combo_id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18 
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
from TRSERS.dbo.TRS_AGG_YR_FIN a
  left join WRK.dbo.wc_trs_depression_exclusions b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.ELIG_FSCYR 
  left outer join WRK.dbo.wc_trs_depression_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where b.combo_id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18 
  and a.enrlmnth = 12
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
  left join WRK.dbo.wc_trs_depression_exclusions b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.ELIG_FSCYR 
  left outer join WRK.dbo.wc_trs_depression_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where b.combo_id is null 
  and a.FSCYR between 2016 and 2019 
  and a.age >= 18 
  and a.enrlmnth = 12
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
