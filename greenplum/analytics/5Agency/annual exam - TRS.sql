in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')




drop table if exists WRK.dbo.wc_trs_annual_clms
select distinct combo_ID, elig_FSCYR 
into WRK.dbo.wc_trs_annual_clms
from 
(
	select combo_id, ELIG_FSCYR 
	from trsers.dbo.TRS_CLM_FIN_NEW a
	where a.MED_FSCYR between 2016 and 2020
	  and (  replace(a.pri_icd9_dx_cd,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_2,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_3,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_4,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_5,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_6,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_7,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_8,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_9,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or replace(a.icd9_dx_cd_10,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	        or a.prcdr_cd in  ('99381','99382','99383','99384','99385','99386','99387','99391',
							 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
		  )
) inr; 


insert into]=WRK.dbo.wc_trs_annual_clms
select distinct combo_ID, elig_FSCYR 
from 
(
	select combo_id, ELIG_FSCYR 
	from trsers.dbo.TRS_BCBS_FIN_NEW a
	where a.MED_FSCYR between 2016 and 2021
	  and ( 
	         a.hcpcs_cpt_code  in ('99381','99382','99383','99384','99385','99386','99387','99391',
							 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
		  or a.diagnosis_code_1  in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or a.diagnosis_code_2  in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or a.diagnosis_code_3  in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or a.diagnosis_code_4  in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	      or a.diagnosis_code_5  in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
		)
) inr 


---active vs cobra vs ret
select replace( (str(a.FSCYR ) +  stat), ' ','' ) as nv, count(distinct a.combo_id) as denom, count(c.combo_id) as numer  
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_annual_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2021
  and a.enrlmnth = 12
group by a.FSCYR , stat 
union 
---ee vs dep / active vs retiree
select replace( (str(a.FSCYR) +  stat + case when rel = 'S' then 'E' when rel = 'D' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_annual_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2021
  and a.enrlmnth = 12
group by a.FSCYR , rel, stat 
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
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_annual_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2021
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
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR a
  left outer join WRK.dbo.wc_trs_annual_clms c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.ELIG_FSCYR 
where a.FSCYR between 2016 and 2021
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
;
