--***********************************************************************************************
-----------diag to find denominator (obese population)
--***********************************************************************************************
drop table if exists WRK.dbo.wc_TRS_obese_cohort;

select distinct combo_ID, fscyr
into WRK.dbo.wc_TRS_obese_cohort
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_CLM_FIN_NEW a
		where a.MED_FSCYR between 2016 and 2019
	       and (   REPLACE(a.pri_icd9_dx_cd,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_2,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_3,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_4,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_5,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_6,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_7,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_8,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_9,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.icd9_dx_cd_10,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.pri_icd9_dx_cd,'.','') like 'Z683%' or REPLACE(a.pri_icd9_dx_cd,'.','') like 'Z684%'    
	       or REPLACE(a.icd9_dx_cd_2,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_2,'.','') like 'Z684%' 
	       or REPLACE(a.icd9_dx_cd_3,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_3,'.','') like 'Z684%' 
	       or REPLACE(a.icd9_dx_cd_4,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_4,'.','') like 'Z684%' 
	       or REPLACE(a.icd9_dx_cd_5,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_5,'.','') like 'Z684%' 		
	       or REPLACE(a.icd9_dx_cd_6,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_6,'.','') like 'Z684%'
	       or REPLACE(a.icd9_dx_cd_7,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_7,'.','') like 'Z684%' 
	       or REPLACE(a.icd9_dx_cd_8,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_8,'.','') like 'Z684%' 
	       or REPLACE(a.icd9_dx_cd_9,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_9,'.','') like 'Z684%' 
	       or REPLACE(a.icd9_dx_cd_10,'.','') like 'Z683%' or REPLACE(a.icd9_dx_cd_10,'.','') like 'Z684%' 
	       )    	       
) inrx;




-----get numerator - weight counselling 
select distinct combo_ID, fscyr
into WRK.dbo.wc_TRS_obese_counselling
from (
  select combo_id, med_FSCYR as fscyr 
		from trsers.dbo.TRS_CLM_FIN_NEW a
		where a.MED_FSCYR between 2016 and 2019 		
	       and 
	       (   REPLACE(a.pri_icd9_dx_cd,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_2,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_3,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_4,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_5,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_6,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_7,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_8,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_9,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.icd9_dx_cd_10,'.','') in ('Z713','Z7189','V653')
	       or a.drg_cd in ('619','620','621')
	       or a.icd9_prcdr_cd_1  in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
	       or a.icd9_prcdr_cd_2 in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
	       or a.icd9_prcdr_cd_3  in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
	       or a.icd9_prcdr_cd_4 in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
	       or a.icd9_prcdr_cd_5  in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
	       or a.icd9_prcdr_cd_6  in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
           )	              
) inrx;


---get counts for spreadsheet--------------------------------------------------------------------------
--validate 1 rec per mem per year 
select count(*), count(distinct combo_id), FSCYR 
from TRSERS.dbo.TRS_AGG_YR_FIN 
group by FSCYR order by FSCYR;

select count(*), count(distinct combo_id), FSCYR 
from WRK.dbo.wc_TRS_obese_cohort
group by FSCYR order by FSCYR;

select count(*), count(distinct combo_id), FSCYR 
from WRK.dbo.wc_TRS_obese_counselling
group by FSCYR order by FSCYR;


select * from TRSERS.dbo.TRS_AGG_YRMON_FIN 

---active vs cobra vs ret
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')
    )
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.combo_id) as denom, count(c.combo_id) as numer 
from TRSERS.dbo.TRS_AGG_YR_FIN a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
  join WRK.dbo.wc_TRS_obese_cohort b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_TRS_obese_counselling c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.fscyr
where a.FSCYR between 2016 and 2019 
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')
    )
select replace( (str(a.FSCYR) +  stat + case when rel = 'S' then 'E' when rel = 'D' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.combo_id) as denom, count(c.combo_id) as numer
from TRSERS.dbo.TRS_AGG_YR_FIN a 
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
  join WRK.dbo.wc_TRS_obese_cohort b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_TRS_obese_counselling c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.fscyr
where a.FSCYR between 2016 and 2019 
group by a.FSCYR , rel, stat 
order by a.FSCYR, stat, rel desc
;



---age group active vs retiree vs cobra 
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')	
    )
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
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
  join WRK.dbo.wc_TRS_obese_cohort b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_TRS_obese_counselling c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.fscyr
where a.FSCYR between 2016 and 2019 
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
with dec_cohort as ( 
	select distinct FSCYR, combo_id 
	from TRSERS.dbo.TRS_AGG_YRMON_FIN 
	where yearmonth in ('201608','201708','201808','201908')		
    )
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
   join dec_cohort x 
     on x.combo_id = a.combo_id 
    and x.fscyr = a.FSCYR 
  join WRK.dbo.wc_TRS_obese_cohort b
     on a.combo_id = b.combo_id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_TRS_obese_counselling c 
      on a.combo_id = c.combo_id 
     and a.FSCYR = c.fscyr
where  a.FSCYR between 2016 and 2019 
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