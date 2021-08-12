--***********************************************************************************************
-----------diag to find denominator (obese population)
--***********************************************************************************************
drop table if exists WRK.dbo.wc_ers_obese_cohort;

select distinct ID, fscyr
into WRK.dbo.wc_ers_obese_cohort
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and ( 
	          a.DiagnosisCode1 in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or a.DiagnosisCode2 in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or a.DiagnosisCode3 in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or left(a.DiagnosisCode1,4) between 'Z683' and 'Z684' 
	       or left(a.DiagnosisCode2,4) between 'Z683' and 'Z684'  
	       or left(a.DiagnosisCode3,4) between 'Z683' and 'Z684'       
	       )
union      
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2020 
	  and ( 
	          REPLACE(a.DiagnosisCode1,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.DiagnosisCode2,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.DiagnosisCode3,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.DiagnosisCode4,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.DiagnosisCode5,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530','V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
	       or REPLACE(a.DiagnosisCode1,'.','') like 'Z683%'   or REPLACE(a.DiagnosisCode1,'.','') like 'Z684%'   
	       or REPLACE(a.DiagnosisCode2,'.','') like 'Z683%'   or REPLACE(a.DiagnosisCode2,'.','') like 'Z684%'          
	       or REPLACE(a.DiagnosisCode3,'.','') like 'Z683%'   or REPLACE(a.DiagnosisCode3,'.','') like 'Z684%' 
	       or REPLACE(a.DiagnosisCode4,'.','') like 'Z683%'   or REPLACE(a.DiagnosisCode4,'.','') like 'Z684%'      
	       or REPLACE(a.DiagnosisCode5,'.','') like 'Z683%'   or REPLACE(a.DiagnosisCode5,'.','') like 'Z684%'
	       )	      	       
) inrx;




-----get numerator - weight counselling 
drop table if exists WRK.dbo.wc_ers_obese_counselling
select distinct ID, fscyr
into WRK.dbo.wc_ers_obese_counselling
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and ( a.HCPCSCPTCode in ('43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085',
                        '43645','43771','43772','43774','43775','43848','43886','43887','43888')
	       or a.DiagnosisCode1 in ('Z713','Z7189','V653')
	       or a.DiagnosisCode2 in ('Z713','Z7189','V653')
	       or a.DiagnosisCode3 in ('Z713','Z7189','V653')    
	       or a.DRG in ('619','620','621')
	       or  a.ICD9ProcedureCode in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
	       )	      	       
union  
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2020
	  and ( a.HCPCSCPTCode in ('43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085',
                        '43645','43771','43772','43774','43775','43848','43886','43887','43888')                        
	       or REPLACE(a.DiagnosisCode1,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.DiagnosisCode2,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.DiagnosisCode3,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.DiagnosisCode4,'.','') in ('Z713','Z7189','V653')
	       or REPLACE(a.DiagnosisCode5,'.','') in ('Z713','Z7189','V653')
	       or a.DRG in ('619','620','621')
	       or a.ICDProcedureCode1 in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
           or a.ICDProcedureCode2 in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
           or a.ICDProcedureCode3 in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
	       )		       
) inrx;


---get counts for spreadsheet--------------------------------------------------------------------------
--validate 1 rec per mem per year 
select count(*), count(distinct id), FSCYR 
from TRSERS.dbo.ERS_AGG_YR 
group by FSCYR order by FSCYR;

select count(*), count(distinct id), FSCYR 
from WRK.dbo.wc_ers_obese_cohort
group by FSCYR order by FSCYR;

select count(*), count(distinct id), FSCYR 
from WRK.dbo.wc_ers_obese_counselling
group by FSCYR order by FSCYR;


---active vs cobra vs ret
with dec_cohort as ( 
	select distinct id, FSCYR 
	from TRSERS.dbo.ERS_AGG_YRMON 
	where yrmnth in ('201608','201708','201808','201908','202008')
    )
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.id) as denom, count(c.id) as numer 
from TRSERS.dbo.ERS_AGG_YR a 
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR
 --obesity
  left outer join WRK.dbo.wc_ers_obese_cohort c
     on a.id = c.id 
     and a.FSCYR = c.fscyr 
  /*--obesity counselling 
  join WRK.dbo.wc_ers_obese_cohort b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_obese_counselling c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
     */
where a.FSCYR between 2016 and 2020 
group by a.FSCYR , stat 
order by a.FSCYR , stat 
;



---ee vs dep / active vs retiree
with dec_cohort as ( 
	select distinct id, FSCYR 
	from TRSERS.dbo.ERS_AGG_YRMON 
	where yrmnth in ('201608','201708','201808','201908','202008')		
    )
select replace( (str(a.FSCYR) +  stat + case when typ = 'SELF' then 'E' when typ = 'DEP' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.id) as denom, count(c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR 
 --obesity
  left outer join WRK.dbo.wc_ers_obese_cohort c
     on a.id = c.id 
     and a.FSCYR = c.fscyr 
  /*--obesity counselling 
  join WRK.dbo.wc_ers_obese_cohort b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_obese_counselling c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
     */
where a.FSCYR between 2016 and 2020
group by a.FSCYR , typ, stat 
order by a.FSCYR, stat, typ desc--, stat 
;



---age group active vs retiree vs cobra 
with dec_cohort as ( 
	select distinct id, FSCYR 
	from TRSERS.dbo.ERS_AGG_YRMON 
	where yrmnth in ('201608','201708','201808','201908','202008')		
    )
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
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR 
 --obesity
  left outer join WRK.dbo.wc_ers_obese_cohort c
     on a.id = c.id 
     and a.FSCYR = c.fscyr 
  /*--obesity counselling 
  join WRK.dbo.wc_ers_obese_cohort b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_obese_counselling c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
     */
where a.FSCYR between 2016 and 2020
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
	select distinct id, FSCYR 
	from TRSERS.dbo.ERS_AGG_YRMON 
	where yrmnth in ('201608','201708','201808','201908','202008')		
    )
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
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR 
 --obesity
  left outer join WRK.dbo.wc_ers_obese_cohort c
     on a.id = c.id 
     and a.FSCYR = c.fscyr 
  /*--obesity counselling 
  join WRK.dbo.wc_ers_obese_cohort b
     on a.id = b.id 
     and a.FSCYR = b.fscyr 
  left outer join WRK.dbo.wc_ers_obese_counselling c 
      on a.id = c.id 
     and a.FSCYR = c.fscyr
     */
where  a.FSCYR between 2016 and 2020 
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