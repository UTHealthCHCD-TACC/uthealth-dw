--***********************************************************************************************
-----------diag to find tobacco users
--***********************************************************************************************

create table wrk.dbo.wc_5a_smoking_dx (diag_cd varchar(20));

insert into wrk.dbo.wc_5a_smoking_dx  values 
('F17200'),('F17201'),('F17203'),('F17208'),('F17209'),('F17210'),('F17211'),('F17213'),('F17218'),('F17219'),('F17220'),('F17221'),
('F17223'),('F17228'),('F17229'),('F17290'),('F17291'),('F17293'),('F17298'),('F17299'),('T65211A'),('T65211D'),('T65211S'),('T65212A'),
('T65212D'),('T65212S'),('T65213A'),('T65213D'),('T65213S'),('T65214A'),('T65214D'),('T65214S'),('T65221A'),('T65221D'),('T65221S'),('T65222A'),
('T65222D'),('T65222S'),('T65223A'),('T65223D'),('T65223S'),('T65224A'),('T65224D'),('T65224S'),('T65291A'),('T65291D'),('T65291S'),('T65292A'),
('T65292D'),('T65292S'),('T65293A'),('T65293D'),('T65293S'),('T65294A'),('T65294D'),('T65294S'),('Z716'),('Z720'),('Z87891'),('P9681'),('P042'),
('O99330'),('O99331'),('O99332'),('O99333'),('O99334'),('O99335');


drop table if exists WRK.dbo.wc_ers_tobacco_cohort_temp;


select ID, fscyr
into WRK.dbo.wc_ers_tobacco_cohort_temp
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	   join wrk.dbo.wc_5a_smoking_dx d 
	     on a.DiagnosisCode1 = d.diag_cd 
	    or a.DiagnosisCode2 = d.diag_cd 
	    or a.DiagnosisCode3 = d.diag_cd  
	where a.MED_FSCYR between 2016 and 2017 
union      
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	   join wrk.dbo.wc_5a_smoking_dx d 
	     on a.DiagnosisCode1 = d.diag_cd 
	    or a.DiagnosisCode2 = d.diag_cd 
	    or a.DiagnosisCode3 = d.diag_cd 
	    or a.DiagnosisCode4 = d.diag_cd 
	    or a.DiagnosisCode5 = d.diag_cd 
	where a.FSCYR between 2018 and 2020     	       
) inrx;




-----get tobacco use cpt/hcpcs
insert into WRK.dbo.wc_ers_tobacco_cohort_temp
select ID, fscyr
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and  a.HCPCSCPTCode in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	       	      	       
union  
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2020
	  and  a.HCPCSCPTCode in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	     
) inrx;


---consolidate tobacco use
drop table if exists WRK.dbo.wc_ers_tobacco_cohort
select distinct id, fscyr 
into WRK.dbo.wc_ers_tobacco_cohort
from WRK.dbo.wc_ers_tobacco_cohort_temp
;



-----*****************************************************************************************************
---WRK.dbo.wc_ers_tobacco_counselling
-----*****************************************************************************************************

---tobacco counselling diags 
drop table if exists WRK.dbo.wc_ers_tobacco_counselling_temp;


select ID, fscyr
into WRK.dbo.wc_ers_tobacco_counselling_temp
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	   join wrk.dbo.wc_5a_smoking_dx d 
	     on a.DiagnosisCode1 = 'Z716'
	    or a.DiagnosisCode2 = 'Z716'
	    or a.DiagnosisCode3 = 'Z716' 
	where a.MED_FSCYR between 2016 and 2017 
union      
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	   join wrk.dbo.wc_5a_smoking_dx d 
	     on a.DiagnosisCode1 = 'Z716'
	    or a.DiagnosisCode2 = 'Z716'
	    or a.DiagnosisCode3 = 'Z716'
	    or a.DiagnosisCode4 = 'Z716'
	    or a.DiagnosisCode5 = 'Z716'
	where a.FSCYR between 2018 and 2020    	       
) inrx;



---tobacco counselling cpt/hcpcs 
insert into WRK.dbo.wc_ers_tobacco_counselling_temp
select ID, fscyr
from (
---2016 and 2017 UHC only, use MED_FSCYR
	select id, MED_FSCYR as fscyr 
	from trsers.dbo.ers_uhcmedclm a 
	where a.MED_FSCYR between 2016 and 2017 
	  and  a.HCPCSCPTCode in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458','4004F','4001F')	       	      	       
union  
---2018 and 2019 only use BCBS and FSCRY
	select id, FSCYR
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where a.FSCYR between 2018 and 2020
	  and  a.HCPCSCPTCode in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458','4004F','4001F')	     
) inrx;



---tobacco counselling ndc 
insert into WRK.dbo.wc_ers_tobacco_counselling_temp
select ID, fscyr
from TRSERS.dbo.ERS_EGWP_RX a where a.ProductLabelNameDosageFormStrength like '%BUPROPION%';

insert into WRK.dbo.wc_ers_tobacco_counselling_temp
select ID, fscyr
from TRSERS.dbo.ERS_UHC_RX a where a.ProductLabelNameDosageFormStrength like '%BUPROPION%'; 

insert into WRK.dbo.wc_ers_tobacco_counselling_temp
select a.benID, fscyr
from TRSERS.dbo.ERS_CAREMARK_RX a where a.ProductServiceName like '%BUPROPION%';


--consolidate counselling
drop table if exists WRK.dbo.wc_ers_tobacco_counselling
select distinct id, fscyr 
into WRK.dbo.wc_ers_tobacco_counselling
from WRK.dbo.wc_ers_tobacco_counselling_temp;






---get counts for spreadsheet--------------------------------------------------------------------------
--validate 1 rec per mem per year 
select count(*), count(distinct id), FSCYR 
from TRSERS.dbo.ERS_AGG_YR 
group by FSCYR order by FSCYR;

select count(*), count(distinct id), FSCYR 
from WRK.dbo.wc_ers_tobacco_cohort
group by FSCYR order by FSCYR;

select count(*), count(distinct id), FSCYR 
from WRK.dbo.wc_ers_tobacco_counselling
group by FSCYR order by FSCYR;


---active vs cobra vs ret
with dec_cohort as ( 
	select distinct id, FSCYR 
	from TRSERS.dbo.ERS_AGG_YRMON 
	where yrmnth in ('201608','201708','201808','201908','202008')
	  and age >= 15
    )
select replace( (str(a.FSCYR) +  stat), ' ','' ) as nv, count(distinct a.id) as denom, count(distinct c.id) as numer 
from TRSERS.dbo.ERS_AGG_YR a 
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR 
  --tobacco use 
  left outer join WRK.dbo.wc_ers_tobacco_cohort c
     on c.id = a.id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR  
  /*
  --tobacco counselling
   join WRK.dbo.wc_ers_tobacco_cohort b
     on b.id = a.id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR 
   left outer join WRK.dbo.wc_ers_tobacco_counselling c 
      on c.id = a.id 
     and c.fscyr = a.FSCYR
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
	  and age >= 15
    )
select replace( (str(a.FSCYR) +  stat + case when typ = 'SELF' then 'E' when typ = 'DEP' then 'D' else 'X' end ), ' ','' ) as nv, 
       count(distinct a.id) as denom, count(distinct c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR 
  --tobacco use 
  left outer join WRK.dbo.wc_ers_tobacco_cohort c
     on c.id = a.id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR  
  /*
  --tobacco counselling
   join WRK.dbo.wc_ers_tobacco_cohort b
     on b.id = a.id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR 
   left outer join WRK.dbo.wc_ers_tobacco_counselling c 
      on c.id = a.id 
     and c.fscyr = a.FSCYR
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
	  and age >= 15
    )
select replace( str(a.FSCYR) + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.id) as denom, count(distinct c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR 
  --tobacco use 
  left outer join WRK.dbo.wc_ers_tobacco_cohort c
     on c.id = a.id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR  
  /*
  --tobacco counselling
   join WRK.dbo.wc_ers_tobacco_cohort b
     on b.id = a.id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR 
   left outer join WRK.dbo.wc_ers_tobacco_counselling c 
      on c.id = a.id 
     and c.fscyr = a.FSCYR
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
	  and age >= 15
    )
select replace( str(a.FSCYR) + gen + stat + 
       case when age between 0 and 19 then '1'
            when age between 20 and 34 then '2' 
       		when age between 35 and 44 then '3'
       		when age between 45 and 54 then '4'
       		when age between 55 and 64 then '5'
       		when age between 65 and 74 then '6'
       		when age >= 75 then '7' end, ' ','' ) as age_group,
       count(distinct a.id) as denom, count(distinct c.id) as numer
from TRSERS.dbo.ERS_AGG_YR a 
   join dec_cohort x 
     on x.id = a.ID 
    and x.fscyr = a.FSCYR 
  --tobacco use 
  left outer join WRK.dbo.wc_ers_tobacco_cohort c
     on c.id = a.id 
     and c.fscyr between a.FSCYR - 2 and a.FSCYR  
  /*
  --tobacco counselling
   join WRK.dbo.wc_ers_tobacco_cohort b
     on b.id = a.id 
     and b.fscyr between a.FSCYR - 2 and a.FSCYR 
   left outer join WRK.dbo.wc_ers_tobacco_counselling c 
      on c.id = a.id 
     and c.fscyr = a.FSCYR
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