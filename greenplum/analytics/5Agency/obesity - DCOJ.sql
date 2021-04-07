--***********************************************************************************************
-----------diag to find denominator (obese population)
--***********************************************************************************************
--free world obesity claims diag 
select state_id, fscyr  
into WRK.dbo.wc_tdcj_obesity_clms_temp
from [wrk].[dbo].[FW_Claim_temp] a
where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%'
       )
  and a.FSCYR between 2016 and 2019
;


--ttumc diag
insert into WRK.dbo.wc_tdcj_obesity_clms_temp
select state_id, fscyr 
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a
where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%'
       )
  and a.FSCYR between 2016 and 2019

----perl diag - check diag_description for wildcard
insert into WRK.dbo.wc_tdcj_obesity_clms_temp
select a.sid_no , fscyr 
from [tdcj_new].[dbo].[perl_diag_sid] a
where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%'
       )
  and a.FSCYR between 2016 and 2019
  ;

 
 ---collapse records for CE
drop table if exists WRK.dbo.wc_tdcj_CE ;
select * 
into WRK.dbo.wc_tdcj_CE 
from (
	select sum(ENRLMNTH) as enr, sid_no, FSCYR, sex, AGE_FSC, min(left(reg,4)) as region 
	from TDCJ_NEW.dbo.AGG_ENRL_OFF_UNT 
	group by sid_no, FSCYR, sex, AGE_FSC
) x where enr >= 12
;


--get obese population
SELECT distinct a.state_id, a.fscyr 
into wrk.dbo.wc_tdcj_obese_cohort
from WRK.dbo.wc_tdcj_obesity_clms_temp a 
   join wrk.dbo.wc_tdcj_CE b  
     on a.fscyr = b.FSCYR 
    and a.state_id = b.sid_no 
;


-----***********identify numberator: people who are in weight counseling or treatment


-----------------------------------
-------------****cpt / hcpcs**************
--------------------------------

---free world claims cpt
select state_id, fscyr 
into WRK.dbo.wc_tdcj_weight_counsel_temp
from [wrk].[dbo].[FW_Claim_temp] a
where a."proc code" in ('43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085',
                        '43645','43771','43772','43774','43775','43848','43886','43887','43888')
  and fscyr between 2016 and 2019
;

---ttumc claims cpt
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select state_id, fscyr 
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a
where a.CPT_Cd in  ('43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085',
                        '43645','43771','43772','43774','43775','43848','43886','43887','43888')
;

---epic inst claims cpt
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select distinct APR_DRG --state_id, FSCYR 
from [tdcj_new].[dbo].[INST_EPIC_SID] a 
where a.UB_CPT_CODE in ('43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085',
                        '43645','43771','43772','43774','43775','43848','43886','43887','43888')
  and STATE_ID is not null 
  and a.FSCYR between 2016 and 2019
  ;

  ---epic prof claims cpt
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select a.STATE_ID , a.FSCYR 
from [tdcj_new].[dbo].[PROF_EPIC_SID] a
where a.CPT_CODE in ('43770','43644','43645','43842','43843','43845','43846','43847','43659','S2082','S2085',
                        '43645','43771','43772','43774','43775','43848','43886','43887','43888')
  and STATE_ID is not null 
  and a.FSCYR between 2016 and 2019
;


--***********************************************************************************************
-----------diag for numerator
--***********************************************************************************************

--free world claims diag 
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select state_id, fscyr  
from [wrk].[dbo].[FW_Claim_temp] a
where REPLACE(dx,'.','') in ('Z713','Z7189','V653')
  and a.FSCYR between 2016 and 2019
;


--ttumc diag
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select state_id, fscyr , a.
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a
where REPLACE(dx,'.','') in ('Z713','Z7189','V653')
  and a.FSCYR between 2016 and 2019

----perl diag - check diag_description for wildcard
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select a.sid_no , fscyr 
from [tdcj_new].[dbo].[perl_diag_sid] a
where REPLACE(dx,'.','') in ('Z713','Z7189','V653')
  and a.FSCYR between 2016 and 2019
  ;

 
 select distinct dx, DIAG_DESCRIPTION from [tdcj_new].[dbo].[perl_diag_sid] where DIAG_DESCRIPTION like '%OBESE%'
 

--epic inst diag
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select state_id, FSCYR 
from [tdcj_new].[dbo].[INST_EPIC_SID] a 
where REPLACE(val,'.','')in ('Z713','Z7189','V653')
  and STATE_ID is not null 
  and a.FSCYR between 2016 and 2019
  ;
 
--epic prof doesn't have diag



--***********************************************************************
------icd proc and drg ---------------------------------------------------
--***********************************************************************

--free world doesn't have icd proc or drg, "proc code" is cpt/hpcps
select * 
from [wrk].[dbo].[FW_Claim_temp]
where [PROC CODE] in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')

---epic inst all null
select * 
from [tdcj_new].[dbo].[INST_EPIC_SID] a
where APR_DRG in ('MS619','MS620','MS621','TAPR619','TAPR620','TAPR621')
;

--epic prof doesn't have icd proc or drg
select * 
from [tdcj_new].[dbo].[PROF_EPIC_SID] a 
;


--all null procs in ttumc
select distinct Proc_Cd_002
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a 

where ( a.Proc_Cd_001 in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
       or a.Proc_Cd_002 in ('4389','443','4431','4438','4439','4468','4495','4496','4497','4499','445','4551','4521',
                          '0DV60CZ','0DV60DZ','0DV63CZ','0DV63DZ','0DV64CZ','0DV64DZ','0DV67DZ','0DV68DZ')
       or a.Drg_Cd in ('MS619','MS620','MS621','TAPR619','TAPR620','TAPR621')
      )


---perl doesn't have icd proc or drg
select * 
from TDCJ_NEW.dbo.perl_diag_sid pds --perl_encounter_sid

---validate
select count(*), count(distinct sid_no), FSCYR 
from WRK.dbo.wc_tdcj_CE 
group by fscyr;

-----counts for spreadsheet
---******************************************

select distinct state_id, fscyr 
into wrk.dbo.wc_tdcj_weight_cousel
from WRK.dbo.wc_tdcj_weight_counsel_temp
;


select * from wrk.dbo.wc_tdcj_weight_cousel

--by region
select replace(str(a.FSCYR) + case when region is null then 'U' else region end, ' ','' )  as nv, 
       count(distinct a.sid_no) as unique_ce, count(distinct c.state_id) as numer
from  WRK.dbo.wc_tdcj_CE  a 
   join wrk.dbo.wc_tdcj_obese_cohort b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
   left outer join wrk.dbo.wc_tdcj_weight_cousel c
      on c.state_id = a.sid_no 
     and c.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2019
group by case when region is null then 'U' else region end , a.FSCYR 
order by a.FSCYR , case when region is null then 'U' else region end
;


 --by age group
select replace(str(a.FSCYR) + 'A' +
       str(case when a.AGE_FSC between 0 and 19 then 1 
            when a.AGE_FSC between 20 and 34 then 2 
            when a.AGE_FSC between 35 and 44 then 3 
            when a.AGE_FSC between 45 and 54 then 4 
            when a.AGE_FSC between 55 and 64 then 5 
            when a.AGE_FSC between 65 and 74 then 6 
            else 7 end), ' ','' )  as nv,
       count(distinct a.sid_no) as unique_ce, count(distinct b.state_id) as numer
from  WRK.dbo.wc_tdcj_CE  a 
   left outer join WRK.dbo.wc_tdcj_obesity_clms b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
   left outer join WRK.dbo.wc_tdcj_obesity_exclusions c 
      on c.state_id = a.sid_no 
     and c.fscyr = a.FSCYR 
where c.state_id is null 
  and  a.FSCYR between 2016 and 2019
  and a.AGE_FSC >= 18
group by a.FSCYR , case when a.AGE_FSC between 0 and 19 then 1 
            when a.AGE_FSC between 20 and 34 then 2 
            when a.AGE_FSC between 35 and 44 then 3 
            when a.AGE_FSC between 45 and 54 then 4 
            when a.AGE_FSC between 55 and 64 then 5 
            when a.AGE_FSC between 65 and 74 then 6 
            else 7 end
order by  a.FSCYR , case when a.AGE_FSC between 0 and 19 then 1 
            when a.AGE_FSC between 20 and 34 then 2 
            when a.AGE_FSC between 35 and 44 then 3 
            when a.AGE_FSC between 45 and 54 then 4 
            when a.AGE_FSC between 55 and 64 then 5 
            when a.AGE_FSC between 65 and 74 then 6 
            else 7 end
; 


 --by age group and gender
select replace(str(a.FSCYR) + a.sex +
       str(case when a.AGE_FSC between 0 and 19 then 1 
            when a.AGE_FSC between 20 and 34 then 2 
            when a.AGE_FSC between 35 and 44 then 3 
            when a.AGE_FSC between 45 and 54 then 4 
            when a.AGE_FSC between 55 and 64 then 5 
            when a.AGE_FSC between 65 and 74 then 6 
            else 7 end), ' ','' )  as nv,
       count(distinct a.sid_no) as unique_ce, count(distinct b.state_id) as numer
from WRK.dbo.wc_tdcj_CE  a 
   left outer join WRK.dbo.wc_tdcj_obesity_clms b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
   left outer join WRK.dbo.wc_tdcj_obesity_exclusions c 
      on c.state_id = a.sid_no 
     and c.fscyr = a.FSCYR 
where c.state_id is null 
  and  a.FSCYR between 2016 and 2019
  and a.AGE_FSC >= 18
group by a.FSCYR , sex, case when a.AGE_FSC between 0 and 19 then 1 
            when a.AGE_FSC between 20 and 34 then 2 
            when a.AGE_FSC between 35 and 44 then 3 
            when a.AGE_FSC between 45 and 54 then 4 
            when a.AGE_FSC between 55 and 64 then 5 
            when a.AGE_FSC between 65 and 74 then 6 
            else 7 end
order by  a.FSCYR , sex, case when a.AGE_FSC between 0 and 19 then 1 
            when a.AGE_FSC between 20 and 34 then 2 
            when a.AGE_FSC between 35 and 44 then 3 
            when a.AGE_FSC between 45 and 54 then 4 
            when a.AGE_FSC between 55 and 64 then 5 
            when a.AGE_FSC between 65 and 74 then 6 
            else 7 end
; 
  