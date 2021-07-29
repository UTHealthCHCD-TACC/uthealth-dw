--***********************************************************************************************
-----------diag to find denominator Tobacco Use 
--***********************************************************************************************


--free world tobacco claims diag 
select state_id, fscyr  
into WRK.dbo.wc_tdcj_tobacco_clms_temp
from [wrk].[dbo].[FW_Claim_temp] a
  join wrk.dbo.wc_5a_smoking_dx b 
     on REPLACE(a.dx,'.','') = b.diag_cd 
where a.FSCYR between 2016 and 2019
;



--ttumc diag
insert into WRK.dbo.wc_tdcj_tobacco_clms_temp
select state_id, fscyr 
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a
  join wrk.dbo.wc_5a_smoking_dx b 
     on REPLACE(a.dx,'.','') = b.diag_cd 
where a.FSCYR between 2016 and 2019;


----perl diag - check diag_description for wildcard
insert into WRK.dbo.wc_tdcj_tobacco_clms_temp

select a.sid_no , fscyr , diag_cd 
from [tdcj_new].[dbo].[perl_diag_sid] a
  join wrk.dbo.wc_5a_smoking_dx b 
     on REPLACE(a.dx,'.','') = b.diag_cd 
  ;
 
 select * from wrk.dbo.wc_5a_smoking_dx

 ---based on diag desc 
 select distinct dx, DIAG_DESCRIPTION from [tdcj_new].[dbo].[perl_diag_sid] where DIAG_DESCRIPTION like '%TOBACCO%'

 insert into WRK.dbo.wc_tdcj_tobacco_clms_temp
 select sid_no, fscyr 
 from [tdcj_new].[dbo].[perl_diag_sid]
 where DIAG_DESCRIPTION like '%TOBACCO%'
    ;
 
 
-------------****cpt / hcpcs**************


---free world claims cpt
insert into WRK.dbo.wc_tdcj_tobacco_clms_temp
select state_id, fscyr 
from [wrk].[dbo].[FW_Claim_temp] a
where a."proc code" in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	
  and fscyr between 2016 and 2019
;

---ttumc claims cpt
insert into WRK.dbo.wc_tdcj_tobacco_clms_temp
select state_id, fscyr 
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a
where a.CPT_Cd in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	
;

---epic inst claims cpt
insert into WRK.dbo.wc_tdcj_tobacco_clms_temp
select state_id, FSCYR 
from [tdcj_new].[dbo].[INST_EPIC_SID] a 
where a.UB_CPT_CODE in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	
  and STATE_ID is not null 
  and a.FSCYR between 2016 and 2019
  ;
 

  ---epic prof claims cpt
insert into WRK.dbo.wc_tdcj_tobacco_clms_temp
select a.STATE_ID , a.FSCYR 
from [tdcj_new].[dbo].[PROF_EPIC_SID] a
where a.CPT_CODE in ( '99406','99407','G0436','G0437','G9016','S9453','S4995','G9276','G9458',
'1034F','4004F','4001F','G9906','G9907','G9908','G9909')	
  and STATE_ID is not null 
  and a.FSCYR between 2016 and 2019
;



select * 
from WRK.dbo.wc_tdcj_tobacco_clms_temp


---**********************************************************************************************************
---- Tobacco Cessation 
---**********************************************************************************************************
-----------diag

--free world claims diag 
insert into WRK.dbo.wc_tdcj_tobacco_cess_temp
select state_id, fscyr  
from [wrk].[dbo].[FW_Claim_temp] a
where REPLACE(dx,'.','') in ('Z713','Z7189','V653')
  and a.FSCYR between 2016 and 2019
;


--ttumc diag
insert into WRK.dbo.wc_tdcj_tobacco_cess_temp
select state_id, fscyr , a.
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a
where REPLACE(dx,'.','') in ('Z713','Z7189','V653')
  and a.FSCYR between 2016 and 2019

----perl diag - check diag_description for wildcard
insert into WRK.dbo.wc_tdcj_tobacco_cess_temp
select a.sid_no , fscyr 
from [tdcj_new].[dbo].[perl_diag_sid] a
where REPLACE(dx,'.','') in ('Z713','Z7189','V653')
  and a.FSCYR between 2016 and 2019
  ;

 
 select distinct dx, DIAG_DESCRIPTION from [tdcj_new].[dbo].[perl_diag_sid] where DIAG_DESCRIPTION like '%TOBACCO%'
 

--epic inst diag
insert into WRK.dbo.wc_tdcj_weight_counsel_temp
select state_id, FSCYR 
from [tdcj_new].[dbo].[INST_EPIC_SID] a 
where REPLACE(val,'.','')in ('Z713','Z7189','V653')
  and STATE_ID is not null 
  and a.FSCYR between 2016 and 2019
  ;
 
--epic prof doesn't have diag





-----counts for spreadsheet
---******************************************

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
   left outer join WRK.dbo.wc_tdcj_tobacco_clms b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
   left outer join WRK.dbo.wc_tdcj_tobacco_exclusions c 
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
   left outer join WRK.dbo.wc_tdcj_tobacco_clms b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
   left outer join WRK.dbo.wc_tdcj_tobacco_exclusions c 
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
  