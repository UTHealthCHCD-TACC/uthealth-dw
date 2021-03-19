--***********************************************************************************************
-----------cpt/hcpcs for cohort 
--***********************************************************************************************

---free world hpv claims cpt
select state_id, fscyr 
into WRK.dbo.wc_tdcj_hpv_clms_temp
from [wrk].[dbo].[FW_Claim_temp] a
where a."proc code" in ('90649','90650','90651')
  and fscyr between 2015 and 2019
;

---ttumc hpv claims cpt
insert into WRK.dbo.wc_tdcj_hpv_clms_temp
select state_id, fscyr 
from [tdcj_new].[dbo].[TTUMC_CLM_SID] a
where a.CPT_Cd in ('90649','90650','90651')
  and a.FSCYR between 2015 and 2019
;

---epic inst hpv claims cpt
insert into WRK.dbo.wc_tdcj_hpv_clms_temp
select state_id, FSCYR 
from [tdcj_new].[dbo].[INST_EPIC_SID] a 
where a.UB_CPT_CODE in ('90649','90650','90651')
  and STATE_ID is not null 
  and a.FSCYR between 2015 and 2019
  ;

  ---epic inst hpv claims cpt
insert into WRK.dbo.wc_tdcj_hpv_clms_temp
select a.STATE_ID , a.FSCYR 
from [tdcj_new].[dbo].[PROF_EPIC_SID] a
where a.CPT_CODE in ('90649','90650','90651')
  and STATE_ID is not null 
  and a.FSCYR between 2015 and 2019
;

-----get distinct cohort
select distinct state_id, fscyr 
into WRK.dbo.wc_tdcj_hpv_clms
from  WRK.dbo.wc_tdcj_hpv_clms_temp
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

select count(*)
from  WRK.dbo.wc_tdcj_CE  a 
where  a.FSCYR between 2016 and 2019
  and a.AGE_FSC = 13


---validate
select count(*), count(distinct sid_no), FSCYR 
from WRK.dbo.wc_tdcj_CE 
group by fscyr;

-----counts for spreadsheet
---******************************************

--by region
select replace(str(a.FSCYR) + case when region is null then 'U' else region end, ' ','' )  as nv, 
       count(distinct a.sid_no) as unique_ce, count(distinct b.state_id) as numer
from  WRK.dbo.wc_tdcj_CE  a 
   left outer join WRK.dbo.wc_tdcj_hpv_clms b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where c.state_id is null 
  and  a.FSCYR between 2016 and 2019
  and a.AGE_FSC = 13
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
   left outer join WRK.dbo.wc_tdcj_hpv_clms b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
   left outer join WRK.dbo.wc_tdcj_hpv_exclusions c 
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
  