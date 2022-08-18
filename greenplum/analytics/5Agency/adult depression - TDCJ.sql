---TDCJ Adult Depression					 
drop table if exists wrk.dbo.wc_5a_tdcj_depression_cohort	
drop table if exists wrk.dbo.wc_5a_tdcj_depression_clms	

---free world		 
select state_id, FSCYR 
into wrk.dbo.wc_5a_tdcj_depression_clms	
from (
	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.AS400_201617_SID 
	where [PROC CODE] in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
	or replace(DX,'.','') like 'Z133%'					 
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.AS400_201819_SID 
	where [PROC CODE] in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
	or replace(DX,'.','') like 'Z133%'	
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.FREEWRLD_FY20_FY21
	where [PROC_CD] in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
	or replace(DX1,'.','') like 'Z133%'	
	or replace(DX2,'.','') like 'Z133%'
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.PCM201819_SID 
	where [PROC_CD] in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
	or replace(DX,'.','') like 'Z133%'
) inr ;


---ttumc
insert into wrk.dbo.wc_5a_tdcj_depression_clms				
select state_id, FSCYR 
from 
(
	select STATE_ID, FSCYR 
	from TDCJ1620.dbo.TTUMC_CLM_SID
	where replace(DX,'.','') like 'Z133%'
	  or CPT_Cd in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
union 
	select SID_NO, 2020 as FSCYR  
	from TDCJ1620.dbo.TTUMC_CLM_SID_2020 
	where replace(DX_001,'.','') like 'Z133%'
	  or CPT_Cd in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
) inr ;


---perl
insert into wrk.dbo.wc_5a_tdcj_depression_clms				
select sid_no, FSCYR 
from 
(
	select sid_no , FSCYR 
	from TDCJ1620.dbo.perl_diag_sid a
	where replace(DX,'.','') like 'Z133%'
	UNION 
	select SID_no , FSCYR 
	from TDCJ1620.dbo.PERL_DX_1620_upd 
	where replace(ICD9,'.','') like 'Z133%'
) inr;
	
---epic
insert into wrk.dbo.wc_5a_tdcj_depression_clms				
select state_id, FSCYR 
from 
(
	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.INST_EPIC_SID
	where replace(VAL,'.','') like 'Z133%'
	  or UB_CPT_CODE in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')		 
	UNION
	select SID_NO , 2020 as fscyr
	from TDCJ1620.dbo.INST_EPIC_SID_2020 
	where replace(VAL,'.','') like 'Z133%'
	  or replace(DX2 ,'.','') like 'Z133%'
	  or [PROC Code] in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
	UNION
	select STATE_ID, FSCYR 
	from TDCJ1620.dbo.PROF_EPIC_SID 
	where CPT_CODE in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')			 								 
	UNION
	select  state_id , FSCYR 
	from TDCJ1620.dbo.PROF_EPIC_SID_2020 
	where proc_cd in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')
	or replace(dx_cd ,'.','') like 'Z133%'
	or replace(DX2 ,'.','') like 'Z133%'
) inr;

---consolidate
select distinct state_id, fscyr 
into wrk.dbo.wc_5a_tdcj_depression_cohort	
from wrk.dbo.wc_5a_tdcj_depression_clms		
;


-----counts for spreadsheet
---******************************************


 ---collapse records for CE
drop table if exists WRK.dbo.wc_tdcj_CE ;
select * 
into WRK.dbo.wc_tdcj_CE 
from (
	select sum(ENRLMNTH) as enr, sid_no, FSCYR, sex, AGE_FSC, min(left(reg,4)) as region 
	from TDCJ1620.dbo.AGG_ENRL_OFF_UNT_New
	group by sid_no, FSCYR, sex, AGE_FSC	
) x where enr >= 12
;


--by region
select replace(str(a.FSCYR) + case when region is null then 'U' else region end, ' ','' )  as nv, 
       count(distinct a.sid_no) as unique_ce, count(distinct b.state_id) as numer
from  WRK.dbo.wc_tdcj_CE_new   a 
   left outer join wrk.dbo.wc_5a_tdcj_depression_cohort	 b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2020 and a.AGE_FSC >= 18
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
from  WRK.dbo.wc_tdcj_CE_new   a 
   left outer join wrk.dbo.wc_5a_tdcj_depression_cohort	 b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2020  and a.AGE_FSC >= 18
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
from  WRK.dbo.wc_tdcj_CE_new   a 
   left outer join wrk.dbo.wc_5a_tdcj_depression_cohort	 b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2020 and a.AGE_FSC >= 18
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




