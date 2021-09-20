							 
					
---free world		 
select state_id, FSCYR 
into wrk.dbo.wc_5a_tdcj_annual_clms	
from (
	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.AS400_201617_SID 
	where [PROC CODE] in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
	or replace(DX,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')							 
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.AS400_201819_SID 
	where [PROC CODE] in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
	or replace(DX,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')		
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.FREEWRLD_20
	where [PROC_CD] in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
	or replace(DX1,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')	
	or replace(DX2,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')	
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.PCM201819_SID 
	where [PROC_CD] in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
	or replace(DX,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')	
) inr ;


---ttumc
insert into wrk.dbo.wc_5a_tdcj_annual_clms				
select state_id, FSCYR 
from 
(
	select STATE_ID, FSCYR 
	from TDCJ1620.dbo.TTUMC_CLM_SID
	where replace(DX,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	  or CPT_Cd in ('99381','99382','99383','99384','99385','99386','99387','99391',
								 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
union 
	select SID_NO, 2020 as FSCYR  
	from TDCJ1620.dbo.TTUMC_CLM_SID_2020 
	where replace(DX_001,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	  or CPT_Cd in ('99381','99382','99383','99384','99385','99386','99387','99391',
								 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
) inr ;


---perl
insert into wrk.dbo.wc_5a_tdcj_annual_clms				
select sid_no, FSCYR 
from 
(
	select sid_no , FSCYR 
	from TDCJ1620.dbo.perl_diag_sid a
	where replace(DX,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	UNION 
	select SID_no , FSCYR 
	from TDCJ1620.dbo.PERL_DX_1620_upd 
	where replace(ICD9,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
) inr;
	
---epic
insert into wrk.dbo.wc_5a_tdcj_annual_clms				
select state_id, FSCYR 
from 
(
	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.INST_EPIC_SID
	where replace(VAL,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	  or UB_CPT_CODE in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 								 
	UNION
	select SID_NO , 2020 as fscyr
	from TDCJ1620.dbo.INST_EPIC_SID_2020 
	where replace(VAL,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	  or replace(DX2 ,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	  or [PROC Code] in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
	UNION
	select STATE_ID, FSCYR 
	from TDCJ1620.dbo.PROF_EPIC_SID 
	where CPT_CODE in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 								 								 
	UNION
	select  state_id , FSCYR 
	from TDCJ1620.dbo.PROF_EPIC_SID_2020 
	where proc_cd in ('99381','99382','99383','99384','99385','99386','99387','99391',
									 '99392','99393','99394','99395','99396','99397','S0610','S0612','S0615' ) 
	or replace(dx_cd ,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
	or replace(DX2 ,'.','') in ('V700','V700','V7231','V705','V703','V7284','V7285','Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419')
) inr;

---consolidate
select distinct state_id, fscyr 
into wrk.dbo.wc_5a_tdcj_annual_cohort	
from wrk.dbo.wc_5a_tdcj_annual_clms		
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

select * from TDCJ1620.dbo.AGG_ENRL_OFF_UNT_New where sid_no = '00254730';

select * from wrk.dbo.wc_tdcj_ce;

--by region
select replace(str(a.FSCYR) + case when region is null then 'U' else region end, ' ','' )  as nv, 
       count(distinct a.sid_no) as unique_ce, count(distinct b.state_id) as numer
from  WRK.dbo.wc_tdcj_CE   a 
   left outer join wrk.dbo.wc_5a_tdcj_annual_cohort	 b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2020
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
from  WRK.dbo.wc_tdcj_CE   a 
   left outer join wrk.dbo.wc_5a_tdcj_annual_cohort	 b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2020
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
from  WRK.dbo.wc_tdcj_CE   a 
   left outer join wrk.dbo.wc_5a_tdcj_annual_cohort	 b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2020
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




