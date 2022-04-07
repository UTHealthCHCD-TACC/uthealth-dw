---TDCJ influenza vacc	

drop table if exists wrk.dbo.wc_5a_tdcj_flu_clms;
					
---free world		 
select state_id, FSCYR 
into wrk.dbo.wc_5a_tdcj_flu_clms	
from (

	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.AS400_201617_SID 
	where [PROC CODE] in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')				 
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.AS400_201819_SID 
	where [PROC CODE] in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')	
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.FREEWRLD_20
	where [PROC_CD] in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.PCM201819_SID 
	where [PROC_CD] in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')

) inr ;


select * from TDCJ1620.dbo.INST_EPIC_SID --where DESCRIPTION like '%FLU%';

---ttumc
insert into wrk.dbo.wc_5a_tdcj_flu_clms				
select state_id, FSCYR 
from 
(
	select STATE_ID, FSCYR 
	from TDCJ1620.dbo.TTUMC_CLM_SID
	where  CPT_Cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
union 
	select SID_NO, 2020 as FSCYR  
	from TDCJ1620.dbo.TTUMC_CLM_SID_2020 
	where CPT_Cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
) inr ;


---perl
insert into wrk.dbo.wc_5a_tdcj_flu_clms				
select sid_no, FSCYR 
from 
(
	select sid_no , FSCYR 
	from TDCJ1620.dbo.perl_diag_sid a
	where replace(DX,'.','')
	UNION 
	select SID_no , FSCYR 
	from TDCJ1620.dbo.PERL_DX_1620_upd 
	where replace(ICD9,'.','') 
) inr;
	
---epic
insert into wrk.dbo.wc_5a_tdcj_flu_clms				
select state_id, FSCYR 
from 
(

	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.INST_EPIC_SID
	where  UB_CPT_CODE in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')

	UNION
	select SID_NO , 2020 as fscyr
	from TDCJ1620.dbo.INST_EPIC_SID_2020 
	where [PROC Code] in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
	UNION
	select STATE_ID, FSCYR 
	from TDCJ1620.dbo.PROF_EPIC_SID 
	where CPT_CODE in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')		 								 
	UNION
	select  state_id , FSCYR 
	from TDCJ1620.dbo.PROF_EPIC_SID_2020 
	where proc_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662','90672',
'90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756','90653',
'90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
) inr;

---consolidate
select distinct state_id, fscyr 
into wrk.dbo.wc_5a_tdcj_flu_cohort	
from wrk.dbo.wc_5a_tdcj_flu_clms		
;


select * from wrk.dbo.wc_5a_tdcj_flu_cohort	

select * 

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
   left outer join wrk.dbo.wc_5a_tdcj_flu_cohort	 b  
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
from  WRK.dbo.wc_tdcj_CE_new   a 
   left outer join wrk.dbo.wc_5a_tdcj_flu_cohort	 b  
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
from  WRK.dbo.wc_tdcj_CE_new   a 
   left outer join wrk.dbo.wc_5a_tdcj_flu_cohort	 b  
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




