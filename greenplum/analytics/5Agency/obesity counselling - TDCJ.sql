---TDCJ obesity					 
					
---free world		 
select state_id, FSCYR 
into wrk.dbo.wc_5a_tdcj_obesity_clms	
from (
	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.AS400_201617_SID 
	where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%'
       )			 
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.AS400_201819_SID 
	where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%'
       )
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.FREEWRLD_20
	where ( REPLACE(dx1,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx1,'.','') like 'Z683%'
       or REPLACE(dx1,'.','') like 'Z684%'
       or REPLACE(dx2,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx2,'.','') like 'Z683%'
       or REPLACE(dx2,'.','') like 'Z684%'
       )
	UNION 
	select SID_NO , FSCYR 
	from TDCJ1620.dbo.PCM201819_SID 
    where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%'
       )
) inr ;


---ttumc
insert into wrk.dbo.wc_5a_tdcj_obesity_clms				
select state_id, FSCYR 
from 
(
	select STATE_ID, FSCYR 
	from TDCJ1620.dbo.TTUMC_CLM_SID
	where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%' 
       )
union 
	select SID_NO, 2020 as FSCYR  
	from TDCJ1620.dbo.TTUMC_CLM_SID_2020 
	where ( REPLACE(dx_001,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx_001,'.','') like 'Z683%'
       or REPLACE(dx_001,'.','') like 'Z684%'
       )
) inr ;


---perl
insert into wrk.dbo.wc_5a_tdcj_obesity_clms				
select sid_no, FSCYR 
from 
(
	select sid_no , FSCYR 
	from TDCJ1620.dbo.perl_diag_sid a
	where ( REPLACE(dx,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx,'.','') like 'Z683%'
       or REPLACE(dx,'.','') like 'Z684%'
       )
	UNION 
	select SID_no , FSCYR 
	from TDCJ1620.dbo.PERL_DX_1620_upd 
	where ( REPLACE(icd9,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(icd9,'.','') like 'Z683%'
       or REPLACE(icd9,'.','') like 'Z684%'
       )
) inr;
	
---epic
insert into wrk.dbo.wc_5a_tdcj_obesity_clms				
select state_id, FSCYR 
from 
(
	select STATE_ID , FSCYR 
	from TDCJ1620.dbo.INST_EPIC_SID
	where ( REPLACE(val,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(val,'.','') like 'Z683%'
       or REPLACE(val,'.','') like 'Z684%'
       )	 
	UNION
	select SID_NO , 2020 as fscyr
	from TDCJ1620.dbo.INST_EPIC_SID_2020 
	where ( REPLACE(val ,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(val ,'.','') like 'Z683%'
       or REPLACE(val ,'.','') like 'Z684%'
       )
	UNION
	--select STATE_ID, FSCYR 
	--from TDCJ1620.dbo.PROF_EPIC_SID 
	--where CPT_CODE in ('96127','G8431','G8510','G0444','G8433','G8940','90791','90792','99420','96160','96161')	 d		 								 
	--UNION
	select  state_id , FSCYR 
	from TDCJ1620.dbo.PROF_EPIC_SID_2020 
	where ( REPLACE(dx_cd ,'.','') in ('E660','E661','E662','E668','E669','27800','27801','V853','V8530',
		'V8531','V8532','V8533','V8534','V8535','V8536','V8537','V8538','V8539','V854' )
       or REPLACE(dx_cd ,'.','') like 'Z683%'
       or REPLACE(dx_cd ,'.','') like 'Z684%'
       )
) inr;

---consolidate
select distinct state_id, fscyr 
into wrk.dbo.wc_5a_tdcj_obesity_cohort	
from wrk.dbo.wc_5a_tdcj_obesity_clms		
;


-----counts for spreadsheet
---******************************************


--by region
select replace(str(a.FSCYR) + case when left(reg,4)   is null then 'U' else left(reg,4)  end, ' ','' )  as nv, 
       count(distinct a.sid_no) as unique_ce, count(distinct b.state_id) as numer
from  TDCJ1620.dbo.AGG_ENRL_OFF_UNT_New a 
   left outer join wrk.dbo.wc_5a_tdcj_obesity_cohort	 b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2020 
group by case when left(reg,4)   is null then 'U' else left(reg,4) end , a.FSCYR 
order by a.FSCYR , case when left(reg,4)   is null then 'U' else left(reg,4)  end
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
from  TDCJ1620.dbo.AGG_ENRL_OFF_UNT_New  a 
   left outer join wrk.dbo.wc_5a_tdcj_obesity_cohort	 b  
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
from  TDCJ1620.dbo.AGG_ENRL_OFF_UNT_New   a 
   left outer join wrk.dbo.wc_5a_tdcj_obesity_cohort b  
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




