--***********************************************************************************************
-----------diag to find denominator (obese population)
--***********************************************************************************************

drop table wrk.dbo.wc_tdcj_obesity_clms_temp ;

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
drop table if exists WRK.dbo.wc_tdcj_dec_enrl ;


--2016 fy 
select distinct i.STATE_ID , '2016' as fy 
into wrk.dbo.wc_tdcj_dec_enrl
from TDCJ_NEW.dbo.INMATE i 
where '2016-08-30' between i.RCV_DT and i.DPT_DT ;

--2017 dec 
insert into wrk.dbo.wc_tdcj_dec_enrl
select distinct i.STATE_ID , '2017' as fy 
from TDCJ_NEW.dbo.INMATE i 
where '2017-08-30' between i.RCV_DT and i.DPT_DT ;

--2018 dec 
insert into wrk.dbo.wc_tdcj_dec_enrl
select distinct i.STATE_ID , '2018' as fy 
from TDCJ_NEW.dbo.INMATE i 
where '2018-08-30' between i.RCV_DT and i.DPT_DT ;

--2019 dec 
insert into wrk.dbo.wc_tdcj_dec_enrl
select distinct i.STATE_ID , '2019' as fy 
from TDCJ_NEW.dbo.INMATE i 
where '2019-08-30' between i.RCV_DT and i.DPT_DT ;

select * 
into WRK.dbo.wc_tdcj_enrl
from (
	select sum(ENRLMNTH) as enr, sid_no, FSCYR, sex, AGE_FSC, min(left(reg,4)) as region 
	from TDCJ_NEW.dbo.AGG_ENRL_OFF_UNT 
	group by sid_no, FSCYR, sex, AGE_FSC
) x 
;


drop table wrk.dbo.wc_tdcj_obese_cohort;

--get obese population
SELECT distinct a.state_id, a.fscyr 
into wrk.dbo.wc_tdcj_obese_cohort
from WRK.dbo.wc_tdcj_obesity_clms_temp a 
   join WRK.dbo.wc_tdcj_dec_enrl  b  
     on a.fscyr = b.FSCYR 
    and a.state_id = b.sid_no 
;




-----counts for spreadsheet
---******************************************


--by region
select replace(str(a.FSCYR) + case when region is null then 'U' else region end, ' ','' )  as nv, 
       count(distinct a.sid_no) as unique_ce, count(distinct b.state_id) as numer
from  WRK.dbo.wc_tdcj_enrl   a 
   join WRK.dbo.wc_tdcj_dec_enrl x 
      on x.state_id = a.sid_no 
     and x.fy = a.fscyr
   left outer join wrk.dbo.wc_tdcj_obese_cohort b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
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
from  WRK.dbo.wc_tdcj_enrl   a 
   join WRK.dbo.wc_tdcj_dec_enrl x 
      on x.state_id = a.sid_no 
     and x.fy = a.fscyr
   left outer join wrk.dbo.wc_tdcj_obese_cohort b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where a.FSCYR between 2016 and 2019
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
from  WRK.dbo.wc_tdcj_enrl   a 
   join WRK.dbo.wc_tdcj_dec_enrl x 
      on x.state_id = a.sid_no 
     and x.fy = a.fscyr
   left outer join wrk.dbo.wc_tdcj_obese_cohort b  
    on b.state_id = a.sid_no 
   and b.fscyr = a.FSCYR 
where  a.FSCYR between 2016 and 2019
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
  