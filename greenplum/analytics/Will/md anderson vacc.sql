select a.ZIPCODE_5, b.CountyName, a.ENRL_YEAR, a.patid 
from OPT_ZIP_TX.dbo.AGG_ENRL_OPTZIPTX a
   join [REF].dbo.ZipCode b  
      on a.ZIPCODE_5 = b.zip 
where patid = '560499200000243'
order by patid, ENRL_YEAR 
;

---confirm only 1 entry per zip
select count(*), count(distinct zip) 
from [REF].dbo.ZipCode w



'90649','90650','90651' then 'HPV'
'90734' then 'MEN'
'90715' then 'TDAP'


select a.ZIPCODE_5, b.CountyName, a.ENRL_YEAR, a.patid 
from OPT_ZIP_TX.dbo.AGG_ENRL_OPTZIPTX a
   join [REF].dbo.ZipCode b  
      on a.ZIPCODE_5 = b.zip 
where patid = '560499200000243'
  and a.age between 13 and 17
order by patid, ENRL_YEAR 
;



----find all vaccinations 2014 to 2018
drop table if exists stage.dbo.wc_mdand_vacc_claims
select * 
into stage.dbo.wc_mdand_vacc_claims
from (
	select patid, year(a.FST_DT) as yr,
	       case when PROC_CD in ('90649','90650','90651') then 'HPV'
			    when PROC_CD in ('90733','90734')  then 'MEN'
			    when PROC_CD in ('90714','90715') then 'TDAP' end as vacc_type
	from OPT_ZIP_TX.dbo.Zip_Medical_2014 a 
	where a.PROC_CD in ('90649','90650','90651','90733','90734','90714','90715')
union 
	select patid, year(a.FST_DT) as yr,
	       case when PROC_CD in ('90649','90650','90651') then 'HPV'
			    when PROC_CD in ('90733','90734')  then 'MEN'
			    when PROC_CD in ('90714','90715')  then 'TDAP' end as vacc_type
	from OPT_ZIP_TX.dbo.Zip_Medical_2015 a 
	where a.PROC_CD in ('90649','90650','90651','90733','90734','90714','90715')
union 
	select patid, year(a.FST_DT) as yr,
	       case when PROC_CD in ('90649','90650','90651') then 'HPV'
			    when PROC_CD in ('90733','90734')  then 'MEN'
			    when PROC_CD in ('90714','90715') then 'TDAP' end as vacc_type
	from OPT_ZIP_TX.dbo.Zip_Medical_2016 a 
	where a.PROC_CD in ('90649','90650','90651','90733','90734','90714','90715')
union 
	select patid, year(a.FST_DT) as yr,
	       case when PROC_CD in ('90649','90650','90651') then 'HPV'
			    when PROC_CD in ('90733','90734') then 'MEN'
			    when PROC_CD in ('90714','90715') then 'TDAP' end as vacc_type
	from OPT_ZIP_TX.dbo.Zip_Medical_2017 a 
	where a.PROC_CD in ('90649','90650','90651','90733','90734','90714','90715')
union 
	select patid, year(a.FST_DT) as yr,
	       case when PROC_CD in ('90649','90650','90651') then 'HPV'
			    when PROC_CD in ('90733','90734')  then 'MEN'
			    when PROC_CD in ('90714','90715') then 'TDAP' end as vacc_type
	from OPT_ZIP_TX.dbo.Zip_Medical_2018 a 
	where a.PROC_CD in ('90649','90650','90651','90733','90734','90714','90715')
) inr 
;


--consolidate
drop table if exists stage.dbo.wc_mdand_vacc;

select distinct patid, yr, vacc_type 
into stage.dbo.wc_mdand_vacc
from stage.dbo.wc_mdand_vacc_claims
;



---commercial
drop table if exists stage.dbo.wc_mdand_cohort;

with cte_maxenrl_com as ( 
	  select patid, gdr_cd, age, ZIPCODE_5, ENRL_YEAR, ENRL_MONTHS, 
      			row_number() over(partition by patid, ENRL_YEAR order by ENRL_YEAR, ENRL_MONTHS desc) as rw 
      from OPT_ZIP_TX.dbo.AGG_ENRL_OPTZIPTX 
      where ENRL_YEAR between 2014 and 2018 
        and AGE between 13 and 17 
        and BUS = 'COM'
                        )
select a.patid, a.gdr_cd, a.age, a.enrl_year, b.CountyName, b.state, 'COM' as bus
into stage.dbo.wc_mdand_cohort
from cte_maxenrl_com a 
   join [REF].dbo.ZipCode b  
      on a.ZIPCODE_5 = b.zip 
where rw = 1
;

select yr, vacc_type, count(*)
from STAGE.dbo.wc_mdand_vacc
group by vacc_type, yr 
order by vacc_type, yr 
;


--breakdown by county
select enrl_year, countyname,
       count(a.patid) as n, 
       count(b.patid) as hpv, count(c.patid) as men, count(d.patid) as tdap,
       cast(count(b.patid) as float) / count(a.patid) as hpv_prev,
       cast(count(c.patid) as float) / count(a.patid) as men_prev,
       cast(count(d.patid) as float) / count(a.patid) as tdap_prev
into stage.dbo.wc_mdand_extract
from STAGE.dbo.wc_mdand_cohort a 
  left outer join STAGE.dbo.wc_mdand_vacc b 
      on a.patid = b.patid 
     and a.enrl_year = b.yr
     and b.vacc_type = 'HPV'
  left outer join STAGE.dbo.wc_mdand_vacc c
      on a.patid = c.patid 
     and a.enrl_year = c.yr 
     and c.vacc_type = 'MEN'  
  left outer join STAGE.dbo.wc_mdand_vacc d
      on a.patid = d.patid 
     and a.enrl_year = d.yr 
     and d.vacc_type = 'TDAP'      
group by enrl_year, countyname
order by enrl_year, countyname
;

--breakdown by county and gender
select enrl_year, countyname, gdr_cd,
       count(a.patid) as n, 
       count(b.patid) as hpv, count(c.patid) as men, count(d.patid) as tdap,
       cast(count(b.patid) as float) / count(a.patid) as hpv_prev,
       cast(count(c.patid) as float) / count(a.patid) as men_prev,
       cast(count(d.patid) as float) / count(a.patid) as tdap_prev
into stage.dbo.wc_mdand_extract_gender
from STAGE.dbo.wc_mdand_cohort a 
  left outer join STAGE.dbo.wc_mdand_vacc b 
      on a.patid = b.patid 
     and a.enrl_year = b.yr
     and b.vacc_type = 'HPV'
  left outer join STAGE.dbo.wc_mdand_vacc c
      on a.patid = c.patid 
     and a.enrl_year = c.yr 
     and c.vacc_type = 'MEN'  
  left outer join STAGE.dbo.wc_mdand_vacc d
      on a.patid = d.patid 
     and a.enrl_year = d.yr 
     and d.vacc_type = 'TDAP'      
group by enrl_year, countyname, gdr_cd
order by enrl_year, countyname, gdr_cd 
;


--breakdown by county and gender
select enrl_year, 
       count(a.patid) as n, 
       count(b.patid) as hpv, count(c.patid) as men, count(d.patid) as tdap,
       cast(count(b.patid) as float) / count(a.patid) as hpv_prev,
       cast(count(c.patid) as float) / count(a.patid) as men_prev,
       cast(count(d.patid) as float) / count(a.patid) as tdap_prev
--into stage.dbo.wc_mdand_extract_gender
from STAGE.dbo.wc_mdand_cohort a 
  left outer join STAGE.dbo.wc_mdand_vacc b 
      on a.patid = b.patid 
     and a.enrl_year = b.yr
     and b.vacc_type = 'HPV'
  left outer join STAGE.dbo.wc_mdand_vacc c
      on a.patid = c.patid 
     and a.enrl_year = c.yr 
     and c.vacc_type = 'MEN'  
  left outer join STAGE.dbo.wc_mdand_vacc d
      on a.patid = d.patid 
     and a.enrl_year = d.yr 
     and d.vacc_type = 'TDAP'      
group by enrl_year
order by enrl_year
;