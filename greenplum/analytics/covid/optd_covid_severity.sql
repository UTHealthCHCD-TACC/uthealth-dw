
-----------------------------------
-----All COVID Confirmed Claims 
------------------------------------

drop table if exists dev.optd_covid_allcovid;

--all covid confirmed cases 
select distinct a.patid, 
       a.clmid
into   dev.optd_covid_allcovid
from   optum_dod.diagnostic a 
       join optum_dod.diagnostic b 
         on a.clmid = b.clmid and a.patid = b.patid 
         and a.fst_dt between '2020-01-01' and '2020-12-31' 
         and b.fst_dt between '2020-01-01' and '2020-12-31' 
         where a.diag = 'U071'
              or a.diag = 'U072'
              or a.diag = 'U10'
              or (((a.diag in ('J1282','J208','J988') and b.diag = 'B9729')
              or (a.diag like 'J22%' and b.diag = 'B9729')
              or (a.diag like 'J40%' and b.diag = 'B9729')
              or (a.diag like 'J80%' and b.diag = 'B9729')) 
              and a.fst_dt between '2020-01-01' and '2020-04-01')
              ;
             
select count(distinct patid) from  dev.optd_covid_allcovid; --708,887
select count(distinct clmid) from  dev.optd_covid_allcovid; --708,918


-----------------------------------
-----All COVID Inpatient
------------------------------------

drop table if exists dev.optd_covid_allcovid_inp;

select distinct a.patid,
			 b.clmid
into   dev.optd_covid_allcovid_inp
from   dev.optd_covid_allcovid a 
       join optum_dod.medical b 
         on a.clmid = b.clmid 
         and a.patid = b.patid 
 join optum_dod.confinement c 
 				on a.patid = c.patid
 					and c.conf_id = b.conf_id 
         where substring(b.bill_type,1,1) in ('1','2')
         and substring(b.bill_type,2,1)in ('1','2','5','6','7','8')
         and b.fst_dt between c.admit_date and c.disch_date;
	 
select count(distinct patid) from  dev.optd_covid_allcovid_inp; --111781
select count(distinct clmid) from  dev.optd_covid_allcovid_inp; --211,613
	

-----------------------------------
------------LVL 1 - Suspected
-------------------------------------

drop table if exists dev.optd_covid_lvl_1;

select patid 
into   dev.optd_covid_lvl_1 
from   optum_dod.diagnostic 
where fst_dt  between '2020-01-01' and '2020-12-31' 
      and (diag = 'Z8616' 
        or diag = 'U08' 
        or diag = 'U09' 
        or diag = 'B948'); 
       
select count(distinct patid) from  dev.optd_covid_lvl_1;

select count(*) from  dev.optd_covid_lvl_1; --7,741


-----------------------------------
------------LVL 2/3 Exclusions
-------------------------------------

drop table if exists dev.optd_covid_exclusions;

select distinct patid, clmid
into   dev.optd_covid_exclusions
from   optum_dod.diagnostic 
where  fst_dt between '2020-01-01' and '2020-12-31' 
       and (diag = 'J1289' 
        or diag = 'J40' 
        or diag = 'J22' 
        or diag = 'J988' 
        or diag = 'J80' 
        or diag = 'R05' 
        or diag = 'R0602' 
        or diag = 'R0603' 
        or diag = 'R509');   
      
      select count(distinct patid) from dev.optd_covid_exclusions;
          
-----------------------------------
------------LVL 2 - all covid without exclusion DX on same claim 
-------------------------------------

drop table if exists dev.optd_covid_lvl_2;

select  a.patid
into    dev.optd_covid_lvl_2
from    dev.optd_covid_allcovid a
				left outer join dev.optd_covid_exclusions b 
							on a.clmid = b.clmid 
							and a.patid = b.patid 
	where b.clmid is null;


select count(distinct patid) from dev.optd_covid_lvl_2; --629259

-----------------------------------
------------LVL 3
-------------------------------------   


drop table if exists dev.optd_covid_lvl_3;

select  a.patid
into    dev.optd_covid_lvl_3
from    dev.optd_covid_allcovid a
				join dev.optd_covid_exclusions b 
							on a.clmid = b.clmid 
							and a.patid = b.patid 
				;

select count(distinct patid) from dev.optd_covid_lvl_3; --236454


-----------------------------------
------------LVL 4
-------------------------------------  


drop table if exists dev.optd_covid_lvl_4;

select b.patid 
into   dev.optd_covid_lvl_4 
from   dev.optd_covid_allcovid a 
join   optum_dod.medical b 
				on     a.patid = b.patid 
				and    a.clmid = b.clmid and  
				b.fst_dt between '2020-01-01' and '2020-12-31' 
where  b.rvnu_cd between  '0450' and '0459'
and    not exists 
       ( 
              select 1 
              from   optum_dod.confinement c
              where  a.patid = c.patid 
              and    c.admit_date = b.fst_dt ); 
          

select count(distinct patid) from dev.optd_covid_lvl_4; --96,254

select count(distinct patid) from dev.optd_covid_allcovid_inp; --111,781

-----------------------------------
------------LVL 6
-------------------------------------                                                    
                                       

drop table if exists dev.optd_covid_lvl_6;
                                                                                                                                                                                      
select a.patid 
  into dev.optd_covid_lvl_6
  from dev.optd_covid_allcovid_inp a
  join optum_dod.medical b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 where (( b.proc_cd in ( '94660', '94662', '94779' ) 
        or b.rvnu_cd in ('0270','0175','0998','0272')))
        and substring(b.bill_type,1,1) in ('1','2')
         and substring(b.bill_type,2,1)in ('1','2','5','6','7','8')
         and b.conf_id is not null
         ;
                        
select count(distinct patid) from  dev.optd_covid_lvl_6;

-----------------------------------
------------LVL 7
-------------------------------------
        
drop table if exists dev.optd_covid_lvl_7;  

			--- CPT and Rev codes
select a.patid 
  into dev.optd_covid_lvl_7
  from dev.optd_covid_allcovid_inp a
  join optum_dod.medical b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 where (b.proc_cd in ( '94002', '94003', '94004', '94005','31500') 
 										or b.rvnu_cd = '0410')   
        and substring(b.bill_type,1,1) in ('1','2')
         and substring(b.bill_type,2,1)in ('1','2','5','6','7','8')
         and b.conf_id is not null
         ;
 	
 		
 		---ICD procedure table
insert into dev.optd_covid_lvl_7 (patid)
select a.patid 
  from dev.optd_covid_allcovid_inp a
  join optum_dod."procedure" b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
  join optum_dod.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where ( b."proc" in ( '5A1955Z', '5A1935Z', '5A1945Z' ) 
              or b."proc" like '5A093%' 
              or b."proc" like '5A094%' 
              or b."proc" like '5A095%' )  
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31' ; 
 		
select count(distinct patid) from dev.optd_covid_lvl_7; 
 	
-----------------------------------
------------LVL 8
-------------------------------------          

drop table if exists dev.optd_covid_lvl_8;   

--- renal CPT 
select a.patid 
  into dev.optd_covid_lvl_8
  from dev.optd_covid_allcovid_inp a
  join optum_dod.medical b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 where b.rvnu_cd between '0800' and '0809'    
        and substring(b.bill_type,1,1) in ('1','2')
         and substring(b.bill_type,2,1)in ('1','2','5','6','7','8')
         and b.conf_id is not null
         ;
 		
 	
--renal ICD proc codes
insert into dev.optd_covid_lvl_8 (patid)
select a.patid 
  from dev.optd_covid_allcovid_inp a
  join optum_dod."procedure" b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_dod.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where b."proc" = '5A1D00Z' 
          or b."proc" = '5A1D60Z' 
          or b."proc" like '3E1M39Z'  
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31' ; 
 		
 	
--- ECMO CPT 
insert into dev.optd_covid_lvl_8 (patid)
select a.patid 
  from dev.optd_covid_allcovid_inp a
  join optum_dod.medical b 
         on a.patid = b.patid 
 where (b.proc_cd between '33946' and '33959' 
                or b.proc_cd between '33962' and '33966'
                or b.proc_cd between '33984' and '33989'
                or b.proc_cd = '33969')
         and b.conf_id is not null
         and b.conf_id in 
          (
         select b.conf_id 
         from dev.optd_covid_allcovid_inp a
  				join optum_dod.medical b 
           on a.patid = b.patid and a.clmid = b.clmid
           )
        ; 	
 		
 	
--ECMO ICD proc codes
insert into dev.optd_covid_lvl_8 (patid)
select a.patid 
  from dev.optd_covid_allcovid_inp a
  join optum_dod."procedure" b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_dod.confinement c 
	 				on a.patid = c.patid
	 					and c."year" = 2020
 where b."proc" = '5A1522F' 
          or b."proc" = '5A1522G' 
          or b."proc" like '5A1522H'  
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31';  	
 		
 select count(distinct patid) from dev.optd_covid_lvl_8;   
--5061


--------------------------------------------
--------Level 9
--------------------------------------------    


drop table if exists dev.optd_covid_lvl_9;

select a.patid
into   dev.optd_covid_lvl_9
from   dev.optd_covid_allcovid_inp a
			 join optum_dod.medical b
			 	 on a.clmid = b.clmid 
       join optum_dod.mbrwdeath c
         on a.patid = c.patid 
        join optum_dod.confinement d
	 				on a.patid = d.patid
	 					and d."year" = 2020
where  d.dstatus = '00'
   and b.fst_dt between '2020-01-01' and '2020-12-31'
       and extract(year from b.lst_dt)::text || lpad(extract(month from b.lst_dt)::text,2,'0')
       												= c.death_ym::text 
       and b.fst_dt between d.admit_date and d.disch_date;
 
 select count(distinct patid) from dev.optd_covid_lvl_9;   
      



--------------------------------------------
--------Severity
--------------------------------------------             
             
 
drop table if exists dev.optd_covid_severity;
   
select distinct patid, max(b.uth_member_id) as uth_member_id, Max(lvl) as severity 
into dev.optd_covid_severity
from   (select distinct patid,1 as lvl 
        from   dev.optd_covid_lvl_1
        union 
        select distinct patid,2 as lvl 
        from   dev.optd_covid_lvl_2
        union 
        select distinct patid,3 as lvl 
        from   dev.optd_covid_lvl_3
        union 
        select distinct patid,4 as lvl 
        from   dev.optd_covid_lvl_4
        union 
        select distinct patid,5 as lvl 
        from   dev.optd_covid_allcovid_inp
        union 
        select distinct patid,6 as lvl 
        from   dev.optd_covid_lvl_6
        union 
        select distinct patid,7 as lvl 
        from   dev.optd_covid_lvl_7
        union 
        select distinct patid,8 as lvl 
        from   dev.optd_covid_lvl_8
        union 
        select distinct patid,9 as lvl 
        from   dev.optd_covid_lvl_9) a
join data_warehouse.dim_uth_member_id b
on trim(a.patid::text) = trim(b.member_id_src)
group by 1 ;

select count(distinct patid) from dev.optd_covid_severity; -- 709,846
select count(distinct uth_member_id) from dev.optd_covid_severity; -- 709815
select * from dev.optd_covid_severity where uth_member_id is null;

select severity, count(*) from dev.optd_covid_severity group by severity;


/*


select count(distinct patid) from dev.optd_covid_severity;
drop table if exists dev.optd_covid_severity_age;

select 
				a.patid,
				a.severity,
				case 
         when e.age_derived between 0 and 19 then 1 
         when e.age_derived between 20 and 34 then 2 
         when e.age_derived between 35 and 44 then 3 
         when e.age_derived between 45 and 54 then 4 
         when e.age_derived between 55 and 64 then 5 
         when e.age_derived between 65 and 74 then 6 
         else 7 end as age_group
         into dev.optd_covid_severity_age
         from dev.optd_covid_severity a
         join data_warehouse.member_enrollment_yearly e on a.uth_member_id = e.uth_member_id
         ;
     
        
        
        
        
        
        
        
        
        
        
       
with ages as (
        select patid, max(yrdob) as dob
        from optum_dod.mbr_enroll_r
        group by patid
     )
select 
				a.patid,
				a.severity,
				case 
         when 2020 - b.dob between 0 and 19 then 1 
         when 2020 - b.dob between 20 and 34 then 2 
         when 2020 - b.dob between 35 and 44 then 3 
         when 2020 - b.dob between 45 and 54 then 4 
         when 2020 - b.dob between 55 and 64 then 5 
         when 2020 - b.dob between 65 and 74 then 6 
         else 7 end as age_group
         into dev.optd_covid_severity_age
         from dev.optd_covid_severity a
         join ages b on a.patid = b.patid 
         ;
        
        
 select severity, count(*) from dev.optd_covid_severity_age group by severity;

 select age_group, count(*) from dev.optd_covid_severity_age group by age_group order by age_group;

select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 1 group by age_group order by age_group;
        
select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 2 group by age_group order by age_group;  
        
select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 3 group by age_group order by age_group;         
        
select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 4 group by age_group order by age_group;            

select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 5 group by age_group order by age_group;   

select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 6 group by age_group order by age_group; 

select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 7 group by age_group order by age_group; 

select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 8 group by age_group order by age_group; 

select age_group, count(*) from dev.optd_covid_severity_age 
where severity = 9 group by age_group order by age_group; 


    select * from dev.optd_covid_severity_age;
        
        --704516
         select count(distinct a.uth_member_id)
         from dev.optd_covid_severity a
         join data_warehouse.member_enrollment_yearly e on a.uth_member_id = e.uth_member_id 
         where e."year" = 2021;
         
        select count(*) from dev.optd_covid_severity_age;
*/
