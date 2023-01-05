
-----------------------------------
-----All COVID Confirmed Claims 
------------------------------------

drop table if exists dev.optz_covid_allcovid;

--all covid confirmed cases 
select a.patid, 
       a.clmid
into   dev.optz_covid_allcovid
from   optum_zip.diagnostic a 
       join optum_zip.diagnostic b 
         on a.clmid = b.clmid and a.patid = b.patid 
         and a.fst_dt between '2020-01-01' and '2020-12-31' 
         and b.fst_dt between '2020-01-01' and '2020-12-31' 
         where a.diag = 'U071'
              or a.diag = 'U072'
              or a.diag = 'U10'
              or (((a.diag in ('J1282','J208','J988') and b.diag = 'B9729')
              or (a.diag like 'J22%' and b.diag = 'B9729')
              or (a.diag like 'J40%' and b.diag = 'B9729')
              or (a.diag like 'J80%' and b.diag = 'B9729')) and a.fst_dt between '2020-01-01' and '2020-04-01')
group by a.patid, a.clmid;
             
select count(distinct patid) from  dev.optz_covid_allcovid; --708,918
select count(distinct clmid) from  dev.optz_covid_allcovid; --708,918


-----------------------------------
-----All COVID Inpatient
------------------------------------

drop table if exists dev.optz_covid_allcovid_inp;

select a.patid,
			 b.clmid
into   dev.optz_covid_allcovid_inp
from   dev.optz_covid_allcovid a 
       join optum_zip.medical b 
         on a.clmid = b.clmid and a.patid = b.patid 
   join optum_zip.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
         where substring(b.bill_type,1,1) in ('1','2')
         and substring(b.bill_type,2,1)in ('1','2','5','6','7','8')
         and b.fst_dt between c.admit_date and c.disch_date 
	 group by a.patid, b.clmid;
	 
select count(distinct patid) from  dev.optz_covid_allcovid_inp; --118,650
select count(distinct clmid) from  dev.optz_covid_allcovid_inp; --211,613
	

-----------------------------------
------------LVL 1 - Suspected
-------------------------------------

drop table if exists dev.optz_covid_lvl_1;

select patid 
into   dev.optz_covid_lvl_1 
from   optum_zip.diagnostic 
where fst_dt  between '2020-01-01' and '2020-12-31' 
      and (diag = 'Z8616' 
        or diag = 'U08' 
        or diag = 'U09' 
        or diag = 'B948'); 
       
select count(distinct patid) from  dev.optz_covid_lvl_1;

select count(*) from  dev.optz_covid_lvl_1; --7,741


-----------------------------------
------------LVL 2/3 Exclusions
-------------------------------------

drop table if exists dev.optz_covid_exclusions;

select patid, clmid
into   dev.optz_covid_exclusions
from   optum_zip.diagnostic 
where  fst_dt between '2020-01-01' and '2020-12-31' 
       and (diag = 'J1289' 
        or diag = 'J40' 
        or diag = 'J22' 
        or diag = 'J988' 
        or diag = 'J80' 
        or diag = 'R05' 
        or diag = 'R0602' 
        or diag = 'R0603' 
        or diag = 'R509')
group by patid, clmid 
       ;      
          
-----------------------------------
------------LVL 2 - all covid without exclusion DX on same claim 
-------------------------------------

drop table if exists dev.optz_covid_lvl_2;

select  a.patid
into    dev.optz_covid_lvl_2
from    dev.optz_covid_allcovid a
				left outer join dev.optz_covid_exclusions b 
							on a.clmid = b.clmid 
							and a.patid = b.patid 
	where b.clmid is null;



-----------------------------------
------------LVL 3
-------------------------------------   


drop table if exists dev.optz_covid_lvl_3;

select  a.patid
into    dev.optz_covid_lvl_3
from    dev.optz_covid_allcovid a
				join dev.optz_covid_exclusions b 
							on a.clmid = b.clmid 
							and a.patid = b.patid 
				;



-----------------------------------
------------LVL 4
-------------------------------------  


drop table if exists dev.optz_covid_lvl_4;

select b.patid 
into   dev.optz_covid_lvl_4 
from   dev.optz_covid_allcovid a 
join   optum_zip.medical b 
				on     a.patid = b.patid 
				and    a.clmid = b.clmid and  b.fst_dt between '2020-01-01' and '2020-12-31' 
where  b.rvnu_cd between  '0450' and '0459'
and    not exists 
       ( 
              select 1 
              from   optum_zip.confinement c
              where  a.patid = c.patid 
              and    c.admit_date = b.fst_dt ); 
          


-----------------------------------
------------LVL 6
-------------------------------------                                                    
                                       

drop table if exists dev.optz_covid_lvl_6;
                                                                                                                                                                                      
select a.patid 
  into dev.optz_covid_lvl_6
  from dev.optz_covid_allcovid_inp a
  join optum_zip.medical b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_zip.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where (( b.proc_cd in ( '94660', '94662', '94779' ) 
        or b.rvnu_cd in ('0270','0175','0998','0272')))
       and b.fst_dt between c.admit_date and c.disch_date
      and b.fst_dt between '2020-01-01' and '2020-12-31' ;
                        


-----------------------------------
------------LVL 7
-------------------------------------
        
drop table if exists dev.optz_covid_lvl_7;  

			--- CPT and Rev codes
select a.patid 
  into dev.optz_covid_lvl_7
  from dev.optz_covid_allcovid_inp a
  join optum_zip.medical b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_zip.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where (b.proc_cd in ( '94002', '94003', '94004', '94005','31500') 
 										or b.rvnu_cd = '0410')   
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31' ;
 	
 		
 		---ICD procedure table
insert into dev.optz_covid_lvl_7 (patid)
select a.patid 
  from dev.optz_covid_allcovid_inp a
  join optum_zip."procedure" b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
  join optum_zip.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where ( b."proc" in ( '5A1955Z', '5A1935Z', '5A1945Z' ) 
              or b."proc" like '5A093%' 
              or b."proc" like '5A094%' 
              or b."proc" like '5A095%' )  
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31' ; 
 		

-----------------------------------
------------LVL 8
-------------------------------------          

drop table if exists dev.optz_covid_lvl_8;   

--- renal CPT 
select a.patid 
  into dev.optz_covid_lvl_8
  from dev.optz_covid_allcovid_inp a
  join optum_zip.medical b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_zip.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where b.rvnu_cd between '0800' and '0809'    
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31' ; 
 		
 	
--renal ICD proc codes
insert into dev.optz_covid_lvl_8 (patid)
select a.patid 
  from dev.optz_covid_allcovid_inp a
  join optum_zip."procedure" b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_zip.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where b."proc" = '5A1D00Z' 
          or b."proc" = '5A1D60Z' 
          or b."proc" like '3E1M39Z'  
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31' ; 
 		
 	
--- ECMO CPT 
insert into dev.optz_covid_lvl_8 (patid)
select a.patid 
  from dev.optz_covid_allcovid_inp a
  join optum_zip.medical b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_zip.confinement c 
 				on a.patid = c.patid
 					and c."year" = 2020
 where b.proc_cd between '33946' and '33959' 
                or b.proc_cd between '33962' and '33966'
                or b.proc_cd between '33984' and '33989'
                or b.proc_cd = '33969'
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31';  		
 		
 	
--ECMO ICD proc codes
insert into dev.optz_covid_lvl_8 (patid)
select a.patid 
  from dev.optz_covid_allcovid_inp a
  join optum_zip."procedure" b 
         on a.patid = b.patid 
         and a.clmid = b.clmid 
 join optum_zip.confinement c 
	 				on a.patid = c.patid
	 					and c."year" = 2020
 where b."proc" = '5A1522F' 
          or b."proc" = '5A1522G' 
          or b."proc" like '5A1522H'  
 			and b.fst_dt between c.admit_date and c.disch_date
 		and b.fst_dt between '2020-01-01' and '2020-12-31';  		
 		
 select count(distinct patid) from dev.optz_covid_lvl_8;   


--------------------------------------------
--------Level 9
--------------------------------------------    


/*drop table if exists dev.optz_covid_lvl_9;

select a.patid
into   dev.optz_covid_lvl_9
from   dev.optz_covid_allcovid_inp a
			 join optum_zip.medical b
			 	 on a.clmid = b.clmid 
       join optum_zip.mbrwdeath c
         on a.patid = c.patid 
        join optum_zip.confinement d
	 				on a.patid = d.patid
	 					and d."year" = 2020
where  d.dstatus = '00'
   and b.fst_dt between '2020-01-01' and '2020-12-31'
       and Date_trunc('month', b.lst_dt) = Date_trunc('month', c.death_ym)
       and b.fst_dt between d.admit_date and d.disch_date;*/
 
--------------------------------------------
--------Severity
--------------------------------------------             
             
 
drop table if exists dev.optz_covid_severity;
   
select distinct patid, max(b.uth_member_id) as uth_member_id, Max(lvl) as severity 
into dev.optz_covid_severity
from   (select distinct patid,1 as lvl 
        from   dev.optz_covid_lvl_1
        union 
        select distinct patid,2 as lvl 
        from   dev.optz_covid_lvl_2
        union 
        select distinct patid,3 as lvl 
        from   dev.optz_covid_lvl_3
        union 
        select distinct patid,4 as lvl 
        from   dev.optz_covid_lvl_4
        union 
        select distinct patid,5 as lvl 
        from   dev.optz_covid_allcovid_inp
        union 
        select distinct patid,6 as lvl 
        from   dev.optz_covid_lvl_6
        union 
        select distinct patid,7 as lvl 
        from   dev.optz_covid_lvl_7
        union 
        select distinct patid,8 as lvl 
        from   dev.optz_covid_lvl_8) a
join data_warehouse.dim_uth_member_id b
on trim(a.patid::text) = trim(b.member_id_src)
group by 1 ;

select count(distinct patid) from dev.optz_covid_severity; -- 709,846
select count(distinct uth_member_id) from dev.optz_covid_severity; -- 709815
select * from dev.optz_covid_severity where uth_member_id is null;

select severity, count(*) from dev.optz_covid_severity group by severity;

select patid, severity from dev.optz_covid_severity where severity > 4;

drop table if exists dev.optz_covid_severity_age;









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
         into dev.optz_covid_severity_age
         from dev.optz_covid_severity a
         join data_warehouse.member_enrollment_yearly e on a.uth_member_id = e.uth_member_id
         ;
     

       
with ages as (
        select patid, max(yrdob) as dob
        from optum_zip.mbr_enroll_r
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
         into dev.optz_covid_severity_age
         from dev.optz_covid_severity a
         join ages b on a.patid = b.patid 
         ;
        
        
 select severity, count(*) from dev.optz_covid_severity_age group by severity;

 select age_group, count(*) from dev.optz_covid_severity_age group by age_group order by age_group;

select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 1 group by age_group order by age_group;
        
select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 2 group by age_group order by age_group;  
        
select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 3 group by age_group order by age_group;         
        
select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 4 group by age_group order by age_group;            

select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 5 group by age_group order by age_group;   

select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 6 group by age_group order by age_group; 

select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 7 group by age_group order by age_group; 

select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 8 group by age_group order by age_group; 

select age_group, count(*) from dev.optz_covid_severity_age 
where severity = 9 group by age_group order by age_group; 


    select * from dev.optz_covid_severity_age;
        
        --704516
         select count(distinct a.uth_member_id)
         from dev.optz_covid_severity a
         join data_warehouse.member_enrollment_yearly e on a.uth_member_id = e.uth_member_id 
         where e."year" = 2021;
         
        select count(*) from dev.optz_covid_severity_age;
