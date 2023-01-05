
-----------------------------------
-----All COVID Claims DW
------------------------------------


-----------------------------------
------------LVL 1
-------------------------------------


drop table if exists dev.dw_covid_lvl_1;

select uth_member_id 
into   dev.dw_covid_lvl_1
from   data_warehouse.claim_diag
where  from_date_of_service between '2020-01-01' and '2020-12-31' 
      and ( diag_cd = 'Z8616' 
        or diag_cd = 'U08' 
        or diag_cd = 'U09' 
        or diag_cd = 'B948'); 

select count(distinct uth_member_id) from  dev.dw_covid_lvl_1;


-----------------------------------
---------All Confirmed Cases
-------------------------------------       
         

drop table if exists dev.dw_covid_all_confirmed_work1; 

select a.uth_member_id, 
       a.uth_claim_id,
       a.diag_cd,
       a.from_date_of_service 
into   dev.dw_covid_all_confirmed_work1
from   data_warehouse.claim_diag a
				 where from_date_of_service between '2020-01-01' and '2020-12-31' 
         and (a.diag_cd in ('U071','U072','U10','J1282','J208','J988','B9729')
              or a.diag_cd like 'J22%' 
              or a.diag_cd like 'J40%' 
              or a.diag_cd like 'J80%') 
              ;

drop table if exists dev.dw_covid_all_confirmed_work2; 


select a.uth_member_id, 
       a.uth_claim_id 
into   dev.dw_covid_all_confirmed_work2
from   dev.dw_covid_all_confirmed_work1 a
join   dev.dw_covid_all_confirmed_work1 b
         on a.uth_member_id = b.uth_member_id and 
         a.uth_claim_id = b.uth_claim_id  
				 where a.diag_cd = 'U071'
              or a.diag_cd = 'U072'
              or a.diag_cd = 'U10'
              or (((a.diag_cd in ('J1282','J208','J988') and b.diag_cd = 'B9729')
              or (a.diag_cd like 'J22%' and b.diag_cd = 'B9729')
              or (a.diag_cd like 'J40%' and b.diag_cd = 'B9729')
              or (a.diag_cd like 'J80%' and b.diag_cd = 'B9729')) 
              and a.from_date_of_service between '2020-01-01' and '2020-04-01')
						;


drop table if exists dev.dw_covid_all_confirmed;

select distinct uth_member_id, 
       uth_claim_id 
into   dev.dw_covid_all_confirmed
from   dev.dw_covid_all_confirmed_work2
;
-----------------------------------
-- all inpatient
-----------------------------------
					
drop table if exists dev.dw_covid_inpatient;

select distinct a.uth_member_id, 
       a.uth_claim_id, 
       b.admit_date,
       b.discharge_date
into   dev.dw_covid_inpatient 
from   dev.dw_covid_all_confirmed a 
       join data_warehouse.claim_detail b 
         on a.uth_member_id = b.uth_member_id 
            and a.uth_claim_id = b.uth_claim_id 
where  b.bill_type_inst in ('1','2')
       and b.bill_type_class in ('1','2','5','6','7','8')
      		and admit_date is not null and discharge_date is not null;  
      	
   	

-----------------------------------
---------Level 2 or 3 inclusion / exclusion
-------------------------------------  

drop table if exists dev.dw_covid_include_exclude;

select distinct uth_member_id, uth_claim_id
into   dev.dw_covid_include_exclude
from   data_warehouse.claim_diag
where   from_date_of_service between '2020-01-01' and '2020-12-31'  
			  and
        (diag_cd = 'J1289' 
        or diag_cd = 'J40' 
        or diag_cd = 'J22' 
        or diag_cd = 'J988' 
        or diag_cd = 'J80' 
        or diag_cd = 'R05' 
        or diag_cd = 'R0602' 
        or diag_cd = 'R0603' 
        or diag_cd = 'R509');  
      
-----------------------------------
---------Level 2 
-------------------------------------  

drop table if exists dev.dw_covid_lvl_2;

--level 2 without level 3 exclusions
select  a.uth_member_id
into    dev.dw_covid_lvl_2
from    dev.dw_covid_all_confirmed a
        left outer join dev.dw_covid_include_exclude b 
						 on a.uth_claim_id = b.uth_claim_id 
						 and a.uth_member_id = b.uth_member_id 
	where b.uth_claim_id is null;


-----------------------------------
------------LVL 3
-------------------------------------   


drop table if exists dev.dw_covid_lvl_3;
 
-- level 2 with level 3 dx
select  a.uth_member_id
into    dev.dw_covid_lvl_3
from    dev.dw_covid_all_confirmed a
				join dev.dw_covid_include_exclude b 
				on a.uth_claim_id = b.uth_claim_id 
				and a.uth_member_id = b.uth_member_id 
;
    
-----------------------------------
------------All hospitalizations (LVL 5)
-------------------------------------  

drop table if exists dev.dw_covid_lvl_5;
 
select distinct uth_member_id  
into dev.dw_covid_lvl_5
from dev.dw_covid_inpatient;   

      
-----------------------------------
------------All hospitalizations (LVL 4)
-------------------------------------        
      
drop table if exists dev.dw_covid_lvl_4;
  
select distinct a.uth_member_id
into   dev.dw_covid_lvl_4
from   dev.dw_covid_all_confirmed a 
       join data_warehouse.claim_detail b 
         on a.uth_member_id = b.uth_member_id 
            and a.uth_claim_id = b.uth_claim_id 
where  b.revenue_cd between '0450' and '0459' 
   and b.from_date_of_service between '2020-01-01' and '2020-12-31'
   and not exists (select 1 
                       from   data_warehouse.claim_detail d
                       where  a.uth_member_id = d.uth_member_id 
                              and d.admit_date = b.from_date_of_service) 
                          group by a.uth_member_id ; 


           
-----------------------------------
------------LVL 6
-------------------------------------                                                    
                                       

drop table if exists dev.dw_covid_lvl_6;
                                                                                                                                                                                                                                      
select distinct a.uth_member_id
  into dev.dw_covid_lvl_6 
   from data_warehouse.claim_detail a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
         and a.uth_claim_id = b.uth_claim_id 
 where (( a.cpt_hcpcs_cd in ( '94660', '94662', '94779' ) 
        or a.revenue_cd in ('0270','0175','0998','0272')))
        and a.bill_type_inst in ('1','2')
       and a.bill_type_class in ('1','2','5','6','7','8')
      and a.admit_date is not null and a.discharge_date is not null
     ;  
        
 

-----------------------------------
------------LVL 7
-------------------------------------
        
drop table if exists dev.dw_covid_lvl_7;  


select distinct a.uth_member_id
  into dev.dw_covid_lvl_7
  from data_warehouse.claim_detail a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
         and a.uth_claim_id = b.uth_claim_id 
where (a.cpt_hcpcs_cd in ( '94002', '94003', '94004', '94005','31500') or revenue_cd = '0410') 
        and a.bill_type_inst in ('1','2')
       and a.bill_type_class in ('1','2','5','6','7','8')
      and a.admit_date is not null and a.discharge_date is not null
     ;  

                 
insert into dev.dw_covid_lvl_7 (uth_member_id)
select a.uth_member_id 
  from data_warehouse.claim_icd_proc a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
          and a.uth_claim_id = b.uth_claim_id 
 where ( a.proc_cd in ( '5A1955Z', '5A1935Z', '5A1945Z' ) 
              or a.proc_cd like '5A093%' 
              or a.proc_cd like '5A094%' 
              or a.proc_cd like '5A095%' ) 
       and a.from_date_of_service between b.admit_date and b.discharge_date
       and a.from_date_of_service between '2020-01-01' and '2020-12-31';  



-----------------------------------
------------LVL 8
-------------------------------------          


drop table if exists dev.dw_covid_lvl_8;             
--drop table if exists dev.dw_covid_lvl_8_renal;

-----------------------renal---------------------------------------

select a.uth_member_id 
into   dev.dw_covid_lvl_8 
from data_warehouse.claim_detail a
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
        	 and a.uth_claim_id = b.uth_claim_id 
where  (( a.revenue_cd between '0800' and '0809' )) 
        and a.bill_type_inst in ('1','2')
       and a.bill_type_class in ('1','2','5','6','7','8')
      and a.admit_date is not null and a.discharge_date is not null
     ;  



insert into dev.dw_covid_lvl_8 (uth_member_id) 
select a.uth_member_id 
 from data_warehouse.claim_icd_proc a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
          and a.uth_claim_id = b.uth_claim_id 
where  ( a.proc_cd = '5A1D00Z' 
          or a.proc_cd = '5A1D60Z' 
          or a.proc_cd like '3E1M39Z' ) 
       and a.from_date_of_service between b.admit_date and b.discharge_date
       and a.from_date_of_service between '2020-01-01' and '2020-12-31';    


  
-----------------------ecmo---------------------------------------  
 
insert  into dev.dw_covid_lvl_8 (uth_member_id)     
select a.uth_member_id
from data_warehouse.claim_detail a
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
  join  dev.dw_covid_inpatient c
         on a.uth_member_id = c.uth_member_id 
where (a.cpt_hcpcs_cd between '33946' and '33959' 
                or ( a.cpt_hcpcs_cd between '33962' and '33966' ) 
                or ( a.cpt_hcpcs_cd between '33984' and '33989' ) 
                or ( a.cpt_hcpcs_cd = '33969' )) 
        and a.admit_date is not null and a.discharge_date is not null 
       and a.from_date_of_service between c.admit_date and c.discharge_date
      and a.data_source = 'optd'
     ;  --163           

insert  into dev.dw_covid_lvl_8 (uth_member_id)
select a.uth_member_id 
from data_warehouse.claim_icd_proc a 
  join  dev.dw_covid_inpatient b
         on a.uth_member_id = b.uth_member_id 
          and a.uth_claim_id = b.uth_claim_id 
 where ( a.proc_cd = '5A1522F' 
          or ( a.proc_cd = '5A1522G' ) 
          or ( a.proc_cd = '5A1522H' ) ) 
       and a.from_date_of_service between b.admit_date and b.discharge_date 
       and a.from_date_of_service between '2020-01-01' and '2020-12-31';                     
          


--------------------------------------------
--------Level 9
--------------------------------------------    


drop table if exists dev.dw_covid_lvl_9;

select distinct a.uth_member_id 
  into  dev.dw_covid_lvl_9
  from data_warehouse.claim_detail a
  join dev.dw_covid_inpatient b
				on a.uth_member_id = b.uth_member_id 
				and a.uth_claim_id = b.uth_claim_id 
 where a.discharge_status in ( '20', '40', '41', '42' )
			and a.admit_date between '2020-01-01' and '2020-12-31'; 

insert  into dev.dw_covid_lvl_9 (uth_member_id)
 select distinct a.uth_member_id 
  from data_warehouse.claim_detail a
  join dev.dw_covid_inpatient b
				on a.uth_member_id = b.uth_member_id 
				and a.uth_claim_id = b.uth_claim_id 
 join  data_warehouse.member_enrollment_yearly c
 				on a.uth_member_id = c.uth_member_id 
 					and a.year = c."year" 
 where a.discharge_status = '00'
      and Date_trunc('month', a.to_date_of_service) = Date_trunc('month', c.death_date)
			and a.admit_date between '2020-01-01' and '2020-12-31'; 
		
		
insert  into dev.dw_covid_lvl_9 (uth_member_id)
select distinct a.uth_member_id 
  from data_warehouse.claim_detail a
  join dev.dw_covid_inpatient b
				on a.uth_member_id = b.uth_member_id 
				and a.uth_claim_id = b.uth_claim_id 
 where a.data_source = 'truv' and a.discharge_status is null ;


 
--------------------------------------------
--------Severity
--------------------------------------------             
             
 
drop table if exists dev.dw_covid_severity;
   
select a.uth_member_id, Max(lvl) as severity, max(data_source) as data_source 
into dev.dw_covid_severity
from
(select distinct uth_member_id,1 as lvl 
        from   dev.dw_covid_lvl_1
        union 
        select distinct uth_member_id,2 as lvl 
        from   dev.dw_covid_lvl_2
        union 
        select distinct uth_member_id,3 as lvl 
        from   dev.dw_covid_lvl_3
        union 
        select distinct uth_member_id,4 as lvl 
        from   dev.dw_covid_lvl_4
        union 
        select distinct uth_member_id,5 as lvl 
        from   dev.dw_covid_lvl_5 
        union 
        select distinct uth_member_id,6 as lvl 
        from   dev.dw_covid_lvl_6
        union 
        select distinct uth_member_id,7 as lvl 
        from   dev.dw_covid_lvl_7
        union 
        select distinct uth_member_id,8 as lvl 
        from   dev.dw_covid_lvl_8
        union 
        select distinct uth_member_id,9 as lvl 
        from   dev.dw_covid_lvl_9) 
        a
join data_warehouse.dim_uth_member_id b
on a.uth_member_id = b.uth_member_id 
group  by a.uth_member_id ;



drop table if exists tableau.dw_severity_2020;


select a.uth_member_Id,
       a.severity,
       b.gender_cd,
       b.age_derived,
       b.plan_type,
       b.data_source,
       b."year",
       b.bus_cd,
       b.state 
  into tableau.dw_severity_2020
  from dev.dw_covid_severity a
  join data_warehouse.member_enrollment_yearly b 
  	on a.uth_member_id = b.uth_member_id 
 where year = 2020;

alter table tableau.dw_severity_2020 owner to uthealth_analyst;



select data_source, severity, COUNT(*) 
from tableau.dw_severity_2020
group by data_source, severity  order by data_source, severity;

select * from tableau.dw_severity_2020;


             


--cleanup 
/*
drop table if exists dev.dw_covid_severity;
drop table if exists dev.dw_covid_conf_alldx;
drop table if exists dev.dw_covid_conf_alldx_clms;
drop table if exists dev.dw_covid_lvl_1;
drop table if exists dev.dw_covid_lvl_2;
drop table if exists dev.dw_covid_lvl_2_1;
drop table if exists dev.dw_covid_lvl_2_2;
drop table if exists dev.dw_covid_lvl_2_3;
drop table if exists dev.dw_covid_lvl_3;
drop table if exists dev.dw_covid_lvl_3_1;
drop table if exists dev.dw_covid_lvl_4;
drop table if exists dev.dw_covid_lvl_5;
drop table if exists dev.dw_covid_lvl_6;
drop table if exists dev.dw_covid_lvl_7;
drop table if exists dev.dw_covid_lvl_8;
drop table if exists dev.dw_covid_lvl_8_renal;
drop table if exists dev.dw_covid_lvl_8;

*/
