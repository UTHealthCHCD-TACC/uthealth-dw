drop table dev.wc_bench_trv_members 

select a.uth_member_id, b.member_id_src, a.total_enrolled_months , bus_cd, a.gender_cd , year ,
      case when employee_status in ('3','4','5') and bus_cd = 'MCR' then 'Retiree MP'
           when employee_status in ('3','4','5') and bus_cd = 'COM' then 'Retiree MS'
           when employee_status = '1' then 'Active FT'
           when employee_status = '6' then 'Cobra'
        	                          else 'Unknown' end as emp_status,
   case when age_derived BETWEEN 0 AND 19 THEN 1
            when age_derived BETWEEN 20 AND 34 THEN 2
                                                when age_derived BETWEEN 35 AND 44 THEN 3
                                                when age_derived BETWEEN 45 AND 54 THEN 4
                                                when age_derived BETWEEN 55 AND 64 THEN 5
                                                when age_derived BETWEEN 65 AND 74 THEN 6
                                                ELSE 7 end as age_group
into dev.wc_bench_trv_members 
from data_warehouse.member_enrollment_yearly a
  join data_warehouse.dim_uth_member_id b 
    on a.uth_member_id = b.uth_member_id 
where year between 2016 and 2019
  and a.data_source = 'truv' 
 and state = 'TX'
;
 


select distinct member_id_src, a.emprel , b.year 
into dev.wc_temp_emprel
from truven.ccaet a 
 join dev.wc_bench_trv_members b 
    on b.member_id_src = a.enrolid::text 
   and a.year = b.year 
;

alter table dev.wc_bench_trv_members add column ee_dep char(1) default '1';

update dev.wc_bench_trv_members a set ee_dep = case when emprel = 1 then '1' else '2' end 
--from dev.wc_bench_trv_members a
from dev.wc_temp_emprel b
where a.member_id_src = b.member_id_src 
  and a.year = b.year 
;

drop table dev.wc_temp_emprel;

 ---- EE STATUS CALCS *******************************
                                         
 --unique # employee status                                            
select count(distinct uth_member_id) as mems, sum(total_enrolled_months) as MM, year , emp_status
 from dev.wc_bench_trv_members
 group by year , emp_status
order by year, emp_status 

---medical  by EE status
select count(uth_claim_id), '' as chg, sum(a.total_allowed_amount) as alw,  a.year, emp_status
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by a.year, emp_status
order by a.year, emp_status
;

---rx  by EE status
select count(uth_rx_claim_id ), sum(a.total_allowed_amount) as alw,  a.year, emp_status
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by a.year, emp_status
order by a.year, emp_status
;

-----------
     ---- Active vs Medicare Primary Counts                                          
--unique # by age group
select  count(distinct uth_member_id) as mems, sum(total_enrolled_months) as MM, bus_cd , year, age_group 
 from dev.wc_bench_trv_members
 group by bus_cd , year, age_group 
order by year, bus_cd , age_group 
;
                                                                                      
---medical  by status and age group
select count(uth_claim_id), '' as chg, sum(a.total_allowed_amount) as alw,  b.year, substring(emp_status,1,5), age_group  
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
 where emp_status in ('Active FT','Retiree MP')
 group by emp_status, b.year, age_group 
order by b.year, emp_status, age_group 
;


---RX by status and by status + age grp  
select count(uth_rx_claim_id), sum(a.total_allowed_amount ) as rxtot , b.year, substring(emp_status,1,5), age_group 
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
 where emp_status in ('Active FT','Retiree MP')
 group by emp_status, b.year, age_group 
order by b.year, emp_status, age_group 
;



----++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
---- PMPY of over $100,000 allowed rx+med---------------------------------------------------------------
drop table dev.wc_bench_trv_pmpy 

--*PMPY
select uth_member_id, sum(chg) as alw, year 
into dev.wc_bench_trv_pmpy
from 
(
	select b.uth_member_id, sum(total_allowed_amount) as alw, b.year
	from data_warehouse.pharmacy_claims a
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
	group by b.uth_member_id, b.year 
union 
	select b.uth_member_id, sum(total_allowed_amount) as alw, b.year 
	from data_warehouse.claim_header a
	   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
	group by b.uth_member_id, b.year 
) inr 
group by uth_member_id, year 
having sum(alw) >= 100000


--*PMPY member counts
select count(a.uth_member_id), sum(a.total_enrolled_months) as mm, a.year, emp_status
from dev.wc_bench_trv_members a 
  join  dev.wc_bench_trv_pmpy b 
  on a.uth_member_id = b.uth_member_id
 and a.year = b.year 
group by a.year, emp_status
order by a.year, emp_status
;


---pmpy med
select count(uth_claim_id) as clm, ''as chg, sum(a.total_allowed_amount) as alw,a.year, emp_status
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
    and b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
group by a.year, emp_status
order by a.year, emp_status
;


select count(uth_rx_claim_id), sum(a.total_allowed_amount ) as rxtot , a.year,emp_status 
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
    and b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by  a.year,emp_status   
order by  a.year,emp_status 
; 	


 --unique # employee status                                            
select count(uth_member_id), count(distinct uth_member_id), year , emp_status
 from dev.wc_bench_trv_members
 where uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
 group by year , emp_status
order by year, emp_status 

--mm count
select count(*), emp_status, a.year
from data_warehouse.member_enrollment_monthly a 
  join dev.wc_bench_trv_members b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year
   and b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
group by emp_status, a.year
order by a.year, emp_status
          

------ admissions

alter table dev.wc_bench_trv_members add column myenrolid text;

update dev.wc_bench_trv_members a set myenrolid = member_id_src
from data_warehouse.dim_uth_member_id b 
where a.uth_member_id = b.uth_member_id 
  and b.data_source = 'truv'
 ;


select * from truven.ccaef where enrolid = 3967816901.0


---------------------------------------------------------------------------------------------------------

------------***************************Utilization*****************************************************************

-----------------------------------------------------------------------------------------------------

----gender+agegrp+status
select  count(uth_member_id) as mem_cnt, sum(total_enrolled_months) as mm, year, emp_status 
from dev.wc_bench_trv_members a
group by year, emp_status 
order by year, emp_status 


----Do Active vs FT first across all rows

drop table dev.wc_bench_truv_inpatient 

--inpatient - get claims from detail
select  distinct uth_claim_id, b.uth_member_id , b.emp_status 
into dev.wc_bench_truv_inpatient 
from data_warehouse.claim_detail a 
  join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id 
    and a.year = b.year 
where a.bill_type_inst = '1' 
 and a.bill_type_class = '1'
 
 
---inpatient admissions for spreadsheet 
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, a.year, c.emp_status 
from data_warehouse.claim_header a 
   join dev.wc_bench_truv_inpatient c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
group by a.year, c.emp_status 
order by a.year, c.emp_status 
;


select count(*), year, table_id_src 
from data_warehouse.claim_detail cd 
where data_source = 'truv' and year between 2016 and 2019
and bill_type_inst = '1'
group by year, table_id_src 
order by "year" , table_id_src 


select year, substring(billtyp,1,2), count(*)
from truven.mdcrf where year between 2016 and 2019
group by year, substring(billtyp,1,2) 
order by year, substring(billtyp,1,2) 



--ED Visits
drop table dev.wc_bench_trv_ER;

select b.uth_member_id, a.from_date_of_service , min(uth_claim_id) as uth_claim_id 
into dev.wc_bench_trv_ER
from data_warehouse.claim_detail a 
  join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id 
    and a.year = b.year 
    and a.revenue_cd in ('450','451','452','456','459','0450','0451','0452','0456','0459')
group by b.uth_member_id, a.from_date_of_service;

---ed for spreadsheet
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, a.year, b.emp_status 
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_ER c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
group by a.year, emp_status 
order by a.year, emp_status 
;

--30 day readmissions
drop table  dev.wc_trv_readmissions;

select a.uth_member_id, a.admission_id_src,  b.year, b.age_group , b.gender_cd ,b.emp_status , min(a.from_date_of_service) as fst_dt
into dev.wc_trv_readmissions
from data_warehouse.claim_detail a 
  join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id 
    and a.year = b.year 
where a.bill_type_inst = '1' 
 and a.bill_type_class = '1'
 group by a.uth_member_id, a.admission_id_src,  b.year, b.age_group , b.gender_cd, b.emp_status 
;


select count(a.uth_claim_id ), year, emp_status 
from data_warehouse.claim_header a 
  join dev.wc_bench_truv_inpatient b 
     on a.uth_member_id = b.uth_member_id 
    and a.uth_claim_id = b.uth_claim_id 
where exists ( select 1 from data_warehouse.claim_header c 
                        join dev.wc_bench_truv_inpatient d 
                           on c.uth_member_id = d.uth_member_id 
                          and c.uth_claim_id = d.uth_claim_id
                          and c.from_date_of_service > a.from_date_of_service
						  and c.from_date_of_service < a.from_date_of_service + interval'30 days' 
						where b.uth_member_id = d.uth_member_id )
group by year,  emp_status 
order by year,  emp_status 
;




------------------------ HCC -------------------- PMPY 
--- *****************************************************************************************
--inpatient
select count(distinct admission_id_src ), sum(total_allowed_amount ), a.year, b.emp_status 
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
where substring(bill_type,1,2) = '11' 
  and b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
group by a.year, emp_status 
order by a.year, emp_status 
;


--ED Visits
select count(distinct a.uth_claim_id::text || from_date_of_service::text), count(a.uth_claim_id ), sum(a.allowed_amount ), 
       b.year , b.emp_status 
from data_warehouse.claim_detail a
   join dev.wc_bench_trv_members b 
     on b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
    and a.uth_member_id  = b.uth_member_id 
    and a.year = b.year
    and a.revenue_cd in ('450','451','452','456','459','0450','0451','0452','0456','0459')
group by b.year , b.emp_status 
order by b.year , b.emp_status 
;


--30 day readmissions
drop table  dev.wc_trv_readmissions;

select a.uth_member_id, a.admission_id_src,  b.year, b.age_group , b.gender ,b.emp_status , min(a.from_date_of_service) as fst_dt
into dev.wc_trv_readmissions
from data_warehouse.claim_header a 
 join dev.wc_bench_trv_members b 
    on b.uth_member_id = a.uth_member_id 
   and b.year = a.year 
where substring(bill_type,1,2) = '11' 
 group by a.uth_member_id, a.admission_id_src,  b.year, b.age_group , b.gender, b.emp_status 
;


select count(distinct admission_id_src), year, emp_status 
from dev.wc_trv_readmissions a 
where a.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy ) 
and exists ( select 1 from dev.wc_trv_readmissions b 
						where a.uth_member_id = b.uth_member_id 
						 and b.fst_dt > a.fst_dt 
						and b.fst_dt < a.fst_dt + interval'30 days' )
group by year,  emp_status 
order by year,  emp_status 
;


------------------------------------------------***************************************************
----------------------------active / retired by age group Utilizations
---------------------------------------------------**********************************************

 --unique # employee status                                            
select count(uth_member_id), count(distinct uth_member_id), year, substring(emp_status,1,5), age_group 
 from dev.wc_bench_trv_members
 where  emp_status in ('Active FT','Retiree MP', 'Retiree MS')
   -- and gender = 'F'
group by year, substring(emp_status,1,5), age_group   
order by year, substring(emp_status,1,5), age_group 
; 	

--mm count
select count(*), a.year, substring(emp_status,1,5), age_group 
from data_warehouse.member_enrollment_monthly a 
  join dev.wc_bench_trv_members b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year
 where  emp_status in ('Active FT','Retiree MP', 'Retiree MS')
    and gender = 'F'
group by a.year, substring(emp_status,1,5), age_group   
order by a.year, substring(emp_status,1,5), age_group 
; 	

--inpatient
select count(distinct admission_id_src ), sum(total_allowed_amount ),  a.year, substring(emp_status,1,5), age_group   
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
     and b.gender = 'F'
where substring(bill_type,1,2) = '11' 
    and b.emp_status in ('Active FT','Retiree MP', 'Retiree MS')
group by a.year, substring(emp_status,1,5), age_group   
order by a.year, substring(emp_status,1,5), age_group 
; 	


--ED Visits
select count(distinct a.uth_claim_id::text || from_date_of_service::text), sum(a.allowed_amount ), 
       a.year, substring(emp_status,1,5), age_group 
from data_warehouse.claim_detail a
   join dev.wc_bench_trv_members b 
    on a.uth_member_id  = b.uth_member_id 
    and a.year = b.year
    and a.revenue_cd in ('450','451','452','456','459','0450','0451','0452','0456','0459')
    and b.gender = 'F'
    and b.emp_status in ('Active FT','Retiree MP', 'Retiree MS')
group by a.year, substring(emp_status,1,5), age_group   
order by a.year, substring(emp_status,1,5), age_group 
;




---30 day readmits
select count(distinct admission_id_src), a.year, substring(emp_status,1,5), age_group 
from dev.wc_trv_readmissions a 
where  exists ( select 1 from dev.wc_trv_readmissions b 
						where a.uth_member_id = b.uth_member_id 
						 and a.emp_status = b.emp_status 
						 and b.fst_dt > a.fst_dt 
						and b.fst_dt < a.fst_dt + interval'30 days' )
    and a.emp_status in ('Active FT','Retiree MP', 'Retiree MS')
   -- and a.gender = 'F'
group by a.year, substring(emp_status,1,5), age_group   
order by a.year, substring(emp_status,1,5), age_group 
;


-----truven bill type updates - needs to be moved to claim load scripts -------
---************************************************

update data_warehouse.claim_header set admission_id_src = trunc(admission_id_src::numeric,0)::text,
                                       member_id_src = trunc(member_id_src::numeric,0)::text,
                                       claim_id_src = trunc(claim_id_src::numeric,0)::text
 where data_source = 'truv' 
 ;


select count (distinct caseid), year 
from truven.ccaef 
group by year ;

select count(distinct uth_claim_id), year 
from data_warehouse.claim_header 
where data_source = 'truv' and admission_id_src is not null 
group by year; 

select count(distinct admission_id_src ), year 
from data_warehouse.claim_header 
where data_source = 'truv' and admission_id_src is not null 
group by year; 


drop table dev.wc_temp_ccaef;

select min(billtyp) as bt, caseid, enrolid, f."year" as yr 
into dev.wc_temp_ccaef
from truven.ccaef f 
where caseid is not null
group by caseid, enrolid, year ;

select min(billtyp) as bt, caseid, enrolid, f."year" as yr 
into dev.wc_temp_mdcrf
from truven.mdcrf f 
where caseid is not null
group by caseid, enrolid, year ;


update data_warehouse.claim_header set bill_type = bt
from dev.wc_temp_ccaef
where caseid::text = admission_id_src 
  and enrolid::text = member_id_src 
  and data_source = 'truv'
  and year = yr 
  ;
 
 update data_warehouse.claim_header set bill_type = bt
from dev.wc_temp_mdcrf
where trunc(caseid,0)::text = admission_id_src 
  and trunc(enrolid,0)::text = member_id_src 
  and data_source = 'truv'
  and year = yr 
  ;
 
 
 select enrolid::text 
 from dev.wc_temp_mdcrf;
 
 
update data_warehouse.claim_header set bill_type = null where data_source = 'truv';
