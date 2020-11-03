drop table dev.wc_bench_trv_members 

select a.uth_member_id, b.member_id_src, a.total_enrolled_months , bus_cd, a.gender_cd , year ,
            case when age_derived between 0 and 19 then 1 
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


 ---- EE STATUS CALCS *******************************
                                         
 --unique # employee status                                            
select count(distinct uth_member_id) as mems, sum(total_enrolled_months) as MM, year , bus_cd 
 from dev.wc_bench_trv_members
 group by year , bus_cd 
order by year, bus_cd 

---medical  by EE status
select count(uth_claim_id), '' as chg, sum(a.total_allowed_amount) as alw,  a.year, bus_cd 
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by a.year, bus_cd 
order by a.year, bus_cd 
;

---rx  by EE status
select count(uth_rx_claim_id ), sum(a.total_allowed_amount) as alw,  a.year, bus_cd
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by a.year, bus_cd
order by a.year, bus_cd
;

----++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
---- PMPY of over $100,000 allowed rx+med---------------------------------------------------------------
drop table dev.wc_bench_trv_pmpy 

--*PMPY
select uth_member_id, sum(alw) as alw, year 
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
select count(a.uth_member_id), sum(a.total_enrolled_months) as mm, a.year, bus_Cd
from dev.wc_bench_trv_members a 
  join  dev.wc_bench_trv_pmpy b 
  on a.uth_member_id = b.uth_member_id
 and a.year = b.year 
group by a.year, bus_cd
order by a.year, bus_Cd
;


---pmpy med
select count(uth_claim_id) as clm, ''as chg, sum(a.total_allowed_amount) as alw,a.year, bus_cd
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
    and b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
group by a.year, bus_cd
order by a.year, bus_cd
;

--rx pmpy
select count(uth_rx_claim_id), sum(a.total_allowed_amount ) as rxtot , a.year,bus_cd 
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
    and b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by  a.year,bus_cd   
order by  a.year,bus_cd 
; 	


-----------
     ---- Active vs Retiree by Age Group                                         
--unique # by age group
select  count(distinct uth_member_id) as mems, sum(total_enrolled_months) as MM, bus_cd , year, age_group 
 from dev.wc_bench_trv_members
 group by bus_cd , year, age_group 
order by year, bus_cd , age_group 
;
                                                                                      
---medical  by status and age group
select count(uth_claim_id), '' as chg, sum(a.total_allowed_amount) as alw,  b.year, bus_cd, age_group  
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
 group by bus_cd, b.year, age_group 
order by b.year, bus_cd, age_group 
;

---RX by status and by status + age grp  
select count(uth_rx_claim_id), sum(a.total_allowed_amount ) as rxtot , b.year, bus_cd, age_group 
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
 group by bus_cd, b.year, age_group 
order by b.year, bus_cd, age_group 
;

---------------------------------------------------------------------------------------------------------
------------***************************Utilization*****************************************************************
-----------------------------------------------------------------------------------------------------

----gender+agegrp+status
select  year, bus_cd, count(uth_member_id) as mem_cnt, sum(total_enrolled_months) as mm, age_group, gender_cd
from dev.wc_bench_trv_members a
group by year, bus_cd, gender_cd , age_group 
order by year, gender_cd desc, bus_cd , age_group 


----Active vs Retiree
drop table dev.wc_bench_truv_inpatient 

--inpatient - get claims from detail
select  distinct uth_claim_id, b.uth_member_id , b.bus_cd , b.year , b.gender_cd , b.age_group 
into dev.wc_bench_truv_inpatient 
from data_warehouse.claim_detail a 
  join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id 
    and a.year = b.year 
where a.bill_type_inst = '1' 
 and a.bill_type_class = '1'
 
 
---inpatient admissions for spreadsheet 
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, c.year, c.bus_cd  
from data_warehouse.claim_header a 
   join dev.wc_bench_truv_inpatient c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id 
group by c.year, c.bus_cd 
order by c.year, c.bus_cd 
;


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
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, a.year, b.bus_cd 
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_ER c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
group by a.year,  b.bus_cd 
order by a.year,  b.bus_cd 
;

--30 day readmissions
select count(a.uth_claim_id ), a.year, bus_cd 
from data_warehouse.claim_header a 
  join dev.wc_bench_truv_inpatient  b 
     on a.uth_member_id = b.uth_member_id 
    and a.uth_claim_id = b.uth_claim_id 
where exists ( select 1 from data_warehouse.claim_header x 
                            join dev.wc_bench_truv_inpatient y 
                               on x.uth_member_id = y.uth_member_id
                              and x.uth_claim_id = y.uth_claim_id 
                        where x.uth_member_id = b.uth_member_id 
                          and x.uth_claim_id <> b.uth_claim_id
                          and x.from_date_of_service > a.from_date_of_service
						  and x.from_date_of_service < a.from_date_of_service + interval'30 days'  )
group by a.year,  bus_cd
order by a.year,  bus_cd
;




------------------------ HCC -------------------- PMPY 
--- *****************************************************************************************
--inpatient
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, a.year, c.bus_cd  
from data_warehouse.claim_header a 
   join dev.wc_bench_truv_inpatient c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
where a.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )     
group by a.year, c.bus_cd 
order by a.year, c.bus_cd 
;

--ED Visits
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, a.year, b.bus_cd 
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_ER c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
where a.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
group by a.year,  b.bus_cd 
order by a.year,  b.bus_cd 
;


--30 day readmissions
select count(a.uth_claim_id ), a.year, bus_cd 
from data_warehouse.claim_header a 
  join dev.wc_bench_truv_inpatient  b 
     on a.uth_member_id = b.uth_member_id 
    and a.uth_claim_id = b.uth_claim_id 
where a.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
 and exists ( select 1 from data_warehouse.claim_header x 
                            join dev.wc_bench_truv_inpatient y 
                               on x.uth_member_id = y.uth_member_id
                              and x.uth_claim_id = y.uth_claim_id 
                        where x.uth_member_id = b.uth_member_id 
                          and x.uth_claim_id <> b.uth_claim_id
                          and x.from_date_of_service > a.from_date_of_service
						  and x.from_date_of_service < a.from_date_of_service + interval'30 days'  )
group by a.year,  bus_cd
order by a.year,  bus_cd
;



------------------------------------------------***************************************************
----------------------------active / retired by age group Utilizations
---------------------------------------------------**********************************************

 ----inpatient by age group and active vs retiree                                         
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, c.year, c.bus_cd , c.age_group 
from data_warehouse.claim_header a 
   join dev.wc_bench_truv_inpatient c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id   
group by c.year, c.bus_cd , c.age_group 
order by c.year, c.bus_cd , c.age_group 
;

 ----inpatient by age group and active vs retiree   M and F                                      
select c.year, bus_cd, count(a.uth_claim_id ), sum(total_allowed_amount ) as alw,  c.age_group , c.gender_cd
from data_warehouse.claim_header a 
   join dev.wc_bench_truv_inpatient c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id   
group by c.year, c.bus_cd , c.age_group, c.gender_cd 
order by c.year,  gender_cd desc, bus_cd, c.age_group 
;
	

--ED Visits by age group 
select count(a.uth_claim_id ), sum(total_allowed_amount ) as alw, a.year, b.bus_cd , b.age_group
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_ER c 
     on c.uth_member_id = a.uth_member_id 
    and c.uth_claim_id = a.uth_claim_id 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
     and a.year = b.year 
   where b.gender_cd = 'F'
group by a.year,  b.bus_cd , b.age_group
order by a.year,  b.bus_cd , b.age_group
;


--30 day readmissions
select count(a.uth_claim_id ), b.year, bus_cd , b.age_group
from data_warehouse.claim_header a 
  join dev.wc_bench_truv_inpatient  b 
     on a.uth_member_id = b.uth_member_id 
    and a.uth_claim_id = b.uth_claim_id 
where b.gender_cd = 'F'
and a.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
 and exists ( select 1 from data_warehouse.claim_header x 
                            join dev.wc_bench_truv_inpatient y 
                               on x.uth_member_id = y.uth_member_id
                              and x.uth_claim_id = y.uth_claim_id 
                        where x.uth_member_id = b.uth_member_id 
                          and x.uth_claim_id <> b.uth_claim_id
                          and x.from_date_of_service > a.from_date_of_service
						  and x.from_date_of_service < a.from_date_of_service + interval'30 days'  )
group by b.year,  bus_cd, b.age_group
order by b.year,  bus_cd, b.age_group
;
