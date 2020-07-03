drop table dev.wc_bench_trv_members 

select uth_member_id, min(age_derived) as age , min(employee_status) as employee_status_raw, bus_cd, year 
into dev.wc_bench_trv_members
from data_warehouse.member_enrollment_monthly 
where year = 2016
  and data_source in ('truv') 
 and state = 'TX'
 group by uth_member_id, bus_cd, year 

 
insert into dev.wc_bench_trv_members 
select uth_member_id, min(age_derived) as age , min(employee_status) as employee_status_raw, bus_cd, year
from data_warehouse.member_enrollment_monthly 
where year = 2017
  and data_source in ('truv') 
 and state = 'TX'
 group by uth_member_id, bus_cd, year
 
 insert into dev.wc_bench_trv_members 
select uth_member_id, min(age_derived) as age , min(employee_status) as employee_status_raw, bus_cd, year
from data_warehouse.member_enrollment_monthly 
where year = 2018
  and data_source in ('truv') 
 and state = 'TX'
 group by uth_member_id, bus_cd, year
 
  ---++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--- Age Group
-------------------------------------------------------------------------------------

alter table dev.wc_bench_trv_members add column age_group int2;


update dev.wc_bench_trv_members set age_group = CAse when AGE BETWEEN 0 AND 19 THEN 1
            when AGE BETWEEN 20 AND 34 THEN 2
                                                when AGE BETWEEN 35 AND 44 THEN 3
                                                when AGE BETWEEN 45 AND 54 THEN 4
                                                when AGE BETWEEN 55 AND 64 THEN 5
                                                when AGE BETWEEN 65 AND 74 THEN 6
                                                ELSE 7 end;

                                               
 --------++++++++++++++++++++++++++++++++++++++++++
 alter table dev.wc_bench_trv_members add column emp_status text;

update dev.wc_bench_trv_members set emp_status = case when employee_status_raw in ('3','4','5') and bus_cd = 'MCR' then 'Retiree MP'
                                                      when employee_status_raw in ('3','4','5') and bus_cd = 'COM' then 'Retiree MS'
                                                      when employee_status_raw = '1' then 'Active FT'
        										      when employee_status_raw = '6' then 'Cobra'
        										      else 'Unknown' end 



        										      
drop table dev.wc_trv_family

select distinct efamid, enrolid, '0' as flg , year, 0 as uth_mem 
into dev.wc_trv_family
from truven.ccaet 

insert into dev.wc_trv_family 
select distinct efamid, enrolid, '0' as flg , year, 0 as uth_mem 
from truven.mdcrt 

drop table dev.wc_trv_fam_cnt

select a.efamid, count(distinct enrolid ) as  cnt , year 
into dev.wc_trv_fam_cnt 
from truven.ccaea a
group by efamid, year 

insert into dev.wc_trv_fam_cnt 
select a.efamid, count(distinct enrolid ) as  cnt , year
from truven.mdcra a
where a.efamid not in ( select efamid from dev.wc_trv_fam_cnt)
group by efamid, year 


update dev.wc_trv_family a set flg = '1'
from dev.wc_trv_fam_cnt b 
  where a.efamid = b.efamid 
    and a.year = b.year 
    and b.cnt > 1
    
update dev.wc_trv_family a set uth_mem = uth_member_id 
from data_warehouse.dim_uth_member_id b 
   where b.data_source = 'truv'
  and b.member_id_src = a.enrolid::text
   

  drop table dev.wc_trv_fam_update;
  
  select uth_mem, year, min(flg) as fam_flag  
  into dev.wc_trv_fam_update
  from dev.wc_trv_family 
  group by uth_mem, year 
 
  alter table dev.wc_bench_trv_members add column family_id text;
 
 
 select distinct efamid, enrolid, year, 0 as uth_mem 
 into dev.wc_trv_fam_xwalk
 from truven.ccaea 
 
 insert into dev.wc_trv_fam_xwalk 
 select distinct efamid, enrolid, year, 0 as uth_mem 
 from truven.mdcra a 
 where enrolid not in ( select efamid from dev.wc_trv_fam_xwalk)
 
 
 update dev.wc_trv_fam_xwalk a set uth_mem = uth_member_id 
from data_warehouse.dim_uth_member_id b 
   where b.data_source = 'truv'
  and b.member_id_src = a.enrolid::text
  
  select count(*), count(distinct(year::text || uth_mem::text)) from dev.wc_trv_fam_xwalk
  
  select uth_mem, year, min(efamid) as efamid
  into dev.wc_trv_fam_xwalk_load
  from dev.wc_trv_fam_xwalk
  group by uth_mem, year 
 
 update dev.wc_bench_trv_members a set family_id = efamid 
 from dev.wc_trv_fam_xwalk_load b 
   where b.uth_mem = a.uth_member_id 
     and b.year = a.year 
   
select count(*)::float / count(distinct family_id)::float, 
       emp_status , year
from dev.wc_bench_trv_members a 
 group by emp_status , year 
 order by year,  emp_status 

select * from dev.wc_bench_trv_members wbtm 

 

select efamid, count(distinct enrolid), year, enrolid 
from truven.ccaet 
group by efamid, year, enrolid 
        										      
                                            
 --unique # employee status                                            
select count(uth_member_id), count(distinct uth_member_id), year , emp_status
 from dev.wc_bench_trv_members
 group by year , emp_status
order by year, emp_status 

--mm count
select count(*), emp_status, a.year
from data_warehouse.member_enrollment_monthly a 
  join dev.wc_bench_trv_members b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year
group by emp_status, a.year
order by a.year, emp_status
                                               
--unique # by age group
select count(uth_member_id), count(distinct uth_member_id),substring(emp_status,1,5), year, age_group 
 from dev.wc_bench_trv_members
 where emp_status in ('Active FT','Retiree MP', 'Retiree MS')
 group by substring(emp_status,1,5), year, age_group 
order by year, substring(emp_status,1,5), age_group 

--MM total
select count(*), count(a.uth_member_id ), count(distinct a.uth_member_id ), substring(emp_status,1,5), a.year, age_group 
from data_warehouse.member_enrollment_monthly a 
  join dev.wc_bench_trv_members b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year
where emp_status in ('Active FT','Retiree MP', 'Retiree MS')
 group by substring(emp_status,1,5), a.year, age_group 
order by a.year, substring(emp_status,1,5), age_group   


delete from data_warehouse.member_enrollment_monthly  where row_identifier in  ( 
select row_identifier 
from (
	select row_number() over(partition by uth_member_id, month_year_id order by month_year_id) as rn, row_identifier 
	from  data_warehouse.member_enrollment_monthly
	where data_source = 'truv' 
 ) a where rn = 2
  ) 
 
                                                                                      
---medical  by status and age group
select count(uth_claim_id), sum(a.total_allowed_amount) as alw,  a.year, substring(emp_status,1,5), age_group  
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
    and b.emp_status in ('Active FT','Retiree MP', 'Retiree MS')
group by a.year, substring(emp_status,1,5), age_group  
order by a.year, substring(emp_status,1,5), age_group  
;


select count(uth_claim_id), sum(a.total_allowed_amount) as alw, a.year,emp_status 
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by a.year, emp_status
order by a.year, emp_status 
;


---RX by status and by status + age grp  
select count(uth_rx_claim_id), sum(a.total_allowed_amount ) as rxtot , a.year, substring(emp_status,1,5), age_group 
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
    and b.emp_status in ('Active FT','Retiree MP', 'Retiree MS')
group by a.year, substring(emp_status,1,5), age_group   
order by a.year, substring(emp_status,1,5), age_group 
; 	


select count(uth_rx_claim_id), sum(a.total_allowed_amount ) as rxtot , a.year,emp_status 
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by  a.year,emp_status   
order by  a.year,emp_status 
; 	

----++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
---- PMPY ----------------------------------------------------------------------------------------------
drop table dev.wc_bench_trv_pmpy 


select uth_member_id, sum(chg) as alw, year 
into dev.wc_bench_trv_pmpy
from 
(
	select b.uth_member_id, sum(total_allowed_amount) as chg, b.year
	from data_warehouse.pharmacy_claims a
   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
	group by b.uth_member_id, b.year 
union 
	select b.uth_member_id, sum(total_allowed_amount) as chg, b.year 
	from data_warehouse.claim_header a
	   join dev.wc_bench_trv_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
	group by b.uth_member_id, b.year 
) inr 
group by uth_member_id, year 
having sum(chg) >= 100000



select count(uth_claim_id), sum(a.total_allowed_amount) as alw,a.year, emp_status
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
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
    and b.uth_member_id in ( select uth_member_id from dev.wc_bench_trv_pmpy )
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


select * from dev.wc_bench_trv_members wbtm 


select count(distinct caseid ), sum(a.netpay ), a.year, b.emp_status 
from truven.ccaef a 
 join dev.wc_bench_trv_members b 
    on enrolid::text = myenrolid 
   and b.year = a.year 
where substring(billtyp,1,2) = '11'
group by a.year, b.emp_status 
;


select count(distinct uth_claim_id ), sum(total_allowed_amount ), a.year, b.emp_status 
from data_warehouse.claim_header a 
   join dev.wc_bench_trv_members b 
     on a.uth_member_id  = b.uth_member_id 
    and a.year = b.year 
where a.place_of_service = '11' 
  and a.claim_type = 'F'
group by a.year, b.emp_status 
order by a.year, emp_status 
