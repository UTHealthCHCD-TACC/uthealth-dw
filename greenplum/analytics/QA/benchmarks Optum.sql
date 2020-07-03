

select uth_member_id, min(age_derived) as age , bus_cd, year 
into dev.wc_bench_optz_members
from data_warehouse.member_enrollment_monthly 
where year = 2016
  and data_source = 'optz' 
 and state = 'TX'
 group by uth_member_id, bus_cd, year 

 
insert into dev.wc_bench_optz_members 
select uth_member_id, min(age_derived) as age , bus_cd, year
from data_warehouse.member_enrollment_monthly 
where year = 2017
  and data_source = 'optz' 
 and state = 'TX'
 group by uth_member_id, bus_cd, year
 
 insert into dev.wc_bench_optz_members 
select uth_member_id, min(age_derived) as age , bus_cd, year
from data_warehouse.member_enrollment_monthly 
where year = 2018
  and data_source = 'optz' 
 and state = 'TX'
 group by uth_member_id, bus_cd, year
 
 
 --unique # Active vs Retired 
 select count(uth_member_id), count(distinct uth_member_id), bus_cd, year 
 from dev.wc_bench_optz_members
 group by bus_cd, year 
order by year, bus_Cd 



---medical FT
select count(uth_claim_id),  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw 
from data_warehouse.claim_header a 
where a.year = 2017 
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'COM' and year = a.year )
  
---RX FT  
select count(uth_rx_claim_id), sum(a.total_charge_amount ) 
from data_warehouse.pharmacy_claims a 
where a.year = 2016
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'COM' and year = a.year )
; 			

---medical Retiree
select count(uth_claim_id),  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw 
from data_warehouse.claim_header a 
where a.year = 2016 
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'MCR' and year = a.year)
  
--RX Retiree  
select count(uth_rx_claim_id), sum(a.total_charge_amount ) 
from data_warehouse.pharmacy_claims a 
where a.year = 2016
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'MCR' and year = a.year)
; 		


drop table dev.wc_bench_optz_pmpy

----++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
---- PMPY ----------------------------------------------------------------------------------------------

select uth_member_id, sum(chg) as charge, bus, year 
into dev.wc_bench_optz_pmpy
from 
(
	select uth_member_id, sum(total_charge_amount) as chg, year, 'MCR' as bus 
	from data_warehouse.pharmacy_claims a
	where year between 2016 and 2018 
	  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'MCR' and a.year = year )
	group by uth_member_id, year 
union 
	select uth_member_id, sum(total_charge_amount) as chg, year , 'MCR' as bus 
	from data_warehouse.claim_header a
	where year between 2016 and 2018 
	  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'MCR' and a.year = year )
	group by uth_member_id, year 
union 
		select uth_member_id, sum(total_charge_amount) as chg, year, 'COM' as bus 
	from data_warehouse.pharmacy_claims a
	where year between 2016 and 2018 
	  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'COM'and a.year = year )
	group by uth_member_id, year 
union
	select uth_member_id, sum(total_charge_amount) as chg, year , 'COM' as bus 
	from data_warehouse.claim_header a
	where year between 2016 and 2018 
	  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'COM' and a.year = year )
	group by uth_member_id, year 
) inr 
group by uth_member_id, year, bus  
having sum(chg) >= 100000


---pmpy unique# 
 select count(uth_member_id), count(distinct uth_member_id), bus, year 
 from dev.wc_bench_optz_pmpy
 group by bus, year 
order by year, bus 

--MM total
select count(*), bus, a.year 
from data_warehouse.member_enrollment_monthly a 
  join dev.wc_bench_optz_pmpy b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year
group by bus, a.year 


----*** RUN all 4 below scripts once per year ***
---pmpy medical FT  
select count(uth_claim_id),  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw 
from data_warehouse.claim_header a 
where a.year = 2016
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_pmpy where bus = 'COM' and year = a.year )
  
---RX FT  
select count(uth_rx_claim_id), sum(a.total_charge_amount ) as rxtot 
from data_warehouse.pharmacy_claims a 
where a.year = 2016
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_pmpy where bus = 'COM' and year = a.year )
; 			

---medical Retiree
select count(uth_claim_id),  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw_retiree
from data_warehouse.claim_header a 
where a.year = 2016
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_pmpy where bus = 'MCR' and year = a.year)
  
--RX Retiree  
select count(uth_rx_claim_id), sum(a.total_charge_amount ) as rxtot_retiree
from data_warehouse.pharmacy_claims a 
where a.year = 2016
  and uth_member_id in ( select uth_member_id from dev.wc_bench_optz_pmpy where bus = 'MCR' and year = a.year)
; 		

---++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--- Age Group
-------------------------------------------------------------------------------------

alter table dev.wc_bench_optz_members add column age_group int2;


update dev.wc_bench_optz_members set age_group = CAse when AGE BETWEEN 0 AND 19 THEN 1
            when AGE BETWEEN 20 AND 34 THEN 2
                                                when AGE BETWEEN 35 AND 44 THEN 3
                                                when AGE BETWEEN 45 AND 54 THEN 4
                                                when AGE BETWEEN 55 AND 64 THEN 5
                                                when AGE BETWEEN 65 AND 74 THEN 6
                                                ELSE 7 end;

--unique #
select count(uth_member_id), count(distinct uth_member_id), bus_cd, year, age_group 
 from dev.wc_bench_optz_members
 group by bus_cd, year, age_group 
order by year, bus_cd, age_group 

--MM total
select count(*), b.bus_cd, a.year, age_group 
from data_warehouse.member_enrollment_monthly a 
  join dev.wc_bench_optz_members b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year
   and a.bus_cd = b.bus_cd
group by b.bus_cd, a.year, age_group 
order by a.year, b.bus_cd, age_group 
     


---medical  
select count(uth_claim_id),  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw, age_group , a.year 
from data_warehouse.claim_header a 
   join dev.wc_bench_optz_members b 
     on b.uth_member_id = a.uth_member_id
    and b.bus_cd = 'MCR' --COM
    and b.year = a.year 
group by age_group, a.year  
order by a.year, age_group 
;


---RX FT  
select count(uth_rx_claim_id), sum(a.total_charge_amount ) as rxtot , age_group , a.year
from data_warehouse.pharmacy_claims a 
   join dev.wc_bench_optz_members b 
     on b.uth_member_id = a.uth_member_id
    and b.bus_cd = 'COM' --MCR
    and b.year = a.year 
group by age_group, a.year  
order by a.year, age_group 
; 			




--------------- Utilization 

alter table dev.wc_bench_optz_members add column mypatid bigint;

update dev.wc_bench_optz_members a set mypatid = member_id_src::bigint 
from data_warehouse.dim_uth_member_id b 
where a.uth_member_id = b.uth_member_id 
  and b.data_source = 'optz'
 ;



select count(distinct conf_id), sum(charge) as chg, sum(std_cost) as alw, b.year, b.bus_cd
from optum_zip.confinement a 
 join dev.wc_bench_optz_members b 
    on patid = mypatid 
   and b.year = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
 group by b.year, b.bus_cd
 order by b.year, b.bus_cd
 ;


select count(distinct conf_id), sum(charge) as chg, sum(std_cost) as alw, b.year, b.bus_cd, b.age_group 
from optum_zip.confinement a 
 join dev.wc_bench_optz_members b 
    on patid = mypatid 
   and b.year = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
 group by b.year, b.bus_cd, b.age_group 
 order by b.year, b.bus_cd, b.age_group 
 ;


---pmpy admissions
select count(distinct conf_id), sum(charge) as chg, sum(std_cost) as alw, b.year, b.bus_cd
from optum_zip.confinement a 
 join dev.wc_bench_optz_members b 
    on patid = mypatid 
   and b.year = a.year 
   and b.uth_member_id in ( select uth_member_id from dev.wc_bench_optz_pmpy wbop )
 where tos_cd = 'FAC_IP.ACUTE'
 group by b.year, b.bus_cd
 order by b.year, b.bus_cd
 ;



select count(*), a.gender, a.bus_cd , a.age_group, year 
from dev.wc_bench_optz_members a
group by  a.gender, a.bus_cd , a.age_group, year 
order by year, a.gender, a.bus_cd , a.age_group 
;

--MM total
select count(*),gender, a.bus_cd , age_group, b.year
from data_warehouse.member_enrollment_monthly a 
  join dev.wc_bench_optz_members b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year
group by gender, a.bus_cd , age_group, b.year 
order by b.year, gender, a.bus_cd , age_group
