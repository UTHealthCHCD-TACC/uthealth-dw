drop table dev.wc_bench_optz_members

select a.uth_member_id, b.member_id_src, age_derived as age , a.total_enrolled_months , bus_cd, year , a.gender_cd 
into dev.wc_bench_optz_members
from data_warehouse.member_enrollment_yearly a
  join data_warehouse.dim_uth_member_id b 
    on a.uth_member_id = b.uth_member_id 
where year between 2016 and 2019
  and a.data_source = 'optz' 
 and state = 'TX'
;


 

 --unique # Active vs Retired 
 select count(uth_member_id), count(distinct uth_member_id),  sum(total_enrolled_months) as MM, bus_cd, year 
 from dev.wc_bench_optz_members
 group by bus_cd, year 
order by year, bus_cd



---medical FT and Retiree from dw
select * from 
(
select count(uth_claim_id),  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw, year , 'COM' as bus
from data_warehouse.claim_header a 
where uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'COM' and year = a.year )
group by year 
union 
select count(uth_claim_id),  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw , year, 'MCR'
from data_warehouse.claim_header a 
where uth_member_id in ( select uth_member_id from dev.wc_bench_optz_members where bus_cd = 'MCR' and year = a.year)
group by year 
) x 
order by year, bus 
;

--medical ft and retiree from raw 
select * from 
(
select count(distinct a.clmid),  sum(a.charge ) as chg, sum(a.std_cost ) as alw, extract(year from a.fst_dt ) as yr , 'COM' as bus
from optum_zip.medical a 
where patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'COM' and year = extract(year from a.fst_dt ) )
group by extract(year from a.fst_dt )
union 
select count(distinct a.clmid),  sum(a.charge ) as chg, sum(a.std_cost ) as alw, extract(year from a.fst_dt ) as yr , 'MCR' as bus
from optum_zip.medical a 
where patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'MCR' and year = extract(year from a.fst_dt ) )
group by extract(year from a.fst_dt )
) x 
order by yr, bus 
;

---RX FT and Retiree
select * 
from ( 
select count(distinct a.clmid) as clm , sum(a.std_cost) as alw ,  extract(year from a.fill_dt ) as yr , 'COM' as bus
from optum_zip.rx a 
where a.patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'COM' and year = extract(year from a.fill_dt ) )
group by  extract(year from a.fill_dt )
union 
select count(distinct a.clmid), sum(a.std_cost),  extract(year from a.fill_dt ), 'MCR'
from optum_zip.rx a 
where a.patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'MCR' and year = extract(year from a.fill_dt ) )
group by  extract(year from a.fill_dt )
) X 
order by yr, bus
;	






drop table dev.wc_bench_optz_pmpy

----++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
---- PMPY ----------------------------------------------------------------------------------------------

select patid, sum(alw) as charge, bus, yr 
into dev.wc_bench_optz_pmpy
from 
(	--medical
	select a.patid::text, sum(a.std_cost ) as alw, extract(year from a.fst_dt ) as yr , 'COM' as bus
	from optum_zip.medical a 
	where patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'COM' and year = extract(year from a.fst_dt ) )
	group by a.patid::text,extract(year from a.fst_dt )
union 
	select a.patid::text, sum(a.std_cost ) as alw, extract(year from a.fst_dt ) as yr , 'MCR' as bus
	from optum_zip.medical a 
	where patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'MCR' and year = extract(year from a.fst_dt ) )
	group by a.patid::text,extract(year from a.fst_dt )
union 
	---rx
	select a.patid::text, sum(a.std_cost) as alw ,  extract(year from a.fill_dt ) as yr , 'COM' as bus
	from optum_zip.rx a 
	where a.patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'COM' and year = extract(year from a.fill_dt ) )
	group by  a.patid::text,extract(year from a.fill_dt )
union 
	select a.patid::text, sum(a.std_cost),  extract(year from a.fill_dt ), 'MCR'
	from optum_zip.rx a 
	where a.patid::text in ( select member_id_src from dev.wc_bench_optz_members where bus_cd = 'MCR' and year = extract(year from a.fill_dt ) )
	group by a.patid::text, extract(year from a.fill_dt )
) inr 
group by patid, yr, bus  
having sum(alw) >= 100000


---pmpy unique# and MM
 select count(distinct patid) as mem, sum(c.total_enrolled_months ) as MM, bus, yr 
 from dev.wc_bench_optz_pmpy a 
   join data_warehouse.dim_uth_member_id b 
     on b.member_id_src = a.patid 
   join data_warehouse.member_enrollment_yearly c 
      on c.uth_member_id = b.uth_member_id 
      and c.year = a.yr 
 group by bus, yr 
order by yr, bus 



----*** PMPY***
---medical 
select * from 
(
select count(distinct a.clmid),  sum(a.charge ) as chg, sum(a.std_cost ) as alw, extract(year from a.fst_dt ) as yr , 'COM' as bus
from optum_zip.medical a 
where patid::text in ( select patid from dev.wc_bench_optz_pmpy where bus = 'COM' and yr = extract(year from a.fst_dt ) )
group by extract(year from a.fst_dt )
union 
select count(distinct a.clmid),  sum(a.charge ) as chg, sum(a.std_cost ) as alw, extract(year from a.fst_dt ) as yr , 'MCR' as bus
from optum_zip.medical a 
where patid::text in ( select patid from dev.wc_bench_optz_pmpy where bus = 'MCR' and yr = extract(year from a.fst_dt ) )
group by extract(year from a.fst_dt )
) x 
order by yr, bus 
;

---RX *PMPY*
select * 
from ( 
select count(distinct a.clmid) as clm , sum(a.std_cost) as alw ,  extract(year from a.fill_dt ) as yr , 'COM' as bus
from optum_zip.rx a 
where a.patid::text in ( select patid from dev.wc_bench_optz_pmpy where bus = 'COM' and yr = extract(year from a.fill_dt ) )
group by  extract(year from a.fill_dt )
union 
select count(distinct a.clmid), sum(a.std_cost),  extract(year from a.fill_dt ), 'MCR'
from optum_zip.rx a 
where a.patid::text in ( select patid from dev.wc_bench_optz_pmpy where bus = 'MCR' and yr = extract(year from a.fill_dt ) )
group by  extract(year from a.fill_dt )
) X 
order by yr, bus
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
select year, bus_cd, age_group, count(distinct uth_member_id) as mems, sum(total_enrolled_months) as MM
 from dev.wc_bench_optz_members
 group by bus_cd, year, age_group 
order by year, bus_cd, age_group 



---medical  
select a.year, bus_cd, age_group, 
	   count(uth_claim_id) as clm,  sum(a.total_charge_amount) as chg, sum(a.total_allowed_amount) as alw
from data_warehouse.claim_header a 
   join dev.wc_bench_optz_members b 
     on b.uth_member_id = a.uth_member_id
    and b.year = a.year 
group by age_group, a.year  , bus_cd
order by a.year, bus_cd, age_group 
;


---RX 
select a.year, bus_cd, age_group, 
       count(distinct a.clmid) as clm, sum(a.std_cost) as alw
from optum_zip.rx a 
   join dev.wc_bench_optz_members b 
     on b.member_id_src = a.patid::text 
    and b.year = extract(year from a.fill_dt )
group by age_group, a.year  , bus_cd
order by a.year, bus_cd, age_group 
;



-----------****---- Utilization --------------------------------------------------------------------------------
-------****-------- Utilization --------------------------------------------------------------------------------
---****------------ Utilization --------------------------------------------------------------------------------
delete from dev.wc_bench_optz_members where gender_cd not in ('M','F')


select year, bus_cd, age_group, gender_cd, count(distinct uth_member_id) as mems, sum(total_enrolled_months) as MM
 from dev.wc_bench_optz_members
 group by bus_cd, year, gender_cd, age_group 
order by year,gender_cd desc, bus_cd, age_group 

----Do Active vs FT first across all rows

--inpatient
select count(distinct conf_id), sum(std_cost) as alw, b.year, b.bus_cd
from optum_zip.medical a 
 join dev.wc_bench_optz_members b 
    on patid::text = member_id_src 
   and b.year = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
 group by b.year, b.bus_cd
 order by b.year, b.bus_cd
 ;

--ER 
select count(distinct patid::text||a.fst_dt::text), sum(std_cost) as alw, b.year, b.bus_cd
from optum_zip.medical a 
 join dev.wc_bench_optz_members b 
    on patid::text = member_id_src 
   and b.year = a.year 
 where tos_cd = 'FAC_OP.ER'
   and a.rvnu_cd in ('0450','0451','0452','0456','0459' )
 group by b.year, b.bus_cd
 order by b.year, b.bus_cd
 ;

--ED Visits
select count(distinct a.uth_claim_id::text || from_date_of_service::text), sum(a.allowed_amount ), 
       b.year, b.bus_cd 
from data_warehouse.claim_detail a
   join dev.wc_bench_optz_members b 
    on a.uth_member_id  = b.uth_member_id 
    and a.year = b.year
    and a.revenue_cd in ('450','451','452','456','459','0450','0451','0452','0456','0459')
    and a.bill_type_class = '3'
    and a.bill_type_inst = '1' 
 group by b.year, b.bus_cd
 order by b.year, b.bus_cd
 ;



--30 day readmissions
drop table  dev.wc_optz_readmissions

select patid, a.conf_id, b.year, b.age_group, b.bus_cd, b.gender_cd, min(a.fst_dt) as fst_dt
into dev.wc_optz_readmissions
from optum_zip.medical a 
 join dev.wc_bench_optz_members b 
   on patid::text = member_id_src 
   and b.year = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
 group by patid, a.conf_id, b.year, b.age_group, b.bus_cd, b.gender_cd
;

select count(distinct conf_id), year, bus_cd 
from dev.wc_optz_readmissions a 
where exists ( select 1 from dev.wc_optz_readmissions b 
						where a.patid = b.patid 
						 and b.fst_dt > a.fst_dt 
						and b.fst_dt < a.fst_dt + interval'30 days' )
group by year, bus_cd 
order by year, bus_cd 
;

----- HCC

select * from   dev.wc_bench_optz_pmpy

--admissions
select count(distinct conf_id), sum(std_cost) as alw, b.yr, b.bus
from optum_zip.medical a 
 join dev.wc_bench_optz_pmpy b 
   on b.patid = a.patid::text
   and b.yr = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
 group by b.yr, b.bus
 order by b.yr, b.bus
 ;

---ED Visits
 select count(distinct a.patid::text||a.fst_dt::text), sum(std_cost) as alw, b.yr, b.bus
from optum_zip.medical a 
 join dev.wc_bench_optz_pmpy b 
   on b.patid = a.patid::text
   and b.yr = a.year 
 where tos_cd = 'FAC_OP.ER'
   and a.rvnu_cd in ('0450','0451','0452','0456','0459' )
 group by b.yr, b.bus
 order by b.yr, b.bus
 ;  

---Readmission
select a.patid, a.conf_id, b.yr, b.bus, min(a.fst_dt) as fst_dt
into dev.wc_optz_readmissions_hcc
from optum_zip.medical a 
 join dev.wc_bench_optz_pmpy b 
   on b.patid = a.patid::text
   and b.yr = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
 group by a.patid, a.conf_id, b.yr, b.bus
;

select count(distinct conf_id), yr, bus 
from dev.wc_optz_readmissions_hcc a 
where exists ( select 1 from dev.wc_optz_readmissions_hcc b 
						where a.patid = b.patid 
						 and b.fst_dt > a.fst_dt 
						and b.fst_dt < a.fst_dt + interval'30 days' )
group by yr, bus 
order by yr, bus 
;
   

--- By Age Group + FT or Ret


--inpatient
select count(distinct conf_id), sum(std_cost) as alw, b.year, b.bus_cd, b.age_group 
from optum_zip.medical a 
 join dev.wc_bench_optz_members b 
    on patid::text = member_id_src 
   and b.year = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
 group by b.year, b.bus_cd, b.age_group 
 order by b.year, b.bus_cd, b.age_group 
 ;

--ED visits
 select count(distinct patid::text||a.fst_dt::text), sum(std_cost) as alw, b.year, b.bus_cd, b.age_group 
from optum_zip.medical a 
 join dev.wc_bench_optz_members b 
    on patid::text = member_id_src 
   and b.year = a.year 
 where tos_cd = 'FAC_OP.ER'
   and a.rvnu_cd in ('0450','0451','0452','0456','0459' )
 group by b.year, b.bus_cd, b.age_group 
 order by b.year, b.bus_cd, b.age_group 
 ;  

--readmissions
select count(distinct conf_id), year, bus_cd, age_group 
from dev.wc_optz_readmissions a 
where exists ( select 1 from dev.wc_optz_readmissions b 
						where a.patid = b.patid 
						 and b.fst_dt > a.fst_dt 
						and b.fst_dt < a.fst_dt + interval'30 days' )
group by year, bus_cd , age_group 
order by year, bus_cd , age_group 
;

--- age group / ft or ret / gender

--run once for M and once for F 
--inpatient
select count(distinct conf_id), sum(std_cost) as alw, b.year, b.bus_cd, b.age_group 
from optum_zip.medical a 
 join dev.wc_bench_optz_members b 
     on patid::text = member_id_src 
   and b.year = a.year 
 where tos_cd = 'FAC_IP.ACUTE'
  and b.gender_cd = 'M'
 group by b.year, b.bus_cd, b.age_group 
 order by b.year, b.bus_cd, b.age_group 
 ;

--ED visits
 select count(distinct patid::text||a.fst_dt::text), sum(std_cost) as alw, b.year, b.bus_cd, b.age_group 
from optum_zip.medical a 
 join dev.wc_bench_optz_members b 
    on patid::text = member_id_src 
   and b.year = a.year 
 where tos_cd = 'FAC_OP.ER'
   and a.rvnu_cd in ('0450','0451','0452','0456','0459' )
   and b.gender_cd = 'M'
 group by b.year, b.bus_cd, b.age_group 
 order by b.year, b.bus_cd, b.age_group 
 ;  

--readmissions
select count(distinct conf_id), year, bus_cd, age_group 
from dev.wc_optz_readmissions a 
where gender_cd = 'M' 
and exists ( select 1 from dev.wc_optz_readmissions b 
						where a.patid = b.patid 
						 and b.fst_dt > a.fst_dt 
						and b.fst_dt < a.fst_dt + interval'30 days' )
group by year, bus_cd , age_group 
order by year, bus_cd , age_group 
;




