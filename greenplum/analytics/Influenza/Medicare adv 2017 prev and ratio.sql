--influenza medicare advantage 2017

---optum and truven cohorts from DW
drop table dev.wc_flu_mcradv_2017;

select uth_member_id, 
       a.zip3, 
	   7 age_group,
       a.gender_cd, 
       data_source 
 into dev.wc_flu_mcradv_2017
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2017 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.bus_cd = 'MCR'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


---get vaccinations
drop table dev.wc_flu_mcradv_2017_vacc

select distinct uth_member_id 
into dev.wc_flu_mcradv_2017_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
      and a.year = 2017 
      and a.uth_member_id in ( select uth_member_id from dev.wc_flu_mcradv_2017)
;


alter table dev.wc_flu_mcradv_2017 add column vacc_flag int2 default 0;


update dev.wc_flu_mcradv_2017 a set vacc_flag = 1
  from dev.wc_flu_mcradv_2017_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

select count(*), sum(vacc_flag), data_source, count(distinct uth_member_id) as mem
from dev.wc_flu_mcradv_2017 
group by data_source;



------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  -- optum 
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems, 'all' as grp 
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'optz'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, gender_cd
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'optz'
group by a.gender_cd
) x 
order by grp
;

--prevalance all - row 51  -- truven weight
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev,count(uth_member_id) as mems,  'all' as grp 
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'truv'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev,count(uth_member_id) as mems,  gender_cd
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'truv'
group by a.gender_cd
) x 
order by grp
;



----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------
 
 -- optum weight  missing zip 753 772 
 insert into dev.wc_flu_mcradv_2017 values 
(0001, 753,1,'M','optz',0),
(0002, 753,2,'M','optz',0),
(0003, 753,3,'M','optz',0),
(0004, 753,4,'F','optz',0),
(0005, 753,5,'F','optz',0),
(0006, 753,6,'F','optz',0),
(0007, 772,1,'M','optz',0),
(0008, 772,2,'M','optz',0),
(0009, 772,3,'M','optz',0),
(0010, 772,4,'F','optz',0),
(0011, 772,5,'F','optz',0),
(0012, 772,6,'F','optz',0)
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id) as mems
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id) as mems
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
  
 
 -- truven
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , count(uth_member_id) as mems
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems
from dev.wc_flu_mcradv_2017 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


