--adult depression medicare advantage 2019

---optum and truven cohorts from DW
drop table dev.wc_depression_mcradv_2019;

select distinct on (a.uth_member_id) 
       a.uth_member_id, 
       a.zip3, 
	   7 age_group,
       a.gender_cd, 
       data_source, 
       case when b.uth_member_id is null then 0 else 1 end as vacc_flag 
 into dev.wc_depression_mcradv_2019
from data_warehouse.member_enrollment_yearly a
  left outer join dev.wc_depression_2019_vacc b 
     on b.uth_member_id = a.uth_member_id 
  left outer join dev.wc_depression_2019_exclusions c
     on c.uth_member_id = a.uth_member_id
where c.uth_member_id is null 
  and a.data_source in ('truv','optz')
  and a.year = 2019 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.bus_cd = 'MCR'
  and a.total_enrolled_months = 12
  and a.age_derived >= 18
  and a.gender_cd in ('M','F')
;


------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  -- optum 
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev, count(uth_member_id) as mems, 'all' as grp 
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'optz'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev, count(uth_member_id) as mems, gender_cd
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'optz'
group by a.gender_cd
) x 
order by grp
;

--prevalance all - row 51  -- truven weight
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev,count(uth_member_id) as mems,  'all' as grp 
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'truv'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev,count(uth_member_id) as mems,  gender_cd
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'truv'
group by a.gender_cd
) x 
order by grp
;



----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------
 
 -- optum weight  missing zip 753 772 
 insert into dev.wc_depression_mcradv_2019 values 
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

select ( sum(vacc_flag) / count(uth_member_id)::float ) *100 as prev, count(uth_member_id) as mems
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) *100 as prev, count(uth_member_id) as mems
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev, count(uth_member_id) as mems
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
  
 
 -- truven
select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev , count(uth_member_id) as mems
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev, count(uth_member_id) as mems
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev, count(uth_member_id) as mems
from dev.wc_depression_mcradv_2019 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


