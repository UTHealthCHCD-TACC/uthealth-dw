--Adult Depression - Commercial 2019
drop table dev.wc_depression_com_2019;

select distinct on (a.uth_member_id) 
       a.uth_member_id, 
       a.zip3, 
	   case  when a.age_derived between 0  and 17 then 1 
	         when a.age_derived between 18 and 29 then 2
	         when a.age_derived between 30 and 39 then 3
	         when a.age_derived between 40 and 49 then 4
	         when a.age_derived between 50 and 59 then 5 
	         when a.age_derived between 60 and 64 then 6 
	   end as age_group,
       a.gender_cd, 
       data_source,
       case when b.uth_member_id is null then 0 else 1 end as vacc_flag
 into dev.wc_depression_com_2019
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
  and a.age_derived < 65
  and a.bus_cd = 'COM'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;

delete from dev.wc_depression_com_2019 where age_group is null;

delete from dev.wc_depression_com_2019 where length(zip3::text) = 2


------ Calculations ---------------------------   
			    
----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------
--prevalance all - row 51  -- optum 
select  ( sum(a.vacc_flag) / count(a.uth_member_id)::float )*100 as prev, count(a.uth_member_id) as mems, 
		( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems 
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.gender_cd = 'F'
    left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.gender_cd = 'M'
where a.data_source = 'optz'
;

---age group row 51 optum
select  ( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems, 
		( sum(d.vacc_flag) / count(d.uth_member_id)::float )*100 as prev, count(d.uth_member_id) as mems, 
		( sum(e.vacc_flag) / count(e.uth_member_id)::float )*100 as prev, count(e.uth_member_id) as mems, 
		( sum(f.vacc_flag) / count(f.uth_member_id)::float )*100 as prev, count(f.uth_member_id) as mems, 
		( sum(g.vacc_flag) / count(g.uth_member_id)::float )*100 as prev, count(g.uth_member_id) as mems
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.age_group = 1
  left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.age_group = 2 
  left outer join dev.wc_depression_com_2019 d 
     on d.uth_member_id = a.uth_member_id 
    and d.age_group = 3 
   left outer join dev.wc_depression_com_2019 e 
     on e.uth_member_id = a.uth_member_id 
    and e.age_group = 4
   left outer join dev.wc_depression_com_2019 f 
     on f.uth_member_id = a.uth_member_id 
    and f.age_group = 5
   left outer join dev.wc_depression_com_2019 g 
     on g.uth_member_id = a.uth_member_id 
    and g.age_group = 6 
where a.data_source = 'optz'
;


--prevalance all - row 51  -- truven weight
select  ( sum(a.vacc_flag) / count(a.uth_member_id)::float )*100 as prev, count(a.uth_member_id) as mems, 
		( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems 
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.gender_cd = 'F'
    left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.gender_cd = 'M'
where a.data_source = 'truv'
;


---age group row 51 truv
select  ( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems, 
		( sum(d.vacc_flag) / count(d.uth_member_id)::float )*100 as prev, count(d.uth_member_id) as mems, 
		( sum(e.vacc_flag) / count(e.uth_member_id)::float )*100 as prev, count(e.uth_member_id) as mems, 
		( sum(f.vacc_flag) / count(f.uth_member_id)::float )*100 as prev, count(f.uth_member_id) as mems, 
		( sum(g.vacc_flag) / count(g.uth_member_id)::float )*100 as prev, count(g.uth_member_id) as mems
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.age_group = 1
  left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.age_group = 2 
  left outer join dev.wc_depression_com_2019 d 
     on d.uth_member_id = a.uth_member_id 
    and d.age_group = 3 
   left outer join dev.wc_depression_com_2019 e 
     on e.uth_member_id = a.uth_member_id 
    and e.age_group = 4
   left outer join dev.wc_depression_com_2019 f 
     on f.uth_member_id = a.uth_member_id 
    and f.age_group = 5
   left outer join dev.wc_depression_com_2019 g 
     on g.uth_member_id = a.uth_member_id 
    and g.age_group = 6 
where a.data_source = 'truv'
;


----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------


 
 -- optum weight  missing zip 753 772 
 insert into dev.wc_depression_com_2019 values 
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

select  ( sum(a.vacc_flag) / count(a.uth_member_id)::float )*100 as prev, count(a.uth_member_id) as mems, 
		( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems 
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.gender_cd = 'F'
    left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.gender_cd = 'M'
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3;




select  ( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems, 
		( sum(d.vacc_flag) / count(d.uth_member_id)::float )*100 as prev, count(d.uth_member_id) as mems, 
		( sum(e.vacc_flag) / count(e.uth_member_id)::float )*100 as prev, count(e.uth_member_id) as mems, 
		( sum(f.vacc_flag) / count(f.uth_member_id)::float )*100 as prev, count(f.uth_member_id) as mems, 
		( sum(g.vacc_flag) / count(g.uth_member_id)::float )*100 as prev, count(g.uth_member_id) as mems
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.age_group = 1
  left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.age_group = 2 
  left outer join dev.wc_depression_com_2019 d 
     on d.uth_member_id = a.uth_member_id 
    and d.age_group = 3 
   left outer join dev.wc_depression_com_2019 e 
     on e.uth_member_id = a.uth_member_id 
    and e.age_group = 4
   left outer join dev.wc_depression_com_2019 f 
     on f.uth_member_id = a.uth_member_id 
    and f.age_group = 5
   left outer join dev.wc_depression_com_2019 g 
     on g.uth_member_id = a.uth_member_id 
    and g.age_group = 6 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3 
;

 
 -- truven
select  ( sum(a.vacc_flag) / count(a.uth_member_id)::float )*100 as prev, count(a.uth_member_id) as mems, 
		( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems 
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.gender_cd = 'F'
    left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.gender_cd = 'M'
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3;


select  ( sum(b.vacc_flag) / count(b.uth_member_id)::float )*100 as prev, count(b.uth_member_id) as mems, 
		( sum(c.vacc_flag) / count(c.uth_member_id)::float )*100 as prev, count(c.uth_member_id) as mems, 
		( sum(d.vacc_flag) / count(d.uth_member_id)::float )*100 as prev, count(d.uth_member_id) as mems, 
		( sum(e.vacc_flag) / count(e.uth_member_id)::float )*100 as prev, count(e.uth_member_id) as mems, 
		( sum(f.vacc_flag) / count(f.uth_member_id)::float )*100 as prev, count(f.uth_member_id) as mems, 
		( sum(g.vacc_flag) / count(g.uth_member_id)::float )*100 as prev, count(g.uth_member_id) as mems
from dev.wc_depression_com_2019 a 
  left outer join dev.wc_depression_com_2019 b  
     on b.uth_member_id = a.uth_member_id 
    and b.age_group = 1
  left outer join dev.wc_depression_com_2019 c 
     on c.uth_member_id = a.uth_member_id 
    and c.age_group = 2 
  left outer join dev.wc_depression_com_2019 d 
     on d.uth_member_id = a.uth_member_id 
    and d.age_group = 3 
   left outer join dev.wc_depression_com_2019 e 
     on e.uth_member_id = a.uth_member_id 
    and e.age_group = 4
   left outer join dev.wc_depression_com_2019 f 
     on f.uth_member_id = a.uth_member_id 
    and f.age_group = 5
   left outer join dev.wc_depression_com_2019 g 
     on g.uth_member_id = a.uth_member_id 
    and g.age_group = 6 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3 
;
