---optum and truven cohorts from DW
drop table dev.wc_flu_com_2015;

select uth_member_id, 
       a.zip3, 
	   case  when a.age_derived between 0  and 17 then 1 
	         when a.age_derived between 18 and 29 then 2
	         when a.age_derived between 30 and 39 then 3
	         when a.age_derived between 40 and 49 then 4
	         when a.age_derived between 50 and 59 then 5 
	         when a.age_derived between 60 and 64 then 6 
	   end as age_group,
       a.gender_cd, 
       data_source 
 into dev.wc_flu_com_2015
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2015 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.age_derived < 65
  and a.bus_cd = 'COM'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


delete from dev.wc_flu_com_2015 where age_group is null;

delete from dev.wc_flu_com_2015 where length(zip3::text) = 2

select distinct uth_member_id 
into dev.wc_flu_com_2015_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
      and a.year = 2015 
      and a.uth_member_id in ( select uth_member_id from dev.wc_flu_com_2015)
;



alter table dev.wc_flu_com_2015 add column vacc_flag int2 default 0;


update dev.wc_flu_com_2015 a set vacc_flag = 1
  from dev.wc_flu_com_2015_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

------ Calculations ---------------------------   
			    
----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------
--prevalance all - row 51  -- optum 
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems, 'all' as grp 
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, gender_cd
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
group by a.gender_cd
) x 
order by grp
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, age_group 
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
group by age_group 
order by age_group;


--prevalance all - row 51  -- truven weight
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev,count(uth_member_id) as mems,  'all' as grp 
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev,count(uth_member_id) as mems,  gender_cd
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
group by a.gender_cd
) x 
order by grp
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems, age_group 
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
group by age_group 
order by age_group;



----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------


 
 -- optum weight  missing zip 753 772 
 insert into dev.wc_flu_com_2015 values 
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
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev , count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.age_group = '1'
  group by a.zip3 
order by a.zip3
  ;
  
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev , count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.age_group = '2'
  group by a.zip3 
order by a.zip3
  ; 
  
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev , count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.age_group = '3'
  group by a.zip3 
order by a.zip3
  ; 
  
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev , count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.age_group = '4'
  group by a.zip3 
order by a.zip3
  ; 
 
  
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev , count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.age_group = '5'
  group by a.zip3 
order by a.zip3
  ; 
 
  
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev ,count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'optz'
  and a.age_group = '6'
  group by a.zip3 
order by a.zip3
  ;  
 
 -- truven
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev , count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.age_group = '1'
  group by a.zip3 
order by a.zip3
  ;
  
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.age_group = '2'
  group by a.zip3 
order by a.zip3
  ;
 
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems 
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.age_group = '3'
  group by a.zip3 
order by a.zip3
  ;
 
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems 
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.age_group = '4'
  group by a.zip3 
order by a.zip3
  ;
 
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev , count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.age_group = '5'
  group by a.zip3 
order by a.zip3
  ;
 
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id) as mems
from dev.wc_flu_com_2015 a 
where a.data_source = 'truv'
  and a.age_group = '6'
  group by a.zip3 
order by a.zip3
  ; 