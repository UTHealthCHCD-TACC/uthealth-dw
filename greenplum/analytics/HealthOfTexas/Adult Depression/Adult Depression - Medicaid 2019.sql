---Health of Texas - Indepressionenza Vacc 2019 Medicaid 
drop table dev.wc_depression_mdcd_2019;

select  distinct on (m.uth_member_id) 
        m.uth_member_id , 
        substring(zip3,1,3) as zip3, 
        sex, 
       	case  when age::float between 0  and 17.99 then 1 
	         when age::float between 18 and 29.99 then 2
	         when age::float between 30 and 39.99 then 3
	         when age::float between 40 and 49.99 then 4
	         when age::float between 50 and 59.99 then 5 
	         when age::float between 60 and 64.99 then 6 
	         else 7
	   end as age_group ,
	   data_source,
       case when b.uth_member_id is null then 0 else 1 end as vacc_flag
into dev.wc_depression_mdcd_2019
from medicaid.agg_enrl_medicaid_cy1220 a 
   join data_warehouse.dim_uth_member_id m
      on m.member_id_src = a.client_nbr
     and m.data_source = 'mdcd'
   left outer join dev.wc_depression_2019_vacc b 
     on b.uth_member_id = m.uth_member_id 
  left outer join dev.wc_depression_2019_exclusions c
     on c.uth_member_id = m.uth_member_id 
where c.uth_member_id is null 
  and enrl_months = 12 
  and enrl_cy = 2019
  and age::float < 65.00
 and zip3::int between 750 and 799;


--cleanup
delete from dev.wc_depression_mdcd_2019 where age_group is null;

delete from dev.wc_depression_mdcd_2019 where zip3 = '771';

delete from dev.wc_depression_mdcd_2019 where sex = 'U';

select count(*), sum(vacc_flag) ,age_group 
from dev.wc_depression_mdcd_2019  
group by age_group order by age_group;


------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  
select * 
from ( 
	select  ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev, count(uth_member_id), sum(vacc_flag), 'all' as grp 
	from dev.wc_depression_mdcd_2019 a 
	union 
	select  ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev , count(uth_member_id), sum(vacc_flag), sex
	from dev.wc_depression_mdcd_2019 a 
	where a.sex = 'F'
	group by sex 
	union 
	select  ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev, count(uth_member_id), sum(vacc_flag), sex 
	from dev.wc_depression_mdcd_2019 a 
	where a.sex = 'M'
	group by sex
) x 
order by grp 
;



select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), age_group
from dev.wc_depression_mdcd_2019
group by age_group
order by age_group;


----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------


select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev , a.zip3 
from dev.wc_depression_mdcd_2019 a 
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev 
from dev.wc_depression_mdcd_2019 a 
where a.sex = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev 
from dev.wc_depression_mdcd_2019 a 
where a.sex = 'M'
  group by a.zip3 
order by a.zip3
;

---by age grp 
select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_depression_mdcd_2019
where age_group = 1
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_depression_mdcd_2019
where age_group = 2
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_depression_mdcd_2019
where age_group = 3
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_depression_mdcd_2019
where age_group = 4
group by zip3
order by zip3;


select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_depression_mdcd_2019
where age_group = 5
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_depression_mdcd_2019
where age_group = 6
group by zip3
order by zip3;

