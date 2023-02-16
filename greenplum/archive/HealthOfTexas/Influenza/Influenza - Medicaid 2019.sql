---Health of Texas - Influenza Vacc 2019 Medicaid 
drop table dev.wc_flu_mdcd_2019;

select  distinct on (uth_member_id) 
        b.uth_member_id , 
        substring(zip3,1,3) as zip3, 
        sex, 
       	case  when age::float between 0  and 17.99 then 1 
	         when age::float between 18 and 29.99 then 2
	         when age::float between 30 and 39.99 then 3
	         when age::float between 40 and 49.99 then 4
	         when age::float between 50 and 59.99 then 5 
	         when age::float between 60 and 64.99 then 6 
	         else 7
	   end as age_group 
into dev.wc_flu_mdcd_2019
from medicaid.agg_enrl_medicaid_cy1220 a 
   join data_warehouse.dim_uth_member_id b 
      on b.member_id_src = a.client_nbr
where enrl_months = 12 
  and enrl_cy = 2019
  and age::float < 65.00
 and zip3::int between 750 and 799;
 
 
delete from dev.wc_flu_mdcd_2019 where age_group is null;

delete from dev.wc_flu_mdcd_2019 where zip3 = '771';

delete from dev.wc_flu_mdcd_2019 where sex = 'U';

select count(*), age_group from dev.wc_flu_mdcd_2019  group by age_group order by age_group;





drop table dev.wc_flu_2019_vacc;

select distinct uth_member_id 
into dev.wc_flu_2019_vacc
from data_warehouse.claim_detail a
where trim(a.cpt_hcpcs_cd) in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
      and a.year = 2019 
;


alter table dev.wc_flu_mdcd_2019 add column vacc_flag int2 default 0;


update dev.wc_flu_mdcd_2019 a set vacc_flag = 1
  from dev.wc_flu_2019_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

select count(*), sum(vacc_flag), data_source, count(distinct uth_member_id) as mem
from dev.wc_flu_mdcd_2019 
group by data_source;



------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  
select * 
from ( 
	select ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev, count(uth_member_id), sum(vacc_flag), 'all' as grp 
	from dev.wc_flu_mdcd_2019 a 
	union 
	select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev, count(uth_member_id), sum(vacc_flag), sex
	from dev.wc_flu_mdcd_2019 a 
	where a.sex = 'F'
	group by sex 
	union 
	select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev, count(uth_member_id), sum(vacc_flag), sex 
	from dev.wc_flu_mdcd_2019 a 
	where a.sex = 'M'
	group by sex
) x 
order by grp 
;



select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), age_group
from dev.wc_flu_mdcd_2019
group by age_group
order by age_group;


----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------


select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev , a.zip3 
from dev.wc_flu_mdcd_2019 a 
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev 
from dev.wc_flu_mdcd_2019 a 
where a.sex = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev 
from dev.wc_flu_mdcd_2019 a 
where a.sex = 'M'
  group by a.zip3 
order by a.zip3
;

---by age grp 
select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_flu_mdcd_2019
where age_group = 1
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_flu_mdcd_2019
where age_group = 2
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_flu_mdcd_2019
where age_group = 3
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_flu_mdcd_2019
where age_group = 4
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_flu_mdcd_2019
where age_group = 5
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_flu_mdcd_2019
where age_group = 6
group by zip3
order by zip3;

