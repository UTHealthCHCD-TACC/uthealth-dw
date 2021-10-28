/****** Medicaid Annual Prev Exam 2019 ******/

drop table dev.wc_mdcd_ape_2019;

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
into dev.wc_mdcd_ape_2019 
from medicaid.agg_enrl_medicaid_cy1220 a 
   join data_warehouse.dim_uth_member_id b 
      on b.member_id_src = a.client_nbr
where enrl_months = 12 
  and enrl_cy = 2019
  and age::float < 65.00
 and zip3::int between 750 and 799;
 
 
 select count(*), count(distinct uth_member_id)
 from dev.wc_mdcd_ape_2019	

 
delete from dev.wc_mdcd_ape_2019 where age_group is null;

delete from dev.wc_mdcd_ape_2019 where zip3 = '771';

delete from dev.wc_mdcd_ape_2019 where sex = 'U';

select count(*), age_group from dev.wc_mdcd_ape_2019  group by age_group order by age_group;


---------------------------------------------------------------------------------------------------------
----proc and hcpc
---------------------------------------------------------------------------------------------------------
drop table dev.wc_mdcd_ape_clm_2019 ;

select uth_member_id
  into dev.wc_mdcd_ape_clm_2019
from data_warehouse.claim_detail 
where data_source = 'mdcd' 
  and year = 2019
  and cpt_hcpcs_cd in ('99381','99382','99383','99384','99385','99386','99387',
					 '99391','99392','99393','99394','99395','99396','99397',
					 'S0610','S0612','S0615')
;


---------------------------------------------------------------------------------------------------------
----Diagnosis Codes
---------------------------------------------------------------------------------------------------------
insert into dev.wc_mdcd_ape_clm_2019
select uth_member_id 
from data_warehouse.claim_diag 
where data_source = 'mdcd' 
  and year = 2019
  and diag_cd in ('Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419',
				  'V700','V700','V7231','V705','V703','V7284','V7285') 
;


---------------------------------------------------------------------------------------------------------
----consolidate
--------------------------------------------------------------------------------------------------------
drop table dev.wc_mdcd_ape_mem_2019;

select distinct uth_member_id
into dev.wc_mdcd_ape_mem_2019
from dev.wc_mdcd_ape_clm_2019
;

-------------------------------------------------

alter table dev.wc_mdcd_ape_2019 add vacc_flag int default 0;


update dev.wc_mdcd_ape_2019 a set vacc_flag = 1
  from dev.wc_mdcd_ape_mem_2019 b 
    where a.uth_member_id = b.uth_member_id
 ;




----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, 'A' as measure, count(uth_member_id), sum(vacc_flag)
from dev.wc_mdcd_ape_2019
union
select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, 'F' as measure, count(uth_member_id), sum(vacc_flag)
from dev.wc_mdcd_ape_2019
where sex = 'F'
union
select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, 'M' as measure, count(uth_member_id), sum(vacc_flag)
from dev.wc_mdcd_ape_2019
where sex = 'M'
;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), age_group
from dev.wc_mdcd_ape_2019
group by age_group
order by age_group;


--- prevalance by zip 

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where sex = 'F'
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where sex = 'M'
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where age_group = 1
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where age_group = 2
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where age_group = 3
group by zip3
order by zip3;

--4
select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where age_group = 4
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where age_group = 5
group by zip3
order by zip3;

select ( cast(sum(vacc_flag) as float) / cast(count(uth_member_id) as float ) )*100 as prev, count(uth_member_id), sum(vacc_flag), zip3 
from dev.wc_mdcd_ape_2019
where age_group = 6
group by zip3
order by zip3;

