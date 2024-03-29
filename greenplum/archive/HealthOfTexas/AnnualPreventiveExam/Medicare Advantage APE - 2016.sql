---Annual Preventive Exam (APE) medicare advantage - 2016 

---optum and truven cohorts from DW
drop table dev.wc_ape_mdcradv_2016;

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
 into dev.wc_ape_mdcradv_2016
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2016 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.bus_cd = 'MCR'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


delete from dev.wc_ape_mdcradv_2016 where length(zip3::text) = 2;

drop table dev.wc_ape_mdcradv_2016_vacc;

select distinct uth_member_id 
into dev.wc_ape_mdcradv_2016_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
      and a.year = 2016 
      and a.uth_member_id in ( select uth_member_id from dev.wc_ape_mdcradv_2016)
;


insert into dev.wc_ape_mdcradv_2016_vacc
select distinct uth_member_id 
from data_warehouse.claim_diag 
where diag_cd in ('Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419',
				  'V700','V700','V7231','V705','V703','V7284','V7285') 
      and year = 2016 
      and uth_member_id in ( select uth_member_id from dev.wc_ape_mdcradv_2016)
      and uth_member_id not in ( select uth_member_id from dev.wc_ape_mdcradv_2016_vacc)
;


alter table dev.wc_ape_mdcradv_2016 add column vacc_flag int2 default 0;


update dev.wc_ape_mdcradv_2016 a set vacc_flag = 1
  from dev.wc_ape_mdcradv_2016_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

------ Calculations ---------------------------

		    
----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  -- optum 
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float )as prev, count(uth_member_id ) as cnt, 'all' as grp 
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'optz'
union all
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, count(uth_member_id ) as cnt, gender_cd
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'optz'
group by a.gender_cd
) x 
order by grp
;



--prevalance all - row 51  -- truven
select * 
from (
select ( sum(vacc_flag) / count(uth_member_id)::float ) as prev, count(uth_member_id ) as cnt, 'all' as grp
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'truv'
union all 
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev,count(uth_member_id ) as cnt,  gender_cd
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'truv'
group by a.gender_cd 
) x 
order by grp
;
;


----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------

-- truven
select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, 
       count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'truv'
group by  a.zip3 
order by  a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, 
       count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'truv'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, 
       count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'truv'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
  

 -- optum weight  missing zip 753 772 
insert into dev.wc_ape_mdcradv_2016 values 
(0001, 753,null,'M','optz',0),
(0002, 772, null,'M','optz',0),
(0003, 753,null,'F','optz',0),
(0004, 772, null,'F','optz',0);

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, 
       count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'optz'
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, 
       count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'optz'
  and a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  as prev, 
       count(uth_member_id) as mems --, a.zip3 
from dev.wc_ape_mdcradv_2016 a 
where a.data_source = 'optz'
  and a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
