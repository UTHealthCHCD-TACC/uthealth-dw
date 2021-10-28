---indepressionenza vacc prevelance 2019 medicare 

---optum and truven cohorts from DW
drop table dev.wc_depression_mdcr_2019;

select distinct on (a.uth_member_id) 
      a.uth_member_id, 
       a.zip3, 
	   7 age_group,
       a.gender_cd, 
       a.data_source,
       case when b.uth_member_id is null then 0 else 1 end as vacc_flag 
 into dev.wc_depression_mdcr_2019
from data_warehouse.member_enrollment_yearly a
  join data_warehouse.medicare_mbsf_abcd_enrollment x
     on x.uth_member_id = a.uth_member_id 
   and x.bene_hi_cvrage_tot_mons = 12
   and x.bene_smi_cvrage_tot_mons > 0
   and x.year = a.year 
  left outer join dev.wc_depression_2019_vacc b 
     on b.uth_member_id = a.uth_member_id 
  left outer join dev.wc_depression_2019_exclusions c
     on c.uth_member_id = a.uth_member_id
where c.uth_member_id is null 
  and a.data_source = 'mcrt'
  and a.year = 2019 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  
select * 
from ( 
	select ( sum(vacc_flag) / count(uth_member_id)::float ) *100 as prev, count(uth_member_id), sum(vacc_flag), 'all' as grp 
	from dev.wc_depression_mdcr_2019 a 
	union 
	select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev, count(uth_member_id), sum(vacc_flag), gender_cd
	from dev.wc_depression_mdcr_2019 a 
	where a.gender_cd = 'F'
	group by gender_cd 
	union 
	select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev, count(uth_member_id), sum(vacc_flag), gender_cd 
	from dev.wc_depression_mdcr_2019 a 
	where a.gender_cd = 'M'
	group by gender_cd
) x 
order by grp 
;




----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------


select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev , a.zip3 
from dev.wc_depression_mdcr_2019 a 
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev 
from dev.wc_depression_mdcr_2019 a 
where a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )  *100 as prev 
from dev.wc_depression_mdcr_2019 a 
where a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


