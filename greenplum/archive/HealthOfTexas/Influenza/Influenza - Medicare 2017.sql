---influenza vacc prevelance 2017 medicare 

---optum and truven cohorts from DW
drop table dev.wc_flu_mdcr_2017;

select a.uth_member_id, 
       a.zip3, 
	   7 age_group,
       a.gender_cd, 
       a.data_source 
 into dev.wc_flu_mdcr_2017
from data_warehouse.member_enrollment_yearly a
  join data_warehouse.medicare_mbsf_abcd_enrollment b 
     on b.uth_member_id = a.uth_member_id 
   and b.bene_hi_cvrage_tot_mons = 12
   and b.bene_smi_cvrage_tot_mons > 0
   and b.year = a.year 
where a.data_source = 'mdcr'
  and a.year = 2017 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;


drop table dev.wc_flu_mdcr_2017_vacc;

select distinct uth_member_id 
into dev.wc_flu_mdcr_2017_vacc
from data_warehouse.claim_detail a
where a.procedure_cd in ('90630','90654','90655','90655','90656','90657','90658','90658','90660','90661','90662',
		  '90672','90672','90673','90674','90682','90685','90685','90686','90687','90688','90756','90756',
		  '90653','90657','90658','90658','G0008','Q2034','Q2035','Q2036','Q2037','Q2038','Q2039','G8482')
      and a.year = 2017 
      and a.uth_member_id in ( select uth_member_id from dev.wc_flu_mdcr_2017)
;


alter table dev.wc_flu_mdcr_2017 add column vacc_flag int2 default 0;


update dev.wc_flu_mdcr_2017 a set vacc_flag = 1
  from dev.wc_flu_mdcr_2017_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

select count(*), sum(vacc_flag), data_source, count(distinct uth_member_id) as mem
from dev.wc_flu_mdcr_2017 
group by data_source;



------ Calculations ---------------------------

----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  
select ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_mdcr_2017 a 
;

select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_mdcr_2017 a 
where a.gender_cd = 'F'
;

select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev, count(uth_member_id), sum(vacc_flag)
from dev.wc_flu_mdcr_2017 a 
where a.gender_cd = 'M'
;




----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------


select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev , a.zip3 
from dev.wc_flu_mdcr_2017 a 
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev 
from dev.wc_flu_mdcr_2017 a 
where a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;


select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev 
from dev.wc_flu_mdcr_2017 a 
where a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;


