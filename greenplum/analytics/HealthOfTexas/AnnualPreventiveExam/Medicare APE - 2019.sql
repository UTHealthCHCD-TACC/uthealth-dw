---Annual Preventive Exam (APE) - 2020 

--- cohorts from DW
drop table dev.wc_ape_mdcr_2020;

select a.uth_member_id, 
       a.zip3, 
	   7 as age_group,
       a.gender_cd, 
       a.data_source 
 into dev.wc_ape_mdcr_2020
from data_warehouse.member_enrollment_yearly a
  join data_warehouse.medicare_mbsf_abcd_enrollment b 
     on b.uth_member_id = a.uth_member_id 
   and b.bene_hi_cvrage_tot_mons = 12
   and b.bene_smi_cvrage_tot_mons > 0
   and b.year = a.year 
where a.data_source = 'mcrt'
  and a.year = 2020 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.total_enrolled_months = 12
  and a.gender_cd in ('M','F')
;

delete from dev.wc_ape_mdcr_2020 where length(zip3::text) = 2


select count(*) from dev.wc_ape_mdcr_2020


drop table dev.wc_ape_mdcr_2020_vacc

select distinct uth_member_id 
into dev.wc_ape_mdcr_2020_vacc
from data_warehouse.claim_detail a
where a.cpt_hcpcs_cd in ('99381','99382','99383','99384','99385','99386','99387',
						 '99391','99392','99393','99394','99395','99396','99397',
						 'S0610','S0612','S0615')
      and a.year = 2020 
      and a.uth_member_id in ( select uth_member_id from dev.wc_ape_mdcr_2020)
;


insert into dev.wc_ape_mdcr_2020_vacc
select distinct uth_member_id 
from data_warehouse.claim_diag 
where diag_cd in ('Z0000','Z0001','Z00110','Z00111','Z00121','Z00129','Z003','Z01411','Z01419',
				  'V700','V700','V7231','V705','V703','V7284','V7285') 
      and extract(year from from_date_of_service) = 2020 
      and uth_member_id in ( select uth_member_id from dev.wc_ape_mdcr_2020)
      and uth_member_id not in ( select uth_member_id from dev.wc_ape_mdcr_2020_vacc)
;



alter table dev.wc_ape_mdcr_2020 add column vacc_flag int2 default 0;


update dev.wc_ape_mdcr_2020 a set vacc_flag = 1
  from dev.wc_ape_mdcr_2020_vacc b 
    where b.uth_member_id = a.uth_member_id
 ;

		    
----------------------------------------------------------------------------------------
---********************** Prevalance All **************************
----------------------------------------------------------------------------------------

--prevalance all - row 51  
select ( sum(vacc_flag) / count(uth_member_id)::float )*100 as prev, 'A' as cat, count(uth_member_id), sum(vacc_flag)
from dev.wc_ape_mdcr_2020 a 
union 
select ( sum(vacc_flag) / count(uth_member_id)::float )*100  as prev, 'F' as cat, count(uth_member_id), sum(vacc_flag)
from dev.wc_ape_mdcr_2020 a 
where a.gender_cd = 'F'
union 
select ( sum(vacc_flag) / count(uth_member_id)::float ) *100 as prev, 'M' as cat, count(uth_member_id), sum(vacc_flag)
from dev.wc_ape_mdcr_2020 a 
where a.gender_cd = 'M'
;

----------------------------------------------------------------------------------------
---********************** Prevalance by ZIP **************************
----------------------------------------------------------------------------------------

select ( sum(vacc_flag) / count(uth_member_id)::float ) *100  as prev --, a.zip3 
from dev.wc_ape_mdcr_2020 a 
group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) *100 as prev --, a.zip3 
from dev.wc_ape_mdcr_2020 a 
where a.gender_cd = 'F'
  group by a.zip3 
order by a.zip3
;

select ( sum(vacc_flag) / count(uth_member_id)::float ) *100 as prev --, a.zip3 
from dev.wc_ape_mdcr_2020 a 
where a.gender_cd = 'M'
  group by a.zip3 
order by a.zip3
;
