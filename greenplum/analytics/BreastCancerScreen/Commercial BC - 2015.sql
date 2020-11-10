---Commercial Breast Cancer (BC) - 2015 

---optum and truven cohorts from DW
drop table dev.wc_bc_com_2015;

select uth_member_id, 
       a.zip3, 
	   case  when a.age_derived between 0  and 17 then 1 
	         when a.age_derived between 18 and 29 then 2
	         when a.age_derived between 30 and 39 then 3
	         when a.age_derived between 40 and 49 then 4
	         when a.age_derived between 50 and 59 then 5 
	         when a.age_derived between 60 and 64 then 6 
	         else 7
	   end as age_group,
       a.gender_cd, 
       data_source 
 into dev.wc_bc_com_2015
from data_warehouse.member_enrollment_yearly a
where a.data_source in ('truv','optz')
  and a.year = 2015 
  and a.state = 'TX'
  and a.zip3 between '750' and '799'
  and a.age_derived between 50 and 74 
  and a.bus_cd = 'COM'
  and a.total_enrolled_months = 12
  and a.gender_cd = 'F'
;

delete
from dev.wc_bc_com_2015 a
where not exists ( select 1 
               from data_warehouse.member_enrollment_yearly b 
               where b.uth_member_id = a.uth_member_id
                 and b.year = 2015 - 1 
                 and b.total_enrolled_months = 12)
;

-----


select *
from data_warehouse.claim_diag
where diag_cd in ('0HTV0ZZ')

select distinct b.uth_member_id 
into dev.wc_bc_exclusions_2015
from data_warehouse.claim_icd_proc a
  join dev.wc_bc_com_2015 b 
    on a.uth_member_id = b.uth_member_id
where proc_cd in ('8542','0HTV0ZZ','0HT0ZZ','8544','8546','8548')
  and a.year <= 2015
;

select * 
from data_warehouse.claim_detail c
where c.proc_cd = '19180'
;

