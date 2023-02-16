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


--require 24 months CE
delete
from dev.wc_bc_com_2015 a
where not exists ( select 1 
               from data_warehouse.member_enrollment_yearly b 
               where b.uth_member_id = a.uth_member_id
                 and b.year = 2015 - 1 
                 and b.total_enrolled_months = 12)
;

-----


---exclusions icdproc
select distinct b.uth_member_id 
into dev.wc_bc_exclusions_2015
from data_warehouse.claim_icd_proc a
  join dev.wc_bc_com_2015 b 
    on a.uth_member_id = b.uth_member_id
where proc_cd in ('8542','0HTV0ZZ','0HT0ZZ','8544','8546','8548')
  and a.year <= 2015
;

---exclusions diag
insert into dev.wc_bc_exclusions_2015 
select distinct a.uth_member_id 
from data_warehouse.claim_diag a 
join dev.wc_bc_com_2015 b 
  on a.uth_member_id = b.uth_member_id 
 where a.diag_cd = 'Z9013' 
  and a.year <= 2015
 ;


----cpt exclusions, appearing twice
insert into dev.wc_bc_exclusions_2015 
select uth_member_id 
from ( 
	select count(distinct uth_claim_id) as cnt, a.uth_member_id, year 
	from data_warehouse.claim_detail a
	  join dev.wc_bc_com_2015 b 
	    on a.uth_member_id = b.uth_member_id 
	where a.cpt_hcpcs in ('19180','19200','19220','19303','19304','19305','19306','19307')
	  and a.year <= 2015 
	group by a.uth_member_id, a.year 
) inr 
where cnt > 1;


----remove exceptions
delete from dev.wc_bc_com_2015 where uth_member_id in ( select uth_member_id from dev.wc_bc_exclusions_2015 );


---find breast cancer
a.cpt_hcpcs in ('19180','19200','19220','19303','19304','19305','19306','19307',
                '77055','77056','77057','77061','77062','77063','77064','77065',
                '77066','77067','G0202','G0204','G0206')
