

select distinct a.patid, b.uth_member_id
into dev.wc_sabella_exchange_ids
from optum_dod.mbr_enroll a 
  join data_warehouse.dim_uth_member_id b   
     on b.member_id_src = a.patid::text 
where health_exch in ('2','3')
and eligeff between '2015-01-01' and '2016-12-31';



select count(*), count(distinct uth_member_id) 
from dev.wc_sabella_exchange_ids


select a.uth_member_id  
into dev.wc_sabella_enrl15
from data_warehouse.member_enrollment_yearly a 
where a.age_derived between 18 and 64
  and a.year = 2015 
  and a.total_enrolled_months = 12 
  and a.uth_member_id in ( select uth_member_id from dev.wc_sabella_exchange_ids)
 ;


select * 
into dev.wc_sabella_cohorts
from data_warehouse.member_enrollment_yearly a 
where a.year = 2016
  and a.total_enrolled_months = 12 
  and a.uth_member_id in ( select uth_member_id from dev.wc_sabella_enrl15)
;

select count(*), count(distinct uth_member_id) from dev.wc_sabella_cohorts



select b.member_id_src::numeric 
  into dev.wc_sabella_patids
from dev.wc_sabella_cohorts a 
  join data_warehouse.dim_uth_member_id b   
     on a.uth_member_id = b.uth_member_id 

     
select * 
into dev.wc_sabella_mbr_enroll
from optum_dod.mbr_enroll me 
where me.patid in ( select member_id_src from dev.wc_sabella_patids)
and eligeff between '2015-01-01' and '2016-12-31';


select count(distinct patid) from dev.wc_sabella_mbr_enroll; 


select * 
into dev.wc_sabella_confinement
from optum_dod.confinement c 
where c.patid in ( select member_id_src from dev.wc_sabella_patids)
and c.admit_date between '2015-01-01' and '2016-12-31';

  
select * 
into dev.wc_sabella_diagnostic
from optum_dod.diagnostic d 
where d.patid in ( select member_id_src from dev.wc_sabella_patids)
and d.fst_dt between '2015-01-01' and '2016-12-31';  


select * 
into dev.wc_sabella_medical
from optum_dod.medical m 
where m.patid in ( select member_id_src from dev.wc_sabella_patids)
and m.fst_dt between '2015-01-01' and '2016-12-31';  


select * 
into dev.wc_sabella_procedure
from optum_dod."procedure" p 
where p.patid in ( select member_id_src from dev.wc_sabella_patids)
and p.fst_dt between '2015-01-01' and '2016-12-31';  


select * 
into dev.wc_sabella_rx
from optum_dod.rx r
where r.patid in ( select member_id_src from dev.wc_sabella_patids)
and r.fill_dt between '2015-01-01' and '2016-12-31';  

select * 
into dev.wc_sabella_lab
from optum_dod.lab_result l
where l.patid in ( select member_id_src from dev.wc_sabella_patids)
and l.fst_dt between '2015-01-01' and '2016-12-31';  
