331.0, ICD-10 Diagnosis Code G30, G30.0, G30.1, G30.8, G30.9



select uth_member_id 
into dev.wc_yiji_alz_diags
from data_warehouse.claim_diag 
where extract(year from from_date_of_service) between 2012 and 2017
  and data_source = 'optd'
  and diag_cd in ('3310','G30','G300','G301','G308','G309')
  ;
 
 drop table if exists  dev.wc_yiji_neck_fracture;
  
 select uth_member_id, from_date_of_service , cpt_hcpcs_cd 
 into dev.wc_yiji_neck_fracture
 from data_warehouse.claim_detail 
where extract(year from from_date_of_service) between 2012 and 2017
  and data_source = 'optd'
  and cpt_hcpcs_cd in ('82000','82001','82002','82003','82004','82005','82006','82007','82008','82009',
'8208', '82010', '82011', '82012', '82013', '82019',
'S720' ,'S7200', 'S7201', 'S7202', 'S7203', 'S7204', 'S7205', 'S7206',
'S7209', 'S72001A', 'S72002A', 'S72031A', 'S72032A', 'S72034A', 'S72035A', 
'S72041A', 'S72042A','S72044A', 'S72045A')
;

select count(*), count(distinct uth_member_id) from dev.wc_yiji_neck_fracture


 select uth_member_id 
 into dev.wc_yiji_neck_fracture_exclusions
 from data_warehouse.claim_detail 
where extract(year from from_date_of_service) = 2011
  and data_source = 'optd'
  and cpt_hcpcs_cd in ('82000','82001','82002','82003','82004','82005','82006','82007','82008','82009',
'8208', '82010', '82011', '82012', '82013', '82019',
'S720' ,'S7200', 'S7201', 'S7202', 'S7203', 'S7204', 'S7205', 'S7206',
'S7209', 'S72001A', 'S72002A', 'S72031A', 'S72032A', 'S72034A', 'S72035A', 
'S72041A', 'S72042A','S72044A', 'S72045A')
;

drop table if exists dev.wc_yiji_cohort ;

select distinct u.member_id_src, n.from_date_of_service, n.cpt_hcpcs_cd  
into dev.wc_yiji_cohort 
from dev.wc_yiji_alz_diags a 
   join dev.wc_yiji_neck_fracture n 
      on a.uth_member_id = n.uth_member_id 
   join data_warehouse.dim_uth_member_id u 
     on a.uth_member_id = u.uth_member_id 
where a.uth_member_id not in ( select b.uth_member_id from dev.wc_yiji_neck_fracture_exclusions b)
  ;
  
 from optum_dod.confinement a 
 
  from optum_dod.medical a 
 
 from optum_dod.diagnostic a 
 
 from optum_dod."procedure" a 
 
 from optum_dod.mbr_enroll_r a 
   left outer join optum_dod.mbrwdeath d 
      on a.patid  = d.patid 
 
 
from optum_dod.rx a 

from optum_dod.provider  a 
