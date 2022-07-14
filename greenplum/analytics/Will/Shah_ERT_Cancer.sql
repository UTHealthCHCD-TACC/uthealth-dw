drop table if exists dev.wc_shah_medical ;

select a.uth_member_id, b.member_id_src, min(a.from_date_of_service) as cancer_date  
into dev.wc_shah_diagnosis
from data_warehouse.claim_diag a 
  join data_warehouse.dim_uth_member_id b 
     on a.uth_member_id = b.uth_member_id 
where a.data_source = 'optd'
  and extract(year from a.from_date_of_service) between 2015 and 2020 
  and (
        a.diag_cd like 'C64%' or a.diag_cd in ('1890','Z8552') 
      )
group by a.uth_member_id, b.member_id_src 
;      


select a.* , m.death_ym 
into dev.wc_shah_mbr_enroll
from optum_dod.mbr_enroll_r a 
   left outer join optum_dod.mbrwdeath m 
      on m.member_id_src = a.member_id_src 
   join dev.wc_shah_diagnosis b 
     on a.member_id_src = b.member_id_src 
    and a.eligend >= b.cancer_date 
 ;
 

select a.* 
into dev.wc_shah_medical 
from optum_dod.medical a 
   join dev.wc_shah_diagnosis b 
     on a.member_id_src = b.member_id_src 
where a.year between 2015 and 2020 
;



select a.* 
into dev.wc_shah_confinement 
from optum_dod.confinement a 
   join dev.wc_shah_diagnosis b 
     on a.member_id_src = b.member_id_src 
where a.year between 2015 and 2020 
;


select a.* 
into dev.wc_shah_diagnostic
from optum_dod.diagnostic a 
   join dev.wc_shah_diagnosis b 
     on a.member_id_src = b.member_id_src 
where a.year between 2015 and 2020 
;



select a.* 
into dev.wc_shah_lab_result
from optum_dod.lab_result a 
   join dev.wc_shah_diagnosis b 
     on a.member_id_src = b.member_id_src 
where a.year between 2015 and 2020 
;


select a.* 
into dev.wc_shah_procedure 
from optum_dod."procedure"  a 
   join dev.wc_shah_diagnosis b 
     on a.member_id_src = b.member_id_src 
where a.year between 2015 and 2020 
;


select a.* 
into dev.wc_shah_rx
from optum_dod.rx a 
   join dev.wc_shah_diagnosis b 
     on a.member_id_src = b.member_id_src 
where a.year between 2015 and 2020 
;


select a.*, b.npi
into dev.wc_shah_provider 
from optum_dod.provider a 
  join optum_dod.provider_bridge b 
    on a.prov_unique  = a.prov_unique 
;

