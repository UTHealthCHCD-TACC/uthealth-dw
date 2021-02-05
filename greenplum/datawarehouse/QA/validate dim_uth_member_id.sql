select count(*), count(distinct uth_member_id), data_source
from data_warehouse.dim_uth_member_id dumi 
group by  data_source
order by  data_source
;


select count(distinct uth_member_id), data_source
from data_warehouse.member_enrollment_monthly
group by  data_source
order by  data_source
;


select count(distinct uth_member_id), data_source
from data_warehouse.member_enrollment_yearly 
group by  data_source
order by  data_source
;


select count(distinct bene_id), 'mcrt' as ds  from medicare_texas.mbsf_abcd_summary
union 
select count(distinct bene_id), 'mcrn' from medicare_national.mbsf_abcd_summary
union 
select count(distinct patid), 'optd' from optum_dod.mbr_enroll_r 
union
select count(distinct patid), 'optz' from optum_zip.mbr_enroll
union
select count(distinct enrolid), 'truv' from truven.ccaet
union 
select count(distinct enrolid), 'truv ma' from truven.mdcrt;




----rx validation
select count(*), data_source , "year" 
from data_warehouse.dim_uth_rx_claim_id a 
where data_source = 'truv'
group by a.data_source , a.year 
order by data_source , "year" 


select count(distinct r.clmid), year 
from optum_dod.rx r 
group by r."year" 
order by r."year" 
;


select count(*), count(distinct uth_rx_claim_id) as uniq, data_source, data_year 
from data_warehouse.pharmacy_claims pc 
where data_source = 'truv'
group by data_source ,data_year 
order by data_source , data_year 
;


select count(*), count(distinct  a.enrolid || ndcnum::text || svcdate::text), a.year 
from truven.ccaed a 
group by a.year 
order by a.year 
;


select count(*), year 
from medicare_texas.admit_clm 
group by "year" 
order by year 

