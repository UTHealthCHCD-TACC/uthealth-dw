select usename, (current_timestamp - query_start)::time as runtime, query, pid, *
from pg_stat_activity 
where state = 'active'


select pg_cancel_backend(136638);

--select pg_terminate_backend(86270);

grant uthealth_analyst to oaborisa;


select count(*) 
from data_warehouse.admission a 
where user = 123;



select *
from data_warehouse.claim_detail a 
  join data_warehouse.member_enrollment_yearly b 
    on a.uth_member_id = b.uth_member_id 
 where a.uth_member_id = 12035
 ;
 

select * 
from data_warehouse.dim_uth_member_id d


select * 
from optum_zip.mbr_enroll me 
where patid = 560499825482340
;


select * 
from data_warehouse.member_enrollment_yearly 
where uth_member_id = 1038789426
;


select * 
from data_warehouse.claim_diag cd 
where diag_cd  = 'Z11';



