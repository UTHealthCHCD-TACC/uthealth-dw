CREATE OR REPLACE VIEW qa_reporting.enrollment_raw_counts
as



--enrollment
	select count(distinct patid) , 'optd' as data_source
	from optum_dod.mbr_enroll_r 
union 
	select count(distinct patid), 'optz' 
	from optum_zip.mbr_enroll 
union 
	select count(distinct bene_id), 'mcrt' 
	from uthealth/medicare_national.mbsf_abcd_summary 
union 
	select count(distinct bene_id), 'mcrn'
	from medicare_national.mbsf_abcd_summary 
union 
    select count(distinct client_nbr), 'mdcd' 
        from 
        (
	        select client_nbr from medicaid.enrl 
	        union all 
	        select client_nbr from medicaid.chip_uth 
        ) inrx 
union 
	select count(distinct enrolid), 'truv'
	from 
		(
		select enrolid from truven.ccaet 
		union all 
		select enrolid from truven.mdcrt 
	    ) inr 
;


select count(distinct uth_member_id), data_source 
from data_warehouse.dim_uth_member_id 
group by data_source  order by data_source 
;

select * from data_warehouse.member_enrollment_yearly mey 



select count(distinct uth_member_id) 
from data_warehouse.member_enrollment_monthly m
where data_source = 'optd'
;


		select count(distinct enrolid) from truven.ccaea 
		
		
		select count(distinct enrolid) from truven.mdcrt 



select version();


select count(distinct patid::text || clmid)
from optum_dod.medical m 


select a.*
from optum_dod.medical a 
  join optum_dod.medical b  
     on a.clmid = b.clmid 
    and a.patid = b.patid 
    and a."year" <> b."year" 

    
    select * from optum_dod.medical m where clmid = '1750335963' and patid = 33041182690 order by year;

select count(distinct uth_claim_id) 
from data_warehouse.dim_uth_claim_id  
where data_source = 'optd'

;


select m.* 
from optum_dod.medical m 
  left outer join data_warehouse.dim_uth_claim_id b 
     on b.claim_id_src = m.clmid 
    and b.member_id_src = m.patid::text 
 where b.uth_claim_id is null 
   and b.data_source = 'optd'
   ;


select b.* 
from  data_warehouse.dim_uth_claim_id b
   join optum_dod.medical m 
     on b.claim_id_src = m.clmid 
    and b.member_id_src = m.patid::text 
 where m.clmid is null 
   and b.data_source = 'optd'
   ;
