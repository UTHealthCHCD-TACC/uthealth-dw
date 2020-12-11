----2018 and 2019 ambulance claims for TEXAS
drop table dev.wc_ambulance_clm_detail;

select * 
into dev.wc_ambulance_clm_detail
from (
	select a.uth_member_id , a.uth_claim_id , a.data_source , a."year" , case when procedure_cd in ('A0430','A0431') then 'A' else 'G' end as air_ground, b.bus_cd 
	from data_warehouse.claim_detail a 
		join data_warehouse.member_enrollment_yearly b  
		 on a.uth_member_id = b.uth_member_id 
	     and a.year = b.year 
	     and b.bus_cd in ('MDCR','MCR')
	     and b.state = 'TX'
	where a.data_source in ('truv','mcrt')
	and a.year between 2018 and 2019 
	and procedure_cd in ('A0225','A0422','A0424','A0425','A0426','A0427','A0428','A0429','A0430','A0431',
	                     'A0432','A0433','A0434','A0435','A0436','A0998','A0999','T2003','T2003') 
union 
	select a.uth_member_id , a.uth_claim_id , a.data_source , a."year" , case when procedure_cd in ('A0430','A0431') then 'A' else 'G' end as air_ground, b.bus_cd 
	from data_warehouse.claim_detail a 
	    join data_warehouse.member_enrollment_yearly b  
	      on a.uth_member_id = b.uth_member_id 
	     and a.year = b.year 
	     and b.bus_cd = 'COM'
	     and b.state = 'TX'
	where a.data_source in ('truv','optz') 
	and a.year between 2018 and 2019 
	and procedure_cd in ('A0225','A0422','A0424','A0425','A0426','A0427','A0428','A0429','A0430','A0431',
	                     'A0432','A0433','A0434','A0435','A0436','A0998','A0999','T2003','T2003') 
)  inr;
                     

--get 1 claim id and air ground flag
drop table dev.wc_ambulance_claims;

select uth_member_id , uth_claim_id , data_source , "year", bus_cd, min(air_ground) as ag_flag
into dev.wc_ambulance_claims
from dev.wc_ambulance_clm_detail 
group by uth_member_id , uth_claim_id , data_source , "year",bus_cd
;


select count(*), count(distinct uth_claim_id), year  
from dev.wc_ambulance_claims
group by year;
        

--analytics
select a.data_source , a."year" , b.bus_cd, b.ag_flag,
       count(*) as total_records,
       count(distinct a.uth_claim_id) as clms, 
       count(distinct a.uth_member_id) as mems, 
       sum(a.total_charge_amount) as total_charge,
       sum(a.total_allowed_amount) as total_allowed 
--into dev.wc_ambulance_clm_hdr 
from data_warehouse.claim_header a
   join  dev.wc_ambulance_claims  b 
      on b.uth_claim_id = a.uth_claim_id 
where a.claim_type = 'P'
group by a.data_source , a.year , b.bus_cd, b.ag_flag
order by a.data_source , a.year , b.bus_cd, b.ag_flag
;

select * 
from data_warehouse.claim_header a
where uth_claim_id in ( select uth_claim_id from dev.wc_ambulance_claims)
and data_source = 'truv';


vacuum analyze data_warehouse.member_enrollment_yearly-- mey where data_source = 'mcrt';
