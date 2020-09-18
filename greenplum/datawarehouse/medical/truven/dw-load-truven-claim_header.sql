-- Claim Header
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 16min

insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						              
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'ccaeo'
from truven.ccaeo a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
where a.year  = 2019
;


select count(*) , year
from data_warehouse.claim_header 
where data_source  = 'truv'
group by year ;

---------------------------------------------------------------------------------------------------
-------------------------------- truven medicare outpatient ---------------------------------------
---------------------------------------------------------------------------------------------------		        
		        
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						            
select distinct on (uth_claim_id) 
	   'truv',, extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcro'   
from truven.mdcro a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
where year = 2019
;

vacuum analyze data_warehouse.claim_header;


delete from data_warehouse.claim_header where table_id_src in ('ccaes','mdcrs');

-------------------------------- truven commercial inpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 8m
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)
select distinct on (uth_claim_id) 
'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, trunc(a.caseid,0)::text,
null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
a.msclmid, a.enrolid, 'ccaes'
from truven.ccaes a
join data_warehouse.dim_uth_claim_id b 
on b.data_source = 'truv'
and b.claim_id_src = a.msclmid::text
and b.member_id_src = a.enrolid::text
where year = 2019
;


-------------------------------- truven medicare advantage inpatient------------------------------
---------------------------------------------------------------------------------------------------	
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  								        						              
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, trunc(a.caseid,0)::text,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcrs'
from truven.mdcrs a
  join data_warehouse.dim_uth_claim_id b 
    on b.data_source ='truv'
   and b.claim_id_src = a.msclmid::text
   and b.member_id_src = a.enrolid::text
where year = 2019
;


vacuum analyze data_warehouse.claim_header;


select count(*), data_source, year 
from data_warehouse.claim_header 
group by data_source , year 
order by data_source , year 

----this will eliminate duplicate records from claim header and put them aside for further research
drop table quarantine.duplicate_claim_headers ;

insert into quarantine.duplicate_claim_headers
select uth_claim_id, data_source , year 
from (
	select count(*) as rc, uth_claim_id, data_source, year 
	from data_warehouse.claim_header 
	group by uth_claim_id, data_source, year
	) a 
where rc > 1


vacuum analyze quarantine.duplicate_claim_headers; 

select count(*), data_source, year  
from quarantine.duplicate_claim_headers
group by data_source, year ;

delete from data_warehouse.claim_header where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers where year = 2019);

delete from data_warehouse.claim_detail where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers where year = 2019);

delete from data_warehouse.claim_diag where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers where year = 2019);

delete from data_warehouse.claim_icd_proc where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_claim_headers);