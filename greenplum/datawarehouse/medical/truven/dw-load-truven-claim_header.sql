-- Claim Header

alter table data_warehouse.claim_header add column data_year int2;

--clean up if needed
delete from data_warehouse.claim_header where data_source = 'truv';

--create copy with matching distribution key
create table dev.truven_dim_uth_claim_id
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from data_warehouse.dim_uth_claim_id
where data_source = 'truv'
distributed by(member_id_src);

vacuum analyze dev.truven_dim_uth_claim_id;

drop table dev.truven_dim_uth_claim_id;

---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		


insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type,  uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, 
						        claim_id_src, member_id_src, table_id_src, data_year)  						              
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'ccaeo', a.year 
from truven.ccaeo a
  join dev.truven_dim_uth_claim_id b
--  join data_warehouse.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
 where a.year = 2019
;



---------------------------------------------------------------------------------------------------
-------------------------------- truven medicare outpatient ---------------------------------------
---------------------------------------------------------------------------------------------------		        

--clm hdr truv mdcr outpatient 
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year)  						            
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof,null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcro' , a.year 
from truven.mdcro a
  join dev.truven_dim_uth_claim_id b
--  join data_warehouse.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source = 'truv'
 and a.year between 2015 and 2019
;

SELECT facprof FROM truven.mdcro;

-------------------------------- truven commercial inpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type,uth_admission_id, admission_id_src,
total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year)
select distinct on (uth_claim_id) 
'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, null, trunc(a.caseid,0)::text,
null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
a.msclmid, a.enrolid, 'ccaes', a."year" 
from truven.ccaes a
  join dev.truven_dim_uth_claim_id b
--  join data_warehouse.dim_uth_claim_id b  
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source = 'truv'
;


-------------------------------- truven medicare advantage inpatient------------------------------
---------------------------------------------------------------------------------------------------	
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year)  								        						              
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, null, trunc(a.caseid,0)::text,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcrs', a.year
from truven.mdcrs a
  join dev.truven_dim_uth_claim_id b
--  join data_warehouse.dim_uth_claim_id b  
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source = 'truv'
;


vacuum analyze data_warehouse.claim_header;


select count(*), year --count(distinct uth_claim_id ), table_id_src, year 
from data_warehouse.claim_header 
where data_source = 'truv'
group by year --table_id_src , year 
order by year-- table_id_src , year 
;


---claim load clean
drop table dev.truven_dim_uth_claim_id;


----this will identify duplicate records from claim header if needed
drop table dev.wc_temp_truv_hdr ;

select uth_claim_id,  year 
 into dev.wc_temp_truv_hdr
from (
	select count(*) as rc, uth_claim_id, year 
	from data_warehouse.claim_header 
	where data_source = 'truv'
	group by uth_claim_id, year
	) a 
where rc > 1
;


drop table quarantine.duplicate_truv_claims 

select * -- uth_claim_id, table_id_src , uth_member_id
--into quarantine.duplicate_truv_claims
from data_warehouse.claim_header 
where uth_claim_id in ( select uth_claim_id from dev.wc_temp_truv_hdr )
order by uth_claim_id


select * from data_warehouse.claim_detail cd where uth_claim_id = 15768744356
 
select count(*), table_id_src
from quarantine.duplicate_truv_claims
group by table_id_src


delete from data_warehouse.claim_header where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);

delete from data_warehouse.claim_detail where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);

delete from data_warehouse.claim_diag where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);

delete from data_warehouse.claim_icd_proc where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);

