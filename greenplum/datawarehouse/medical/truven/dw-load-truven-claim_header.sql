-- Claim Header
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 16min

create table dev.truven_ccaeo
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from truven.ccaeo
distributed by(enrolid);

vacuum analyze dev.truven_ccaeo;


create table dev.truven_dim_uth_claim_id
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from data_warehouse.dim_uth_claim_id
where data_source = 'truv'
distributed by(member_id_src);

vacuum analyze dev.truven_dim_uth_claim_id;



insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						              
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'ccaeo'
from dev.truven_ccaeo a 
--from truven.ccaeo a
  join data_warehouse.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.data_source = 'truv'
   and b.claim_id_src = a.msclmid::text
;


select * from truven.ccaeo;



---------------------------------------------------------------------------------------------------
-------------------------------- truven medicare outpatient ---------------------------------------
---------------------------------------------------------------------------------------------------		        
create table dev.truven_mdcro
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from truven.mdcro
distributed by(enrolid);		


insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						            
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, null,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcro'   
from dev.truven_mdcro a 
--from truven.mdcro a
  join data_warehouse.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source = 'truv'
;


-------------------------------- truven commercial inpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 8m
create table dev.truven_ccaes
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from truven.ccaes
distributed by(enrolid);	

insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)
select distinct on (uth_claim_id) 
'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, trunc(a.caseid,0)::text,
null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
a.msclmid, a.enrolid, 'ccaes'
from dev.truven_ccaes a 
--from truven.ccaes a
  join data_warehouse.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source = 'truv'
;


-------------------------------- truven medicare advantage inpatient------------------------------
---------------------------------------------------------------------------------------------------	
create table dev.truven_mdcrs
with(appendonly=true,orientation=column,compresstype=zlib)
as select *
from truven.mdcrs
distributed by(enrolid);	


insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  								        						              
select distinct on (uth_claim_id) 
	   'truv', extract(year from a.svcdate), b.uth_claim_id, b.uth_member_id, a.svcdate, a.facprof, trunc(stdplac,0)::text, null, trunc(a.caseid,0)::text,
        null, sum(a.pay) over(partition by b.uth_claim_id), sum(a.netpay) over(partition by b.uth_claim_id), 
        a.msclmid, a.enrolid, 'mdcrs'
from dev.truven_mdcrs a 
--from truven.mdcrs a
  join data_warehouse.dim_uth_claim_id b 
    on b.member_id_src = a.enrolid::text
   and b.claim_id_src = a.msclmid::text
   and b.data_source = 'truv'
;


vacuum analyze data_warehouse.claim_header;


select count(*), count(distinct uth_claim_id ), year -- table_id_src, year 
from data_warehouse.claim_header 
where data_source = 'truv'
group by year --table_id_src , year 
order by year-- table_id_src , year 
;


select * from truven.ccaef where caseid = 387075

select count(*)
from data_warehouse.claim_header a
where data_source = 'truv' 
and year = 2019
and uth_claim_id in (   select uth_claim_id 
						from data_warehouse.claim_header b
						where a.uth_claim_id = b.uth_claim_id 
						  and a.table_id_src <> b.table_id_src );
						 
						 
						 select * from data_warehouse.claim_header where uth_claim_id = 15776138580


----this will eliminate duplicate records from claim header and put them aside for further research
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

select uth_claim_id, table_id_src , uth_member_id
into quarantine.duplicate_truv_claims
from data_warehouse.claim_header 
where uth_claim_id in ( select uth_claim_id from dev.wc_temp_truv_hdr )
  and table_id_src in( 'ccaes','mdcrs'); 

 
 select count(*) from quarantine.duplicate_truv_claims
 
delete from data_warehouse.claim_header where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);

delete from data_warehouse.claim_detail where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);

delete from data_warehouse.claim_diag where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);

delete from data_warehouse.claim_icd_proc where uth_claim_id in ( select uth_claim_id from quarantine.duplicate_truv_claims);


---claim load clean
drop table dev.truven_ccaeo;

drop table dev.truven_ccaes;

drop table dev.truven_mdcro;

drop table dev.truven_mdcrs;



