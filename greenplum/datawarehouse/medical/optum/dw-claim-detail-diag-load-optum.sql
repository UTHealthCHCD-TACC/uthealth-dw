

drop table if exists dev.claim_detail_diag_dw;

create table dev.claim_detail_diag_dw (
id bigserial NOT NULL,
	claim_detail_id int8,
	diag_code varchar,
	diag_position int
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence dev.claim_detail_diag_dw_id_seq cache 100;

--Optum load: 
insert into dev.claim_detail_diag_dw(claim_detail_id, diag_code, diag_position)
select distinct d.id, diag.diag, diag.diag_position
from dev.claim_detail_dw d
join dev.claim_header_dw h on d.claim_header_id=h.id
join optum_dod_diagnostic diag on diag.clmid=h.claim_id_src
limit 10;

select * from dev.claim_header_dw where id=745186;
select * from dev.claim_detail_dw where claim_header_id in (select distinct id from dev.claim_header_dw where source='OPTD')


select * from 
select *
from dev.claim_detail_dw d
join dev.claim_header_dw h on d.claim_header_id=h.id
where h.source='OPTD'
limit 10;

select source, count(*)
from dev.claim_header_dw
group by 1;


-- Diagnostics


--Verify
select source, count(*)
from data_warehouse.medical
group by 1;




