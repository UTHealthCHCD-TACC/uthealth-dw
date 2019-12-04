

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

--trvc load: 
insert into dev.claim_detail_diag_dw(claim_detail_id, diag_code, diag_position)
select distinct d.id, diag.dx4, 4
from dev.claim_detail_dw d
join dev.claim_header_dw h on d.claim_header_id=h.id
join truven_ccaeo diag on h.claim_id_src=split_part(diag.msclmid::text, '.', 1) and h.member_id_src=split_part(diag.enrolid::text, '.', 1);

select * from dev.claim_header_dw where id=745186;
select * from dev.claim_detail_dw where claim_header_id in (select distinct id from dev.claim_header_dw where source='OPTD')

select * from truven_ccaeo where msclmid::text='746287';

select * from 
select *
from dev.claim_detail_dw d
join dev.claim_header_dw h on d.claim_header_id=h.id
where h.source='TRUV'
limit 10;

select source, count(*)
from dev.claim_header_dw
group by 1;


-- Diagnostics


--Verify
select source, count(*)
from data_warehouse.medical
group by 1;




