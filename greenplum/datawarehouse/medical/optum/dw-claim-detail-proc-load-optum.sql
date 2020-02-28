
analyze dev.claim_header_optum;
analyze dev.claim_detail_optum;

drop table if exists dev.claim_detail_proc_optum;

create table dev.claim_detail_proc_optum (
id bigserial NOT NULL,
	uth_claim_id int8,
	claim_sequence_number int2,
	proc_code text,
	proc_sequence int2
) 
WITH (appendonly=true, orientation=column)
distributed by (uth_claim_id);

--Optum load: 
insert into dev.claim_detail_proc_optum(uth_claim_id, claim_sequence_number, proc_code, proc_sequence)
select distinct uth.uth_claim_id, d.claim_sequence_number, proc.proc, proc.proc_position
from dev.claim_detail_optum d
join dev.claim_header_optum h on d.uth_claim_id=h.uth_claim_id
join data_warehouse.dim_uth_claim_id uth on h.uth_claim_id=uth.uth_claim_id
join optum_dod.procedure proc on proc.clmid=h.claim_id_src and proc.patid::text=h.member_id_src and proc.year=uth.data_year
where h.data_source='optd';

limit 10;

-- Diagnostics

analyze dev.claim_detail_proc_optum;

--Verify
select data_source, count(*), count(distinct d.uth_claim_id)
from dev.claim_detail_proc_optum d
join dev.claim_detail_optum l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id
group by 1;

select data_source, count(*)
from dev.claim_header_optum
group by 1;

--Missing diags???
select distinct h.uth_claim_id, h.claim_id_src, h.member_id_src
from dev.claim_header_optum h
left outer join dev.claim_detail_diag_optum diag on h.uth_claim_id=diag.uth_claim_id
where h.data_source = 'optd' and diag.diagnosis_code is null;


