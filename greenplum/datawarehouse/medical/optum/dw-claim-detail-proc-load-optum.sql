
analyze dw_qa.claim_header;
analyze dw_qa.claim_detail;

drop table if exists dw_qa.claim_icd_proc;

create table dw_qa.claim_icd_proc (
    data_source char(4),
    year int2,
	uth_claim_id int8,
	uth_member_id int8,
	claim_sequence_number int4,
	date date,
	proc_cd text,
	proc_position int2,
	icd_type text
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (uth_member_id);


delete from data_warehouse.claim_icd_proc where data_source = 'optd';


--Optum load: 
insert into data_warehouse.claim_icd_proc(data_source, year, uth_claim_id, uth_member_id, claim_sequence_number, date, proc_cd, proc_position, icd_type)
select distinct d.data_source, d.year, d.uth_claim_id, d.uth_member_id, d.claim_sequence_number, d.from_date_of_service, proc.proc, proc.proc_position, proc.icd_flag
from data_warehouse.claim_detail d
join optum_dod.procedure proc 
   on proc.clmid= d.claim_id_src 
  and proc.patid::text= d.member_id_src  
  and proc.fst_dt=d.from_date_of_service 
where d.data_source='optd'
  and proc.year >= 2019
;

select * from data_warehouse.claim_icd_proc cip where data_source = 'optd';

select *
from optum_dod.procedure
limit 2;

select data_source, count(*)
from dw_qa.claim_detail
group by 1;


UPDATE dw_qa.claim_detail d
SET claim_id_src = h.claim_id_src
FROM dw_qa.dim_uth_claim_id h
WHERE d.uth_member_id = h.uth_member_id and d.uth_claim_id = h.uth_claim_id;

select *
from dw_qa.claim_detail
where data_source = 'optd'
limit 10;

select *
from dw_qa.claim_detail cd 
where member_id_src =33010130528::text;
select trunc(90934.0)::text

-- Diagnostics

analyze data_warehouse.claim_icd_proc;

--Verify
select data_source, count(*), count(distinct d.uth_claim_id)
from data_warehouse.claim_icd_proc d
group by 1
order by 1;

select data_source, count(*)
from dw_qa.claim_icd_proc
group by 1;

--Missing diags???
select distinct h.uth_claim_id, h.claim_id_src, h.member_id_src
from dev.claim_header_optum h
left outer join dev.claim_detail_diag_optum diag on h.uth_claim_id=diag.uth_claim_id
where h.data_source = 'optd' and diag.diagnosis_code is null;


