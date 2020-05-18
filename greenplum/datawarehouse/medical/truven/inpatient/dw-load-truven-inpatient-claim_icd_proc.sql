--- Claim Proc
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial/medicare inpatient --------------------------------------
---------------------------------------------------------------------------------------------------		 
insert into data_warehouse.claim_icd_proc (data_source, YEAR, uth_claim_id, uth_member_id, date, proc_cd, proc_position, icd_type)  
select distinct h.data_source, h.YEAR, h.uth_claim_id, h.uth_member_id, h.from_date_of_service , i.proc15, 15, i.dxver 
from data_warehouse.claim_header  h
join truven.ccaes s on h.data_source ='trvc' 
and h.claim_id_src = s.msclmid::text 
and h.member_id_src = s.enrolid::text 
join truven.ccaei i on i.caseid = s.caseid and i.enrolid = s.enrolid 
where i.proc15 is not null;

--delete from data_warehouse.claim_icd_proc where data_source ='trvm';

delete from data_warehouse.claim_icd_proc where proc_cd is null;

--delete from dw_qa.claim_detail_diag where uth_claim_id in (select uth_claim_id from dw_qa.claim_detail where data_source='trvm')

alter table dw_qa.claim_icd_proc alter column claim_sequence_number type int4;


-- SCRATCH

vacuum full data_warehouse.claim_icd_proc;
analyze data_warehouse.claim_icd_proc;

select *
from dw_qa.claim_detail
where data_source='trvm'
limit 10;

select data_source, year, count(*)
from data_warehouse.claim_icd_proc
group by 1, 2
order by 1, 2;
