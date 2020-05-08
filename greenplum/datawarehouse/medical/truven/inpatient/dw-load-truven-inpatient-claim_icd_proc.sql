--- Claim Detail Diag
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 
insert into dw_qa.claim_proc (data_source, YEAR, uth_claim_id, uth_member_id, claim_sequence_number, date, proc_cd, proc_position, icd_type)  

select distinct h.data_source, h.YEAR, h.uth_claim_id, h.uth_member_id, null, h.from_date_of_service , i.proc2, 2, i.dxver 
from dw_qa.claim_header  h
join truven.mdcrs s on h.data_source ='trvm' 
and h.claim_id_src = s.msclmid::text 
and h.member_id_src = s.enrolid::text 
join truven.mdcri i on i.caseid = s.caseid and i.enrolid = s.enrolid 
where i.proc2 is not null
LIMIT 1;

--delete from dw_qa.claim_detail_diag where uth_claim_id in (select uth_claim_id from dw_qa.claim_detail where data_source='trvm')

alter table dw_qa.claim_icd_proc alter column claim_sequence_number type int4;


-- SCRATCH

vacuum full dw_qa.claim_detail_diag;
analyze dw_qa.claim_detail_diag;

select *
from dw_qa.claim_detail
where data_source='trvm'
limit 10;

select data_source, count(*)
from dw_qa.claim_detail
group by 1;

select data_source, diag_position , count(*), count(distinct d.uth_claim_id)
from dw_qa.claim_detail_diag d
join dw_qa.claim_detail l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id
group by 1,2 
order by 1, 2;

select distinct pproc
from dev2016.truven_ccaeo;

select distinct pproc 
from dev2016.truven_ccaes;

select a.*, b.*
from dev2016.truven_ccaeo a
join dev2016.truven_ccaei b on a.caseid=b.caseid
where a.pproc is not null
limit 1;

select seqnum, pproc
from dev2016.truven_ccaes
where caseid=1115609.0
order by 1

select *
from dev2016.truven_ccaei
where caseid=1115609.0
