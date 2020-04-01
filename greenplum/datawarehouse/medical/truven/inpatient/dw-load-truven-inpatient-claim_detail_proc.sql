--- Claim Detail Diag
---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------		
-- 
insert into dw_qa.claim_detail_proc (uth_claim_id, uth_member_id, claim_sequence_number, date, proc_cd, proc_position, icd_type)  								        						              
select distinct d.uth_claim_id, d.uth_member_id, d.claim_sequence_number , d.from_date_of_service , a.dx1, 1, a.dxver 
from dw_qa.claim_detail  d
join truven.mdcri a on d.data_source ='trvm' 
and d.claim_id_src = a.msclmid::text 
and d.member_id_src = a.enrolid::text 
and d.from_date_of_service = a.svcdate 
and d.claim_sequence_number_src = a.seqnum::text;

--delete from dw_qa.claim_detail_diag where uth_claim_id in (select uth_claim_id from dw_qa.claim_detail where data_source='trvm')

alter table dw_qa.claim_detail_diag alter column claim_sequence_number type int4;


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
