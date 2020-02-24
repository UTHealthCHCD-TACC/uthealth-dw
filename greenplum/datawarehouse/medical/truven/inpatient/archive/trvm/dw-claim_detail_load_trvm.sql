/* 12/18/2019 - Joe Harrison
 * 	This was created to populate the dw_qa.claim_detail table with Truven Medicare claim details
 * 	- claim IDs are looked up from the data_warehouse.dim_uth_claim_id table
 * 	- temp tables were used to be consistent with the trvc loader
 * 	- the operations complete in ...
 * 	- remember to run the drop statements at the end (otherwise they will hang around until the session ends)
 */
--1.156s to run
create temp table tmp_uth_members_trvm as
	select trunc(cast(member_id_src as numeric),0) member_id_src, data_source, uth_member_id
	from data_warehouse.dim_uth_member_id
	where data_source='trvm'

--1m29s 438,616,809 rows
create temp table tmp_uth_claims_trvm as 
	select d.uth_claim_id, trunc(cast(claim_id_src as numeric),0) claim_id_src, d.data_source, d.data_year, 
	trunc(cast(d.member_id_src as numeric),0) member_id_src, m.uth_member_id
	from data_warehouse.dim_uth_claim_id d join tmp_uth_members_trvm m
	on d.data_source = m.data_source
	and d.member_id_src::numeric = m.member_id_src::numeric
	where d.data_source = 'trvm'

--36.284s   146,864,403 rows
create temp table tmp_trvm_srvcs as
	select 'trvm' as dt_src, trunc(coalesce(s.msclmid, s.caseid),0) claim_no, trunc(s.enrolid,0) enrolid, 
	trunc(s."year",0) s_year, trunc(s.msclmid,0) msclmid ,trunc(s.seqnum,0) seqnum, 
	m.uth_member_id, cast(to_char(s.admdate,'YYYYMM') as int) mon_year, s.proc1, substring(s.procmod, 1,1) as procmod1, s.stdplac,
	substring(s.procmod, 2,1) procmod2, s.revcode, s.cob, s.coins, s.copay, s.deduct, s.pay, s.netpay, s.provid, s.qty, s.drg, 
	s.proctyp, s.ntwkprov::boolean, s.paidntwk::boolean, s.svcdate, s.tsvcdat, s.admdate, s.disdate
	from truven.mdcrs s inner join tmp_uth_members_trvm m
	on trunc(s.enrolid,0) = m.member_id_src 
	where m.data_source='trvm'


--17.72s  429,399 rows   
create temp table tmp_claim_details_trvm as
select t.dt_src, t.claim_no, t.enrolid, t.s_year, t.msclmid, t.seqnum, t.uth_member_id, t.mon_year,
t.proc1, t.procmod1, t.procmod2, t.revcode, t.cob, t.coins, t.copay, t.deduct, t.pay, t.netpay, t.provid, t.qty, t.drg,t.stdplac,
t.proctyp, t.ntwkprov, t.paidntwk, t.svcdate, t.tsvcdat, t.admdate, t.disdate, u.uth_claim_id
--select min(t.mon_year), max(t.mon_year)
from tmp_trvm_srvcs t inner join tmp_uth_claims_trvm u 
on t.dt_src = u.data_source
and t.claim_no = u.claim_id_src
and t.uth_member_id = u.uth_member_id
and t.s_year = u.data_year::int
where t.mon_year between 201501 and 201712

--893ms  429,399 rows  (corresponds to 180,130 claims)
insert into dw_qa.claim_detail(data_source, uth_claim_id, claim_id_src, claim_sequence_number, uth_member_id, 
from_date_of_service, to_date_of_service, month_year_id, perf_provider_id, place_of_service, network_ind, 
network_paid_ind, admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_cd, 
allowed_amount, paid_amount, copay, deductible, coins, cob, units, drg_cd, member_id_src)
select dt_src, uth_claim_id , msclmid, 
	row_number() over (
		partition by uth_claim_id
		order by seqnum) rownum, 
	uth_member_id, svcdate, tsvcdat, mon_year, provid, stdplac, ntwkprov, paidntwk, admdate, disdate, proc1, proctyp, procmod1, procmod2, revcode,
	pay, netpay, copay, deduct, coins, cob, qty, drg, enrolid
from tmp_claim_details_trvm

select * from tmp_uth_members_trvm
drop table tmp_uth_members_trvm
select * from tmp_uth_claims_trvm
drop table tmp_uth_claims_trvm
select * from tmp_trvm_srvcs
drop table tmp_trvm_srvcs
select * from tmp_claim_details_trvm
drop table tmp_claim_details_trvm




