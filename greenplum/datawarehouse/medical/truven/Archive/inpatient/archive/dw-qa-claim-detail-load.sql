/* 11/19/2019 - Joe Harrison
 * 	This was created to populate the dev.claim_detail_v1 table
 * 	- differs from original load script because claim IDs are now looked up from the data_warehouse.dim_uth_claim_id table
 * 	- temp tables were used because VMEM was being exceeded resulting in any query being killed
 * 	- the operations complete in less than 5 mins except for tmp_uth_claims and tmp_trvc_srvcs
 * 	- remember to run the drop statements at the end (otherwise they will hang around until the session ends)
 */
--in progress rewrite

create temp table tmp_uth_member_claim_ids as 
	select trunc(cast(m.member_id_src as numeric),0) member_id_src, m.data_source, m.uth_member_id,
	trunc(cast(c.claim_id_src as numeric),0) claim_id_src, c.data_year, c.uth_claim_id
	from data_warehouse.dim_uth_member_id m join data_warehouse.dim_uth_claim_id c 
	on m.data_source = c.data_source
	and m.member_id_src::numeric = c.member_id_src::numeric
	where m.data_source = 'trvc'
	
create temp table tmp_trvc_srvcs as
	select 'trvc' as dt_src, trunc(s.enrolid,0) enrolid, trunc(s."year",0) s_year, trunc(s.msclmid,0) msclmid,
	trunc(s.seqnum,0) seqnum, m.uth_member_id, get_my_from_date(s.admdate) mon_year, s.proc1, 
	substring(s.procmod, 1,1) as procmod1, s.stdplac, substring(s.procmod, 2,1) procmod2, s.revcode, 
	s.cob, s.coins, s.copay, s.deduct, s.pay, s.netpay, s.provid, s.qty, s.drg, s.proctyp, s.ntwkprov::boolean, 
	s.paidntwk::boolean, s.svcdate, s.tsvcdat, s.admdate, s.disdate
	from truven.ccaes s inner join tmp_uth_member_claim_ids m
	on trunc(s.enrolid,0) = m.member_id_src 
	where m.data_source='trvc'
	
create temp table tmp_claim_details as
select 'trvc' as dt_src, trunc(t.msclmid,0) claim_no, t.enrolid, t.s_year, t.msclmid, t.seqnum, t.uth_member_id, t.mon_year,
t.proc1, t.procmod1, t.procmod2, t.revcode, t.cob, t.coins, t.copay, t.deduct, t.pay, t.netpay, t.provid, t.qty, t.drg,t.stdplac,
t.proctyp, t.ntwkprov, t.paidntwk, t.svcdate, t.tsvcdat, t.admdate, t.disdate, u.uth_claim_id
--select min(t.mon_year), max(t.mon_year)
from truven.ccaes t inner join data_warehouse.dim_uth_claim_id u 
on t.msclmid::text = u.claim_id_src
and t.uth_member_id = u.uth_member_id
and t.s_year = u.data_year::int
where u.data_source = 'trvc'
and t.mon_year between 201501 and 201712

insert into dev.claim_detail_v1(data_source, uth_claim_id, claim_id_src, claim_sequence_number, uth_member_id, 
from_date_of_service, to_date_of_service, month_year_id, perf_provider_id, place_of_service, network_ind, 
network_paid_ind, admit_date, discharge_date, procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_code, 
allowed_amount, paid_amount, copay, deductible, coins, cob, units, drg_cd)
select dt_src, uth_claim_id , msclmid, 
	row_number() over (
		partition by uth_claim_id
		order by seqnum) rownum, 
	uth_member_id, svcdate, tsvcdat, mon_year, provid, stdplac, ntwkprov, paidntwk, admdate, disdate, proc1, proctyp, procmod1, procmod2, revcode,
	pay, netpay, copay, deduct, coins, cob, qty, drg
from tmp_claim_details

select * from tmp_uth_members
drop table tmp_uth_members
select * from tmp_uth_claims
drop table tmp_uth_claims
select * from tmp_trvc_srvcs
drop table tmp_trvc_srvcs
select * from tmp_claim_details
drop table tmp_claim_details

create temp table tmp_uth_members as
	select trunc(cast(member_id_src as numeric),0) member_id_src, data_source, uth_member_id
	from data_warehouse.dim_uth_member_id
	where data_source='trvc'
	
create temp table tmp_uth_claims as 
	select d.uth_claim_id, trunc(cast(claim_id_src as numeric),0) claim_id_src, d.data_source, d.data_year, 
	trunc(cast(d.member_id_src as numeric),0) member_id_src, m.uth_member_id
	from data_warehouse.dim_uth_claim_id d join tmp_uth_members m
	on d.data_source = m.data_source
	and d.member_id_src::numeric = m.member_id_src::numeric
	where d.data_source = 'trvc'

