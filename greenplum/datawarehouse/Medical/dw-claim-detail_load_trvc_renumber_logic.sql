/* inserting trvc data for inpatient services (truven "s" table) into dev.claim_detail_v2
 * this procedure "calculates" a uth_claim_id as follows:
 * 		if msclmid is available, then uth_claim_id = msclmid||year||enrolid, else caseid||year||enrolid
 * 		* does not address when enrolid is null (529,823 records in truven.ccaes had enrolid=[null])
 * this procedure also renumbers the claim sequence numbers for each claim
 * 		the original seqnum field is NOT copied into the target but is instead replaced with the new sequence number
 * 		the new sequence number simply runs from 1 and increments by 1 for each claim number
 * 		a common table expression (cte) is used to allow for the renumbering in one step
 * this procedure does not address the following fields (assumption is that these are not relevant for data_source='trvc':
 * 		bill_provider_id, ref_provider_id, charge_amount, bill_type_inst, bill_type_class, bill_type_freq, drg_type
 * this procedure only loads data for a specific period of time (years)
 */
insert into dev.claim_detail_v2(data_source, uth_claim_id, claim_id_src, claim_sequence_number, uth_member_id, 
from_date_of_service, to_date_of_service, month_year_id, perf_provider_id, network_ind, network_paid_ind, admit_date, discharge_date,
procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_code, allowed_amount, paid_amount, copay, deductible, coins, cob,    
units, drg_cd)

with cte1
	as 
	(
		select 'trvc' as dt_src, 
			case s.msclmid
				when null then cast(trunc(s.caseid,0)::text||trunc(s.year,0)::text||trunc(s.enrolid,0)::text as numeric)
				else cast(trunc(s.msclmid,0)::text||trunc(s.year,0)::text||trunc(s.enrolid,0)::text as numeric)
			end claim_no,
			s.msclmid ,s.seqnum, m.uth_member_id, get_my_from_date(s.admdate) as mon_year, s.proc1, substring(s.procmod, 1,1) as procmod1, 
			substring(s.procmod, 2,1) procmod2, s.revcode, s.cob, s.coins, s.copay, s.deduct, s.pay, s.netpay, s.provid, s.qty, s.drg, 
			s.proctyp, s.ntwkprov::boolean, s.paidntwk::boolean, s.svcdate, s.tsvcdat, s.admdate, s.disdate
		from truven.ccaes s inner join data_warehouse.dim_uth_member_id m
		on s.enrolid = m.member_id_src::int8 and m.data_source='trvc'
	)
select dt_src, claim_no, msclmid, 
	row_number() over (
		partition by claim_no
		order by seqnum) rownum, 
	uth_member_id, svcdate, tsvcdat, mon_year, provid, ntwkprov, paidntwk, admdate, disdate, proc1, proctyp, procmod1, procmod2, revcode,
	pay, netpay, copay, deduct, coins, cob, qty, drg
from cte1
where mon_year between 201501 and 201712