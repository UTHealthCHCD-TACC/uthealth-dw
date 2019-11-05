insert into dev.claim_detail_v2(data_source,uth_claim_id,claim_id_src, claim_sequence_number,uth_member_id, 
month_year_id, procedure_cd, proc_mod_1, proc_mod_2, revenue_code, cob, coins, copay, deductible, 
allowed_amount, paid_amount,perf_provider_id, units, drg_cd, procedure_type, network_ind, network_paid_ind,
from_date_of_service, to_date_of_service, admit_date, discharge_date)
select 'trvc', 
	case s.msclmid
		when null then cast(trunc(s.caseid,0)::text||trunc(s.year,0)::text||trunc(s.enrolid,0)::text as numeric)
		else cast(trunc(s.msclmid,0)::text||trunc(s.year,0)::text||trunc(s.enrolid,0)::text as numeric)
	end claim_no,
	s.msclmid ,s.seqnum, m.uth_member_id, get_my_from_date(s.admdate), s.proc1, substring(s.procmod, 1,1), substring(s.procmod, 2,1),
	s.revcode, s.cob, s.coins, s.copay, s.deduct, s.pay, s.netpay, s.provid, s.qty, s.drg, 
	s.proctyp, s.ntwkprov::boolean, s.paidntwk::boolean, s.svcdate, s.tsvcdat, s.admdate, s.disdate
from truven.ccaes s inner join data_warehouse.dim_uth_member_id m
on s.enrolid = m.member_id_src::int8 and m.data_source='trvc'