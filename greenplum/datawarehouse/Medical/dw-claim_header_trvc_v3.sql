/* question:  for truven, can a case id be associated with multiple claim IDs? 
 *  keep in mind that case ids might be reused across years
 * 
 */

select year, caseid, msclmid, count(*)
from truven.ccaes s
group by year, caseid, msclmid
having count(*)>1
order by year, caseid, msclmid

/*
 * Purpose:  populate claim_header_v1 with truven data
 * 	1. need to add up totals from the ccaes table by claim
 *  2. this means I need to partition based on the claim id in order to get the sum
 *  3. joining truven.ccaes with dim_uth_claim_id to be able to use the uth_claim_id
 */

insert into dev.claim_header_v1(data_source,uth_claim_id, uth_member_id, admit_id, place_of_service, claim_type,
	total_allowed_amount, total_paid_amount)
--with cte1 as 
	--(  
		select distinct on(d.uth_claim_id) d.data_source, d.uth_claim_id, --d.claim_id_src, s.caseid, s.msclmid, 
		d.uth_member_id, s.admtyp, s.stdplac, 
			case s.facprof
				when 'F' then 'I'
				when 'P' then 'P'
				else ''
			end claim_type,
		sum(s.pay) over(partition by d.uth_claim_id order by d.uth_claim_id) allowed_amount,
		sum(s.netpay) over(partition by d.uth_claim_id order by d.uth_claim_id) paid_amount
		--select count(*)  417,858,041 rows
		from (truven.ccaes s join data_warehouse.dim_uth_claim_id d 
		on d.claim_id_src = s.msclmid::text
		and d.member_id_src = s.enrolid::text
		and d.data_year = s."year")
		where d.data_source = 'trvc'
		and s.year between 2015 and 2017
	--)
--select count(*) from cte1
--112,633,546 rows
--where s_year between 2015 and 2017
--33,622,914 rows

		--"pay" is allowed amount
--"netpay" is paid amount

select count(uth_claim_id) from data_warehouse.dim_uth_claim_id where data_source='trvc'

select * from reference_tables.ref_admit_source

select distinct facprof from truven.ccaes

select count(*) from dev.claim_header_v1 where data_source='trvc'
--delete from dev.claim_header_v1 where data_source='trvc'

select uth_claim_id, count(*) from dev.claim_header_v1 where data_source='trvc'
group by uth_claim_id
having count(*)>1

select count(distinct coalesce(msclmid, caseid))
from truven.ccaes

