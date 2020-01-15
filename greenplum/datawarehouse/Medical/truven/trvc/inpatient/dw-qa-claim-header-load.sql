/* NOTE: 
 *  Both case ids and claim IDs might be reused across years
 */

select count(*) from dw_qa.claim_header where data_source='trvc' and table_id_src='truven.ccaes'
--33,622,914 rows as of Jan 15, 2020

/*
 * Purpose:  populate claim_header_v1 with truven data for period 2015-2017
 * 	1. need to add up totals from the ccaes table by claim
 *  2. partition based on the claim id in order to get the sum
 *  3. joining truven.ccaes with dim_uth_claim_id to retrieve uth_claim_id
 */

/* check for negative payment values to see if there are claim reversal 
 * the presence of negative values implies that sums will be correct without any adjustment being necessary
 * */
select count(*) from truven.ccaes s where s.pay<0 or s.netpay<0

delete from dw_qa.claim_header h where h.data_source='trvc' and h.table_id_src='truven.ccaes' and extract(year from h.from_date_of_service) between 2015 and 2017

insert into dw_qa.claim_header(data_source,uth_claim_id, claim_id_src, uth_member_id, member_id_src, place_of_service, claim_type,
	total_allowed_amount, total_paid_amount, from_date_of_service, table_id_src, admission_id_src)
		select distinct on(d.uth_claim_id) d.data_source, d.uth_claim_id, s.msclmid,
		d.uth_member_id, s.enrolid, trunc(s.stdplac,0), 
			case s.facprof
				when 'F' then 'I'
				when 'P' then 'P'
				else ''
			end claim_type,
		--"pay" is allowed amount
		sum(s.pay) over(partition by d.uth_claim_id) allowed_amount,
		--"netpay" is paid amount
		sum(s.netpay) over(partition by d.uth_claim_id) paid_amount,
		s.svcdate, 'truven.ccaes', s.caseid
		from (truven.ccaes s join data_warehouse.dim_uth_claim_id d 
		on d.claim_id_src = s.msclmid::text
		and d.member_id_src = s.enrolid::text
		and d.data_year = s."year")
		where d.data_source = 'trvc'
		and s.year between 2015 and 2017

--33,622,914 rows inserted 1/15/2020

select count(*) from dw_qa.claim_header where table_id_src='truven.ccaes'


