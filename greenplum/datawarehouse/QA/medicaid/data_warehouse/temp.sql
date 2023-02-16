select * from data_warehouse.member_enrollment_monthly_1_prt_mdcd limit 5;

select * from data_warehouse.member_enrollment_yearly_1_prt_mdcd limit 5;

select * from data_warehouse.claim_detail_1_prt_mdcd

select * from data_warehouse.claim_header_1_prt_mdcd

select * from data_warehouse.claim_diag_1_prt_mdcd

select * from data_warehouse.claim_icd_proc_1_prt_mdcd

select * from data_warehouse.claim_header_1_prt_mdcd

select * from data_warehouse.claim_detail_1_prt_mdcd

select * from data_warehouse.pharmacy_claims_1_prt_mdcd

select rx_id from dev.xz_dwqa_temp2 where fill_date_mismatch = 1;
/*
7424725308077702739920210406
7424725308077702739920210504
7078035925926710000220210608
G460224050078161395720120514
6085411808077702731020201228
5122772645926710000220210616
G005011010008512880120120412
7201097410053601229720171030
 */

select * from data_warehouse.pharmacy_claims_1_prt_mdcd where rx_claim_id_src = '7424725308077702739920210406';


on a.rx_id = b.rx_claim_id_src and
	a.spc_total_charge_amount::float = b.total_charge_amount and
	a.spc_total_paid_amount::float = b.total_paid_amount and
	a.spc_refill_count = b.refill_count::text and
	a.spc_days_supply::int = b.days_supply and
	a.spc_script_id = b.script_id and
	a.spc_provider_npi = b.provider_npi and
	a.spc_pharmacy_id = b.pharmacy_id;

--total charge amount, total paid amount
select a.spc_total_charge_amount, b.total_charge_amount, a.spc_total_paid_amount, b.total_paid_amount
from dev.xz_dwqa_temp1 a left join data_warehouse.pharmacy_claims_1_prt_mdcd b
on a.rx_id = b.rx_claim_id_src and a.spc_refill_count = b.refill_count::text
where b.rx_claim_id_src = '7424725308077702739920210406';

--refill count, days supply
select a.spc_refill_count, b.refill_count, a.spc_days_supply, b.days_supply
from dev.xz_dwqa_temp1 a left join data_warehouse.pharmacy_claims_1_prt_mdcd b
on a.rx_id = b.rx_claim_id_src and a.spc_refill_count = b.refill_count::text
where b.rx_claim_id_src = '7424725308077702739920210406';

--script id, provider npi, pharmacy id
select a.spc_script_id, b.script_id, a.spc_provider_npi, b.provider_npi, a.spc_pharmacy_id, b.pharmacy_id
from dev.xz_dwqa_temp1 a left join data_warehouse.pharmacy_claims_1_prt_mdcd b
on a.rx_id = b.rx_claim_id_src and a.spc_refill_count = b.refill_count::text
where b.rx_claim_id_src = '7424725308077702739920210406';


select spc_member_id_src, spc_fy from dev.xz_dwqa_temp2 where fill_date_mismatch = 1;
/*Member IDs for mismatches
707803592	2021 --exists
720109741	2018
G46022405	2012
742472530	2021
509393548	2021
512277264	2021
707803592	2021
716383982	2021*/

select * from data_warehouse.member_enrollment_yearly where member_id_src = '707803592';

select * from dev.xz_dwqa_temp2 where from_date_of_service_mismatch = 1;
/*claims that do not match - HTW claims
100020031201807448252597  --not in claim_detail
100020030201814421515370
100020030201816540818858
100020030201820556895417
100020030201823265219957
100020030201819051517875 */

select * from data_warehouse.claim_detail_1_prt_mcrn where claim_id_src = '100020031201807448252597'

select spc_fy, spc_from_date_of_service, from_date_of_service
from dev.xz_dwqa_temp2
where from_date_of_service_mismatch = 1 and spc_fy != 'HTW';
--These are good

select count(*) from dev.xz_dwqa_temp2 where from_date_of_service_mismatch = 1;
--5014 : 5000 HTW, 14 others

select * from dev.xz_dwqa_temp2 where provider_type_mismatch = 1;

select * from dev.xz_dwqa_temp3 where icd_mismatch = 1;

drop table if exists dev.xz_dwqa_temp4;
select * into dev.xz_dwqa_temp4 from dev.xz_dwqa_temp2;

update dev.xz_dwqa_temp2
	set icd = NULL
		where icd = ''
	;


select count(distinct claim_id_src) from data_warehouse.claim_detail_1_prt_mdcd
where fiscal_year = 2021;
--55,202,602
--18,442,500 count distinct

select count(distinct claim_id_src) from data_warehouse.claim_header_1_prt_mdcd
where fiscal_year = 2021;
--111,404,359
--111,404,359 distinct

--spc has 111,404,359 for 2021

select count(distinct claim_id_src) from dw_staging.claim_detail
where fiscal_year = 2021;
--

select count(distinct claim_id_src) from data_warehouse.claim_header
where fiscal_year = 2021;
--111404359

select count(distinct claim_id_src) from data_warehouse.claim_detail_1_prt_mdcd;
--55,202,602
--18442500 count distinct

select count(distinct claim_id_src) from data_warehouse.claim_header_1_prt_mdcd;