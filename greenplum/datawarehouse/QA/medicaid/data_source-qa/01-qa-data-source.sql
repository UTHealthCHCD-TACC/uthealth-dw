/***********************************************
 * This script QAs the data source and makes sure it's correctly assigned
 * after data_warehouse tables have been built
 * 
 * Claims tables are all populated from the same source
 * so fairly safe to assume that if claim_header is correct
 * then claim_detail, claim_diag, and claim_icd_proc are also correct
 * 
 * (but spot-checking is always recommended)
 * 
 * Rx tables are checked separately
 */

--first get a random selection of claims from claim header
drop table if exists dev.xz_dwqa_claims_to_check;

create table dev.xz_dwqa_claims_to_check as
select data_source, uth_claim_id, uth_member_id, member_id_src, claim_id_src,
	from_date_of_service,
	cast(to_char(from_date_of_service, 'YYYYMM') as integer) as month_year_id
from data_warehouse.claim_header_1_prt_mdcd
order by random()
limit 1000
distributed by (uth_claim_id);

--grab mhtw claims
insert into dev.xz_dwqa_claims_to_check
select data_source, uth_claim_id, uth_member_id, member_id_src, claim_id_src,
	from_date_of_service,
	cast(to_char(from_date_of_service, 'YYYYMM') as integer) as month_year_id
from data_warehouse.claim_header_1_prt_mhtw
order by random()
limit 1000;

--grab chip perinatal claims
insert into dev.xz_dwqa_claims_to_check
select data_source, uth_claim_id, uth_member_id, member_id_src, claim_id_src,
	from_date_of_service,
	cast(to_char(from_date_of_service, 'YYYYMM') as integer) as month_year_id
from data_warehouse.claim_header_1_prt_mcpp
order by random()
limit 1000;

/******************************
 * Join to OG Medicaid enrollment tables and check accuracy of data source assignment
 */

drop table if exists dev.xz_dwqa_data_source_check;

create table dev.xz_dwqa_data_source_check as
select a.*, b.me_code, c.chip_per_fl,
	case when d.client_nbr is not null then 'htw.enrl'::text else null end as htw_enrl,
	case when (a.data_source = 'mcpp' and c.chip_per_fl = 'CP' and c.chip_per_fl is not null) then 1
	when (a.data_source = 'mhtw' and ((b.me_code = 'W' and b.me_code is not null)
		or d.client_nbr is not null)) then 1
	when (a.data_source = 'mdcd' and 
		(b.me_code != 'W' or b.me_code is null) and 
		(c.chip_per_fl != 'CP' or c.chip_per_fl is null) and
		d.client_nbr is null) then 1
	else 0 end as "match"
from dev.xz_dwqa_claims_to_check a left join medicaid.enrl b
	on a.member_id_src = b.client_nbr and a.month_year_id = b.elig_date::int
	left join medicaid.chip_enrl c
	on a.member_id_src = c.client_nbr and a.month_year_id = c.elig_month::int
	left join medicaid.htw_enrl d
	on a.member_id_src = d.client_nbr and a.month_year_id = d.elig_date::int;

select * from dev.xz_dwqa_data_source_check where match = 0;

/**********************
 * Grab random selection from rx tables
 */

--first get a random selection of claims from claim header
drop table if exists dev.xz_dwqa_rx_claims_to_check;

create table dev.xz_dwqa_rx_claims_to_check as
select data_source, uth_rx_claim_id, uth_member_id, member_id_src, rx_claim_id_src,
	fill_date,
	cast(to_char(fill_date, 'YYYYMM') as integer) as month_year_id
from data_warehouse.pharmacy_claims_1_prt_mdcd
order by random()
limit 1000
distributed by (uth_rx_claim_id);

--grab mhtw claims
insert into dev.xz_dwqa_rx_claims_to_check
select data_source, uth_rx_claim_id, uth_member_id, member_id_src, rx_claim_id_src,
	fill_date,
	cast(to_char(fill_date, 'YYYYMM') as integer) as month_year_id
from data_warehouse.pharmacy_claims_1_prt_mhtw
order by random()
limit 1000;

--grab chip perinatal claims
insert into dev.xz_dwqa_rx_claims_to_check
select data_source, uth_rx_claim_id, uth_member_id, member_id_src, rx_claim_id_src,
	fill_date,
	cast(to_char(fill_date, 'YYYYMM') as integer) as month_year_id
from data_warehouse.pharmacy_claims_1_prt_mcpp
order by random()
limit 1000;

/******************************
 * Join to OG Medicaid enrollment tables and check accuracy of data source assignment
 */

drop table if exists dev.xz_dwqa_rx_data_source_check;

create table dev.xz_dwqa_rx_data_source_check as
select a.*, b.me_code, c.chip_per_fl,
	case when d.client_nbr is not null then 'htw.enrl'::text else null end as htw_enrl,
	case when (a.data_source = 'mcpp' and c.chip_per_fl = 'CP' and c.chip_per_fl is not null) then 1
	when (a.data_source = 'mhtw' and ((b.me_code = 'W' and b.me_code is not null)
		or d.client_nbr is not null)) then 1
	when (a.data_source = 'mdcd' and 
		(b.me_code != 'W' or b.me_code is null) and 
		(c.chip_per_fl != 'CP' or c.chip_per_fl is null) and
		d.client_nbr is null) then 1
	else 0 end as "match"
from dev.xz_dwqa_rx_claims_to_check a left join medicaid.enrl b
	on a.member_id_src = b.client_nbr and a.month_year_id = b.elig_date::int
	left join medicaid.chip_enrl c
	on a.member_id_src = c.client_nbr and a.month_year_id = c.elig_month::int
	left join medicaid.htw_enrl d
	on a.member_id_src = d.client_nbr and a.month_year_id = d.elig_date::int;

select * from dev.xz_dwqa_rx_data_source_check where match = 0;







/********************************
 * Did it all check out?? Then drop temp tables
 * 
drop table if exists dev.xz_dwqa_claims_to_check;
drop table if exists dev.xz_dwqa_data_source_check;
 */













