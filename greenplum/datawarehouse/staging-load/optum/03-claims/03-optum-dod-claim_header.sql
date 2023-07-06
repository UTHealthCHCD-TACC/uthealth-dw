/* ******************************************************************************************************
 *  load claim header for optum zip and optum dod 
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 9/09/2021 || add comment block. migrate to dw_staging load 
 * ****************************************************************************************************** 
 *  gmunoz  || 10/20/2021 || added fiscal year logic with function dev.fiscal_year_func
 * ****************************************************************************************************** 
 *  jw002   || 11/08/2021 || add provider variables
 * ******************************************************************************************************
 * iperez	|| 07/06/2023 || added source member id, claim id, and table id. added bill type code
 * *************************************************************************************
 * */

drop table if exists dw_staging.optd_claim_header;

create table dw_staging.optd_claim_header
(like data_warehouse.claim_header including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

insert into dw_staging.optd_claim_header 
(
	data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service,
	total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, cost_factor_year,
	bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider,
	claim_id_src, member_id_src, table_id_src, load_date
)	
select distinct on(b.uth_claim_id)
		'optd', 
		extract(year from (min(a.fst_dt) over(partition by b.uth_claim_id))),
		b.uth_member_id, 
		b.uth_claim_id,
		c.claim_type_code,	
		min(a.fst_dt) over(partition by b.uth_claim_id) as from_date_of_service,
		max(a.lst_dt) over(partition by b.uth_claim_id) as to_date_of_service,
		sum((a.charge * c.cost_factor)) over(partition by b.uth_claim_id) as total_charge_amount,
		sum((a.std_cost * c.cost_factor)) over(partition by b.uth_claim_id) as total_allowed_amount, 
		null as total_paid_amount,
		dev.fiscal_year_func(min(a.fst_dt) over(partition by b.uth_claim_id)) as fiscal_year,
		a.std_cost_yr::int as cost_year,
		null as bill_provider, null as ref_provider, null as other_provider, 
		null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider,
		a.clmid, a.patid::text, 'medical',
		current_date
from optum_dod.medical a
join dw_staging.optd_uth_claim_id b 
  on a.member_id_src = b.member_id_src 
 and a.clmid = b.claim_id_src
 and a."year" = b.data_year
join reference_tables.ref_optum_cost_factor c
  on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1)) 
 and c.standard_price_year = a.std_cost_yr::int
;

vacuum analyze dw_staging.optd_claim_header;