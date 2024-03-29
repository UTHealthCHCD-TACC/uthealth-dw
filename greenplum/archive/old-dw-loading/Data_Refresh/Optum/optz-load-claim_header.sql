---optum zip insert 
insert into dw_staging.claim_header 
(
	data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, uth_admission_id, 
	total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, cost_factor_year,
	bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
)	
select distinct on(b.uth_claim_id)
	'optz', 
	extract(year from (min(a.fst_dt) over(partition by b.uth_claim_id))),
	b.uth_member_id, 
	b.uth_claim_id,
	c.claim_type_code,	
	min(a.fst_dt) over(partition by b.uth_claim_id) as from_date_of_service,
	max(a.lst_dt) over(partition by b.uth_claim_id) as to_date_of_service,
	d.uth_admission_id,
	sum((a.charge * c.cost_factor)) over(partition by b.uth_claim_id) as total_charge_amount,
	sum((a.std_cost * c.cost_factor)) over(partition by b.uth_claim_id) as total_allowed_amount, 
	null as total_paid_amount,
	dev.fiscal_year_func(min(a.fst_dt) over(partition by b.uth_claim_id)) as fiscal_year,
	a.std_cost_yr::int as cost_year,
	null as bill_provider, null as ref_provider, null as other_provider, 
	null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from optum_zip.medical a
    join dw_staging.optz_uth_claim_id b   
		on a.member_id_src = b.member_id_src 
		and a.clmid = b.claim_id_src
	join reference_tables.ref_optum_cost_factor c 
	    on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1)) 
	   and c.standard_price_year = a.std_cost_yr::int
    left outer join data_warehouse.dim_uth_admission_id d
       on d.member_id_src = a.patid::text 
      and d.admission_id_src = a.conf_id 
      and d."year" = a."year" 
     ;