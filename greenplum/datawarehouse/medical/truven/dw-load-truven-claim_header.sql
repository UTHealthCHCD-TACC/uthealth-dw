

/* ******************************************************************************************************
 *  load claim header for optum zip and optum dod
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jsw001  || 9/20/2021 || add to_date_of_service and cost_factor_year. change to dw_staging
 * ******************************************************************************************************
 *  gmunoz  || 10/20/2021 || added fiscal year logic with function dev.fiscal_year_func
 * ******************************************************************************************************
 *  jw002   || 11/08/2021 || add provider variables, remove old _src variables
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc001  || 11/12/2021 || consolidate to single script, add comments and raise notices
 * ******************************************************************************************************
 *    jwozny  || 09/21/2022 || fixed payment variables
 * ****************************************************************************************************** 
 *  iperez  || 09/28/2022 || added claim id source and member id source to columns
 * ******************************************************************************************************
 * */


do $$ 

begin 
	

--create copy of dim claim id with matching distribution key to join efficiently on raw tables 
drop table if exists dev.truven_dim_uth_claim_id;

create table dev.truven_dim_uth_claim_id
with (	appendonly = true, orientation = column,compresstype = zlib) as
	select *
	from data_warehouse.dim_uth_claim_id
	where data_source = 'truv' 
distributed by (member_id_src);

analyze dev.truven_dim_uth_claim_id;

raise notice 'uth claim id table built';
-------------------------------- truven commercial outpatient--------------------------------------
analyze truven.ccaeo;

----ccaeo-----------------------------------------------------------------------------------------------
insert into dw_staging.claim_header
(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_type, 
	from_date_of_service,
	to_date_of_service,
	uth_admission_id, 
	total_charge_amount, 
	total_allowed_amount, 
	total_paid_amount, 
	fiscal_year, 
	cost_factor_year,
	claim_id_src,
	member_id_src
)
select distinct on (uth_claim_id) 
    'truv',
	  extract(year from a.svcdate),
      b.uth_member_id,
      b.uth_claim_id,
      a.facprof,
	  a.svcdate,
	  a.tsvcdat,
	  null as uth_admission_id,
	  sum(a.netpay) over (partition by b.uth_claim_id) as total_charge_amount,
	  sum(a.pay) over (partition by b.uth_claim_id) as allowed_amt,
	  null as paid_amt,
	  dev.fiscal_year_func(a.svcdate) as fiscal_year,
	  null as cost_factor_year,
	  a.msclmid::text,
	  a.enrolid::text
 -- ,	null as bill_provider, null as ref_provider, null as other_provider, null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from truven.ccaeo a
join dev.truven_dim_uth_claim_id b   --  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.data_source = 'truv'
	and b.claim_id_src = a.msclmid::text
;

raise notice 'ccaeo header loaded';

-------------------------------- truven medicare outpatient ---------------------------------------
analyze truven.mdcro;

------mdcro
insert into dw_staging.claim_header
(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_type, 
	from_date_of_service,
	to_date_of_service,
	uth_admission_id, 
	total_charge_amount, 
	total_allowed_amount, 
	total_paid_amount, 
	fiscal_year, 
	cost_factor_year,
	claim_id_src,
	member_id_src
)
select distinct on (uth_claim_id) 
    'truv',
	  extract(year from a.svcdate),
      b.uth_member_id,
      b.uth_claim_id,
      a.facprof,
	  a.svcdate,
	  a.tsvcdat,
	  null as uth_admission_id,
	  sum(a.netpay) over (partition by b.uth_claim_id) as total_charge_amount,
	  sum(a.pay) over (partition by b.uth_claim_id) as allowed_amt,
	  null as paid_amt,
	  dev.fiscal_year_func(a.svcdate) as fiscal_year,
	  null as cost_factor_year,
	  a.msclmid::text,
	  a.enrolid::text
from truven.mdcro a
join dev.truven_dim_uth_claim_id b --  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.claim_id_src = a.msclmid::text
	and b.data_source = 'truv'
;

raise notice 'mdcro header loaded';

-------------------------------- truven commercial inpatient--------------------------------------
analyze truven.ccaes;

-----ccaes--------------------------------------------------------------------------------------------
insert into dw_staging.claim_header
(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_type, 
	from_date_of_service,
	to_date_of_service,
	uth_admission_id, 
	total_charge_amount, 
	total_allowed_amount, 
	total_paid_amount, 
	fiscal_year, 
	cost_factor_year,
	claim_id_src,
	member_id_src
)
select distinct on (uth_claim_id) 
    'truv',
	  extract(year from a.svcdate),
    b.uth_member_id,
    b.uth_claim_id,
    a.facprof,
	  a.svcdate,
	  a.tsvcdat,
	  null as uth_admission_id,
	  sum(a.netpay) over (partition by b.uth_claim_id) as total_charge_amount,
	  sum(a.pay) over (partition by b.uth_claim_id) as allowed_amt,
	  null as paid_amt,
	  dev.fiscal_year_func(a.svcdate) as fiscal_year,
	  null as cost_factor_year,
	  a.msclmid::text,
	  a.enrolid::text
from truven.ccaes a
join dev.truven_dim_uth_claim_id b--  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.claim_id_src = a.msclmid::text
	and b.data_source = 'truv';

raise notice 'ccaes header loaded';



-------------------------------- truven medicare advantage inpatient------------------------------
analyze truven.mdcrs;

----mdcrs-----------------------------------------------------------------------------------------------
insert into dw_staging.claim_header
(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_type, 
	from_date_of_service,
	to_date_of_service,
	uth_admission_id, 
	total_charge_amount, 
	total_allowed_amount, 
	total_paid_amount, 
	fiscal_year, 
	cost_factor_year,
	claim_id_src,
	member_id_src
 -- bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider
)
select distinct on (uth_claim_id) 
    'truv',
	  extract(year from a.svcdate),
    b.uth_member_id,
    b.uth_claim_id,
    a.facprof,
	  a.svcdate,
	  a.tsvcdat,
	  null as uth_admission_id,
	  sum(a.netpay) over (partition by b.uth_claim_id) as total_charge_amount,
	  sum(a.pay) over (partition by b.uth_claim_id) as allowed_amt,
	  null as paid_amt,
	  dev.fiscal_year_func(a.svcdate) as fiscal_year,
	  null as cost_factor_year,
	  a.msclmid::text,
	  a.enrolid::text
 -- ,	null as bill_provider, null as ref_provider, null as other_provider, null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from truven.mdcrs a
join dev.truven_dim_uth_claim_id b	--  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.claim_id_src = a.msclmid::text
	and b.data_source = 'truv';

raise notice 'mdcrs header loaded';

end $$;

