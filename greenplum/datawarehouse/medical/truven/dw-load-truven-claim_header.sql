

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
 * */


--create copy with matching distribution key
create table dev.truven_dim_uth_claim_id
with (	appendonly = true, orientation = column,compresstype = zlib) as
	select *
	from data_warehouse.dim_uth_claim_id
	where data_source = 'truv' 
distributed by (member_id_src);

vacuum analyze dev.truven_dim_uth_claim_id;


---------------------------------------------------------------------------------------------------
-------------------------------- truven commercial outpatient--------------------------------------
---------------------------------------------------------------------------------------------------
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
	cost_factor_year
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
	  null as total_charge_amount,
	  sum(a.pay) over (partition by b.uth_claim_id) as allowed_amt,
	  sum(a.netpay) over (partition by b.uth_claim_id) as paid_amt,
	  dev.fiscal_year_func(a.svcdate) as fiscal_year,
	  null as cost_factor_year
from truven.ccaeo a
join dev.truven_dim_uth_claim_id b   --  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.data_source = 'truv'
	and b.claim_id_src = a.msclmid::text
;


select count(distinct msclmid), year from truven.ccaeo group by year order by year;


select data_source, year, count(*) from dw_staging.claim_header ch group by 1,2 order by 1,2;

---------------------------------------------------------------------------------------------------
-------------------------------- truven medicare outpatient ---------------------------------------
---------------------------------------------------------------------------------------------------
--clm hdr truv mdcr outpatient
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
	cost_factor_year
)
select distinct on (uth_claim_id) 'truv',
	extract(year from a.svcdate),
	b.uth_claim_id,
	b.uth_member_id,
	a.svcdate,
	a.facprof,
	null as uth_admission_id,
	null as admission_id_src,
	null as total_charge_amount,
	sum(a.pay) over (partition by b.uth_claim_id),
	sum(a.netpay) over (partition by b.uth_claim_id),
	a.msclmid,
	a.enrolid,
	'mdcro',
	dev.fiscal_year_func(a.svcdate) as fiscal_year,
	null as cost_factor_year,
	a.tsvcdat
from truven.mdcro a
join dev.truven_dim_uth_claim_id b
	--  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.claim_id_src = a.msclmid::text
	and b.data_source = 'truv'
	and a.year between 2015
		and 2019;

select facprof
from truven.mdcro;

-------------------------------- truven commercial inpatient--------------------------------------
---------------------------------------------------------------------------------------------------
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
	cost_factor_year
)
select distinct on (uth_claim_id) 'truv',
	extract(year from a.svcdate),
	b.uth_claim_id,
	b.uth_member_id,
	a.svcdate,
	a.facprof,
	null,
	trunc(a.caseid, 0)::text,
	null,
	sum(a.pay) over (partition by b.uth_claim_id),
	sum(a.netpay) over (partition by b.uth_claim_id),
	a.msclmid,
	a.enrolid,
	'ccaes',
	dev.fiscal_year_func(a.svcdate) as fiscal_year,
	null as cost_factor_year,
	a.tsvcdat
from truven.ccaes a
join dev.truven_dim_uth_claim_id b
	--  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.claim_id_src = a.msclmid::text
	and b.data_source = 'truv';

-------------------------------- truven medicare advantage inpatient------------------------------
---------------------------------------------------------------------------------------------------
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
	cost_factor_year
)
select distinct on (uth_claim_id) 'truv',
	extract(year from a.svcdate),
	b.uth_claim_id,
	b.uth_member_id,
	a.svcdate,
	a.facprof,
	null,
	trunc(a.caseid, 0)::text,
	null,
	sum(a.pay) over (partition by b.uth_claim_id),
	sum(a.netpay) over (partition by b.uth_claim_id),
	a.msclmid,
	a.enrolid,
	'mdcrs',
	dev.fiscal_year_func(a.svcdate) as fiscal_year,
	null as cost_factor_year,
	a.tsvcdat
from truven.mdcrs a
join dev.truven_dim_uth_claim_id b
	--  join data_warehouse.dim_uth_claim_id b
	on b.member_id_src = a.enrolid::text
	and b.claim_id_src = a.msclmid::text
	and b.data_source = 'truv';

vacuum analyze dw_staging.claim_header;

select count(*),
	year --count(distinct uth_claim_id ), table_id_src, year
from dw_staging.claim_header
where data_source = 'truv'
group by year --table_id_src , year
order by year -- table_id_src , year
	;

---claim load clean
drop table dev.truven_dim_uth_claim_id;

----this will identify duplicate records from claim header if needed
drop table dev.wc_temp_truv_hdr;

select uth_claim_id,
	year
into dev.wc_temp_truv_hdr
from (
	select count(*) as rc,
		uth_claim_id,
		year
	from dw_staging.claim_header
	where data_source = 'truv'
	group by uth_claim_id,
		year
	) a
where rc > 1;

drop table quarantine.duplicate_truv_claims

select * -- uth_claim_id, table_id_src , uth_member_id
	--into quarantine.duplicate_truv_claims
from dw_staging.claim_header
where uth_claim_id in (
		select uth_claim_id
		from dev.wc_temp_truv_hdr
		)
order by uth_claim_id

select *
from data_warehouse.claim_detail cd
where uth_claim_id = 15768744356

select count(*),
	table_id_src
from quarantine.duplicate_truv_claims
group by table_id_src

delete
from dw_staging.claim_header
where uth_claim_id in (
		select uth_claim_id
		from quarantine.duplicate_truv_claims
		);

delete
from data_warehouse.claim_detail
where uth_claim_id in (
		select uth_claim_id
		from quarantine.duplicate_truv_claims
		);

delete
from data_warehouse.claim_diag
where uth_claim_id in (
		select uth_claim_id
		from quarantine.duplicate_truv_claims
		);

delete
from data_warehouse.claim_icd_proc
where uth_claim_id in (
		select uth_claim_id
		from quarantine.duplicate_truv_claims
		);
