
/* ******************************************************************************************************
 *  load pharmacy claims for optum zip and optum dod - !! Run dim_uth_rx_claim_id update prior to running this script. !!
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw01    || 9/01/2021 || add variables  retail_or_mail_indicator, dispensed_as_written, dose, strength,
 *                       || formulary_ind, special_drug_ind.
 * ****************************************************************************************************** 
 *  wcc001  || 9/20/2021 || add comment block. migrate to dw_staging load 
 * ****************************************************************************************************** 
 * */

--------------- BEGIN SCRIPT -------
do $$

begin

---******************************************************************************************************************
------ ***** Optum DoD - optd *****
---******************************************************************************************************************

---create copy uth claims with optd only and distribute on member id src
drop table if exists dw_staging.optd_uth_rx_claim;

create table dw_staging.optd_uth_rx_claim
with(appendonly=true,orientation=column) as 
select * from data_warehouse.dim_uth_rx_claim_id where data_source = 'optd'
distributed by (member_id_src);


analyze dw_staging.optd_uth_rx_claim;

raise notice 'start optum dod';


--optd  9m
insert into dw_staging.pharmacy_claims (
		data_source,
		year,
		uth_rx_claim_id,
		uth_member_id,
		fill_date,
		ndc,
		days_supply,
		script_id,
		refill_count,
		month_year_id,
		generic_ind,
		generic_name,
		brand_name,
		quantity,
		provider_npi,
		pharmacy_id,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob,
		fiscal_year,
		cost_factor_year,
		therapeutic_class,
		ahfs_class,
		first_fill,
		rx_claim_id_src,
		member_id_src,
		table_id_src,
		retail_or_mail_indicator,
		dispensed_as_written,
		dose,
		strength,
		formulary_ind,
		special_drug_ind
		)
select 'optd',
       extract(year from a.fill_dt) as cal_yr,
       b.uth_rx_claim_id,
       b.uth_member_id,
       a.fill_dt,
       lpad(ndc, 11,'0'),
       a.days_sup,
       a.prescript_id,
       a.rfl_nbr::numeric,
       c.month_year_id,
       a.gnrc_ind,
       a.gnrc_nm,
       a.brnd_nm,
       a.quantity,
       a.prescriber_prov,
       a.pharm,
       ( a.charge * d.cost_factor) as chg, ( a.std_cost * d.cost_factor) as alw, null as paid_amount,
       a.deduct, a.copay, null as coins, null as cob,
       extract(year from a.fill_dt) as fsc_yr,
       a.std_cost_yr,
       null as thera_class,
       a.ahfsclss,
       a.fst_fill,
       a.clmid,
       a.patid::text,
       'rx' as table_id_src,
			 a.mail_ind,
       lpad(a.daw,2,'0'),
			 null,
			 a.strength,
			 a.form_ind,
			 a.spclt_ind
from optum_dod.rx a
  join dw_staging.optd_uth_rx_claim b
     on b.member_id_src = a.member_id_src 
    and b.rx_claim_id_src = a.clmid
    and b.data_source = 'optd'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
  left outer join reference_tables.ref_optum_cost_factor d
     on d.service_type = 'PHARM'
    and d.standard_price_year = a.std_cost_yr
 ;

raise notice 'finish optum dod';

---******************************************************************************************************************
------ ***** Optum DoD - optd *****
---******************************************************************************************************************

---create copy uth claims with optd only and distribute on member id src
drop table if exists dw_staging.optd_uth_rx_claim;

create table dw_staging.optz_uth_rx_claim
with(appendonly=true,orientation=column) as 
select * from data_warehouse.dim_uth_rx_claim_id where data_source = 'optz'
distributed by (member_id_src);


analyze dw_staging.optz_uth_rx_claim;
raise notice 'start optum zip';

--optz  9m
insert into dw_staging.pharmacy_claims (
		data_source,
		year,
		uth_rx_claim_id,
		uth_member_id,
		fill_date,
		ndc,
		days_supply,
		script_id,
		refill_count,
		month_year_id,
		generic_ind,
		generic_name,
		brand_name,
		quantity,
		provider_npi,
		pharmacy_id,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		deductible, copay, coins, cob,
		fiscal_year,
		cost_factor_year,
		therapeutic_class,
		ahfs_class,
		first_fill,
		rx_claim_id_src,
		member_id_src,
		table_id_src,
		retail_or_mail_indicator,
		dispensed_as_written,
		dose,
		strength,
		formulary_ind,
		special_drug_ind
		)
select 'optz',
       extract(year from a.fill_dt) as cal_yr,
       b.uth_rx_claim_id,
       b.uth_member_id,
       a.fill_dt,
       lpad(ndc, 11,'0'),
       a.days_sup,
       a.prescript_id,
       a.rfl_nbr::numeric,
       c.month_year_id,
       a.gnrc_ind,
       a.gnrc_nm,
       a.brnd_nm,
       a.quantity,
       a.prescriber_prov,
       a.pharm,
       ( a.charge * d.cost_factor) as chg, ( a.std_cost * d.cost_factor) as alw, null as paid_amount,
       a.deduct, a.copay, null as coins, null as cob,
       extract(year from a.fill_dt) as fsc_yr,
       a.std_cost_yr,
       null as thera_class,
       a.ahfsclss,
       a.fst_fill,
       a.clmid,
       a.patid::text,
       'rx' as table_id_src,
			 a.mail_ind,
       lpad(a.daw,2,'0'),
			 null,
			 a.strength,
			 a.form_ind,
			 a.spclt_ind
from optum_zip.rx a
  join dw_staging.optz_uth_rx_claim b
     on b.member_id_src = a.member_id_src 
    and b.rx_claim_id_src = a.clmid
    and b.data_source = 'optz'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
  left outer join reference_tables.ref_optum_cost_factor d
     on d.service_type = 'PHARM'
    and d.standard_price_year = a.std_cost_yr
 ;
raise notice 'finish optum zip';
---va
analyze dw_staging.pharmacy_claims;

end $$;

--validate 
select data_source, year, count(*) 
from dw_staging.pharmacy_claims
group by data_source, year
order by data_source, year;

--cleanup
drop table if exists dw_staging.optz_uth_rx_claim;
drop table if exists dw_staging.optd_uth_rx_claim;


