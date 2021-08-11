----optum pharmacy claims load

/*
new variables to be added to table
retail_or_mail_indicator  bpchar(1) null,
dispensed_as_written  bpchar(2) null,
dose bpchar(50)  null,
strength bpchar(30)  null,
formulary_ind  bpchar(1) null,
special_drug_ind bpchar(1) null
*/

---******************************************************************************************************************
------ Optum Zip - optz
---******************************************************************************************************************
----optum pharmacy claims load

/*
new variables to be added to table
retail_or_mail_indicator  bpchar(1) null,
dispensed_as_written  bpchar(2) null,
dose bpchar(50)  null,
strength bpchar(30)  null,
formulary_ind  bpchar(1) null,
special_drug_ind bpchar(1) null


*/
---******************************************************************************************************************
------ Optum Zip - optz
---******************************************************************************************************************

--create copy of rx table and distribute on patid as text field
drop table if exist dev.wc_optz_rx;

create table dev.wc_optz_rx
with(appendonly=true,orientation=column)
as
	select patid::text as member_id_src, *
	from optum_zip.rx
distributed by (member_id_src);


vacuum analyze dev.wc_optz_rx;


---create copy uth claims with optz only and distribute on member id src
drop table if exists dev.wc_optz_uth_rx_claim;

create table dev.wc_optz_uth_rx_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_rx_claim_id where data_source = 'optz'
distributed by (member_id_src);


vacuum analyze dev.wc_optz_uth_rx_claim;


---work table to load
drop table dev.wc_optz_rx_load;

create table dev.wc_optz_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);

--optz
insert into dev.wc_optz_rx_load (
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
from dev.wc_optz_rx a
  join dev.wc_optz_uth_rx_claim b
     on b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
    and b.data_source = 'optz'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
  left outer join reference_tables.ref_optum_cost_factor d
     on d.service_type = 'PHARM'
    and d.standard_price_year = a.std_cost_yr
 ;


vacuum analyze dev.wc_optz_rx_load;


select count(*), year
from dev.wc_optz_rx_load
group by year order by year;

select count(*), year from optum_zip.rx group by year order by year;


---delete old recs
delete from data_warehouse.pharmacy_claims where data_source ='optz';

---insert new optz recs
insert into data_warehouse.pharmacy_claims
select * from dev.wc_optz_rx_load;



---******************************************************************************************************************
------ Optum DoD - optd
---******************************************************************************************************************

--create copy of diagnosis table and distribute on patid as text field
drop table if exists dev.wc_optd_rx;

create table dev.wc_optd_rx
with(appendonly=true,orientation=column)
as
	select patid::text as member_id_src, *
	from optum_dod.rx
distributed by (member_id_src);


vacuum analyze dev.wc_optd_rx;


---create copy uth claims with optz only and distribute on member id src
drop table if exists dev.wc_optd_uth_rx_claim;

create table dev.wc_optd_uth_rx_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_rx_claim_id where data_source = 'optd'
distributed by (member_id_src);


vacuum analyze dev.wc_optd_uth_rx_claim;


---work table to load
drop table dev.wc_optd_rx_load;

create table dev.wc_optd_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);

--optd
insert into dev.wc_optd_rx_load (
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
from dev.wc_optd_rx a
  join dev.wc_optd_uth_rx_claim b
     on b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
    and b.data_source = 'optd'
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
  left outer join reference_tables.ref_optum_cost_factor d
     on d.service_type = 'PHARM'
    and d.standard_price_year = a.std_cost_yr
 ;



----********************************
--- validate and load to production
----********************************
vacuum analyze dev.wc_optd_rx_load;

select count(*), year
from dev.wc_optd_rx_load group by year order by year;


select count(*), year from optum_dod.rx group by year order by year;



---delete old recs
delete from data_warehouse.pharmacy_claims where data_source = 'optd';



---insert new optz recs
insert into data_warehouse.pharmacy_claims
select * from dev.wc_optd_rx_load;


vacuum analyze data_warehouse.pharmacy_claims;


---validate
select count(*), data_source, year
from data_warehouse.pharmacy_claims pc
group by data_source , year
order by data_source , year
;




-----cleanup ************************

---
drop table if exists dev.wc_optz_rx_load;

drop table if exists dev.wc_optz_rx;

drop table if exists dev.wc_optz_uth_rx_claim;

----
drop table if exists dev.wc_optd_rx_load;

drop table if exists dev.wc_optd_rx;

drop table if exists dev.wc_optd_uth_rx_claim;
