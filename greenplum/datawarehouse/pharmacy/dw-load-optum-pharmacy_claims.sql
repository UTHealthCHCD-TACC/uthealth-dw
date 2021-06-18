----optum pharmacy claims load



---******************************************************************************************************************
------ Optum Zip - optz
---******************************************************************************************************************

--create copy of diagnosis table and distribute on patid as text field
drop table dev.wc_optz_rx;

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
		data_source, year, uth_rx_claim_id, uth_member_id, fill_date, 
		ndc, days_supply, script_id, refill_count, 
		month_year_id, generic_ind, generic_name, brand_name, quantity, 
		provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount, 
		deductible, copay, coins, cob, 
		rx_claim_id_src, member_id_src, fiscal_year, 
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, year_adj, 
		therapeutic_class, ahfs_class, first_fill, script_id_src
		)						
select 'optz', extract(year from a.fill_dt) as cal_yr, b.uth_rx_claim_id, b.uth_member_id, a.fill_dt, 
       lpad(ndc, 11,'0'), a.days_sup, null as script_id, a.rfl_nbr::numeric,  
       c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm, a.quantity, 
       a.prescriber_prov, a.pharm, a.charge, a.std_cost, null as paid_amount, 
       a.deduct, a.copay, null as coins, null as cob, 
       a.clmid, a.patid::text, extract(year from a.fill_dt) as fsc_yr,
       ( a.charge * d.cost_factor) as chrg_adj , ( a.std_cost * d.cost_factor) as alw_adj, null as paid_adj, a.std_cost_yr, 
       null as thera_class, a.ahfsclss, a.fst_fill, null as script_id_src
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


select 
count(*), year 
from dev.wc_optz_rx_load group by year order by year;

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
drop table dev.wc_optd_rx;

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
		data_source, year, uth_rx_claim_id, uth_member_id, fill_date, 
		ndc, days_supply, script_id, refill_count, 
		month_year_id, generic_ind, generic_name, brand_name, quantity, 
		provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount, 
		deductible, copay, coins, cob, 
		rx_claim_id_src, member_id_src, fiscal_year, 
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, year_adj, 
		therapeutic_class, ahfs_class, first_fill, script_id_src
		)						
select 'optd', extract(year from a.fill_dt) as cal_yr, b.uth_rx_claim_id, b.uth_member_id, a.fill_dt, 
       lpad(ndc, 11,'0'), a.days_sup, null as script_id, a.rfl_nbr::numeric,  
       c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm, a.quantity, 
       a.prescriber_prov, a.pharm, a.charge, a.std_cost, null as paid_amount, 
       a.deduct, a.copay, null as coins, null as cob, 
       a.clmid, a.patid::text, extract(year from a.fill_dt) as fsc_yr,
       ( a.charge * d.cost_factor) as chrg_adj , ( a.std_cost * d.cost_factor) as alw_adj, null as paid_adj, a.std_cost_yr, 
       null as thera_class, a.ahfsclss, a.fst_fill, null as script_id_src
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

