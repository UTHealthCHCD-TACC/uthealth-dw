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
------ Medicare Texas - mcrt
---******************************************************************************************************************

--joe: had patid before, changed to bene_id
--create copy of pharm table and distribute on bene_id as text field
drop table if exists dev.wc_mcrt_rx;

create table dev.wc_mcrt_rx
with(appendonly=true,orientation=column)
as
	select bene_id::text as member_id_src, *
	from medicare_texas.pde_file
distributed by (member_id_src);


vacuum analyze dev.wc_mcrt_rx;


---create copy uth claims with mcrt only and distribute on member id src
drop table if exists dev.wc_mcrt_uth_rx_claim;

create table dev.wc_mcrt_uth_rx_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_rx_claim_id where data_source = 'mcrt'
distributed by (member_id_src);


vacuum analyze dev.wc_mcrt_uth_rx_claim;


---work table to load
drop table if exists dev.wc_mcrt_rx_load;

create table dev.wc_mcrt_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);


---Medicare Texas
insert into dev.wc_mcrt_rx_load (
	data_source, year, uth_rx_claim_id, uth_member_id, fill_date,
	ndc, days_supply, script_id, refill_count,
	month_year_id, generic_ind, generic_name, brand_name, quantity,
	provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
	deductible, copay, coins, cob,
	rx_claim_id_src, member_id_src, fiscal_year,
	total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, year_adj,
	therapeutic_class, ahfs_class, first_fill, script_id_src, retail_or_mail_indicator,
	dispensed_as_written, dose, strength, formulary_ind, special_drug_ind
	)
select 'mcrt', a.year::int, b.uth_rx_claim_id, b.uth_member_id, a.srvc_dt::date,
	a.prod_srvc_id, trunc(a.days_suply_num::numeric, 0)::int, a.bene_id || a.prod_srvc_id || a.srvc_dt, a.fill_num::numeric,
	c.month_year_id, a.brnd_gnrc_cd, a.gnn, a.bn, a.qty_dspnsd_num::numeric,
	a.srvc_prvdr_id, a.rx_srvc_rfrnc_num, a.tot_rx_cst_amt::numeric, null, a.ptnt_pay_amt::numeric,
	null, null, null, null,
	a.pde_id, a.bene_id, a.year::int2,
	null, null, null, null,
	null, null, null, null, null,
	lpad(a.daw_prod_slctn_cd,2,'0'), a.gcdf_desc, a.str, null, null
from dev.wc_mcrt_rx a
join dev.wc_mcrt_uth_rx_claim b on b.data_source = 'mcrt'
	and b.member_id_src = a.bene_id
	and b.rx_claim_id_src = a.pde_id
join reference_tables.ref_month_year c on c.month_int = extract(month from a.srvc_dt::date)
	and c.year_int = extract(year from a.srvc_dt::date);

-- doesn't have logic for first fill ?

--prescript id
with updmcrt as
(
	select b.rx_srvc_rfrnc_num , b.bene_id , b.pde_id , b."year",
	       row_number () over (partition by b.bene_id , b.pde_id , b.year order by srvc_dt ) as rn
	from dev.wc_mcrt_rx b
)
update dev.wc_mcrt_rx_load a set script_id_src = updmcrt.rx_srvc_rfrnc_num
   from updmcrt
   where a.member_id_src = updmcrt.bene_id
     and a.rx_claim_id_src = updmcrt.pde_id
     and a.fiscal_year = updmcrt."year"::int2
     and updmcrt.rn = 1
;

----********************************
--- validate and load to production
----********************************

vacuum analyze dev.wc_mcrt_rx_load;

select
count(*), year
from dev.wc_mcrt_rx_load group by year order by year;

select count(*), year from medicare_texas.pde_file group by year order by year;

---delete old recs
delete from data_warehouse.pharmacy_claims where data_source ='mcrt';

---insert new mcrt recs
insert into data_warehouse.pharmacy_claims
select * from dev.wc_mcrt_rx_load;


-- cleanup
drop table dev.wc_mcrt_rx; drop table dev.wc_mcrt_uth_rx_claim; drop table dev.wc_mcrt_rx_load;



---******************************************************************************************************************
------ Medicare National  - mcrn
---******************************************************************************************************************


--create copy of pharm table and distribute on bene_id as text field
drop table if exists dev.wc_mcrn_rx;

create table dev.wc_mcrn_rx
with(appendonly=true,orientation=column)
as
	select bene_id::text as member_id_src, *
	from medicare_national.pde_file
distributed by (member_id_src);

vacuum analyze dev.wc_mcrn_rx;


---create copy uth claims with mcrn only and distribute on member id src
drop table if exists dev.wc_mcrn_uth_rx_claim;

create table dev.wc_mcrn_uth_rx_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_rx_claim_id where data_source = 'mcrn'
distributed by (member_id_src);


vacuum analyze dev.wc_mcrn_uth_rx_claim;


---work table to load
drop table if exists dev.wc_mcrn_rx_load;

create table dev.wc_mcrn_rx_load
with(appendonly=true,orientation=column)
as select * from data_warehouse.pharmacy_claims limit 0
distributed by (uth_member_id);


insert into dev.wc_mcrn_rx_load (
	data_source, year, uth_rx_claim_id, uth_member_id, fill_date,
	ndc, days_supply, script_id, refill_count,
	month_year_id, generic_ind, generic_name, brand_name, quantity,
	provider_npi, pharmacy_id, total_charge_amount, total_allowed_amount, total_paid_amount,
	deductible, copay, coins, cob,
	rx_claim_id_src, member_id_src, fiscal_year,
	total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, year_adj,
	therapeutic_class, ahfs_class, first_fill, script_id_src, retail_or_mail_indicator,
	dispensed_as_written, dose, strength, formulary_ind, special_drug_ind
		)
select 'mcrn',a.year::int, b.uth_rx_claim_id, b.uth_member_id, a.srvc_dt::date,
	a.prod_srvc_id, trunc(a.days_suply_num::numeric, 0)::int, a.bene_id || a.prod_srvc_id || a.srvc_dt, a.fill_num::numeric,
	c.month_year_id, a.brnd_gnrc_cd, a.gnn, a.bn, a.qty_dspnsd_num::numeric,
	a.srvc_prvdr_id, a.rx_srvc_rfrnc_num, a.tot_rx_cst_amt::numeric, null, a.ptnt_pay_amt::numeric,
	null, null, null, null,
	a.pde_id, a.bene_id, a.year::int2,
	null, null, null, null,
	null, null, null, null, null,
	lpad(a.daw_prod_slctn_cd,2,'0'), a.gcdf_desc, a.str, null, null
from dev.wc_mcrn_rx a
  join dev.wc_mcrn_uth_rx_claim b
     on b.data_source = 'mcrn'
    and b.member_id_src = a.bene_id
    and b.rx_claim_id_src = a.pde_id
  join reference_tables.ref_month_year c
    on c.month_int = extract(month from srvc_dt::date)
    and c.year_int = extract(year from srvc_dt::date)
 ;

--prescript id
with updmcrn as
(
	select b.rx_srvc_rfrnc_num , b.bene_id , b.pde_id , b."year",
	       row_number () over (partition by b.bene_id , b.pde_id , b.year order by srvc_dt ) as rn
	from dev.wc_mcrn_rx b
)
update dev.wc_mcrn_rx_load a set script_id_src = updmcrn.rx_srvc_rfrnc_num
   from updmcrn
   where a.member_id_src = updmcrn.bene_id
     and a.rx_claim_id_src = updmcrn.pde_id
     and a.fiscal_year = updmcrn."year"::int2
     and updmcrn.rn = 1
;

----********************************
--- validate and load to production
----********************************

vacuum analyze dev.wc_mcrn_rx_load;

select
count(*), year
from dev.wc_mcrn_rx_load group by year order by year;

select count(*), year from medicare_national.pde_file group by year order by year;

---delete old recs
delete from data_warehouse.pharmacy_claims where data_source ='mcrn';

---insert new mcrn recs
insert into data_warehouse.pharmacy_claims
select * from dev.wc_mcrn_rx_load;


--cleanup
drop table dev.wc_mcrn_rx; drop table dev.wc_mcrn_uth_rx_claim; drop table dev.wc_mcrn_rx_load;
