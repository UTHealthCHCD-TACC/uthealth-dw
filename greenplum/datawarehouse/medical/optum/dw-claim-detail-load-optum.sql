----------------------------------------------------------------------
-- ***   Script to load data_warehouse.claim_detail for optd and optz 
--- 7/8/2021 w coughlin: cleaned up and corrected 
----------------------------------------------------------------------

--Optum dod load

---empty claim detail table to load records into before insert into data warehouse
drop table if exists dev.wc_claim_detail_optd;

create table dev.wc_claim_detail_optd
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_detail limit 0
distributed by (member_id_src);


---medical table distributed by member id 
drop table if exists dev.wc_optd_medical;

create table dev.wc_optd_medical
with(appendonly=true,orientation=column)
as select patid::text as mem_id_src, * from optum_dod.medical
where year = 2020
distributed by (mem_id_src);



---uth claims for optd only distributed by member id 
drop table if exists dev.wc_optd_uth_claim;

create table dev.wc_optd_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);


vacuum analyze dev.wc_optd_uth_claim;

vacuum analyze dev.wc_optd_medical;

vacuum analyze dev.wc_claim_detail_optd;

--------------

--load OPTD to dev claim_detail table
insert into dev.wc_claim_detail_optd (
	data_source, year, uth_claim_id, claim_sequence_number, 
	uth_member_id, from_date_of_service, to_date_of_service, month_year_id, 
	place_of_service, admit_date, discharge_date, cpt_hcpcs, 
	procedure_type, proc_mod_1, proc_mod_2,	revenue_cd,
	charge_amount, allowed_amount, paid_amount,	copay,
	deductible, coins, cob,	bill_type_inst,	
	bill_type_class, bill_type_freq, units,	drg_cd,
	claim_id_src, member_id_src, table_id_src, claim_sequence_number_src, 
	cob_type, fiscal_year, cost_factor_year, discharge_status
)	
select 'optd', extract(year from a.fst_dt) as year, b.uth_claim_id, null as claim_seq, 
       b.uth_member_id, a.fst_dt, a.lst_dt, get_my_from_date(a.fst_dt) as month_year, 
       a.pos, d.admit_date, d.disch_date, a.proc_cd, 
       null, substring(a.procmod, 1,1), substring(a.procmod, 2,1), a.rvnu_cd,
       (a.charge * c.cost_factor) as charge_amount, (a.std_cost * c.cost_factor) as allowed_amount, null as paid_amount, a.copay,
       a.deduct, a.coins, null as cob, substring(a.bill_type,1,1),
       substring(a.bill_type,2,1), substring(a.bill_type,3,1), a.units, a.drg, 
       a.clmid, a.patid::text, 'medical', a.clmseq, 
       a.cob as cob_type, a."year", c.standard_price_year, d.dstatus
from dev.wc_optd_medical a   --optum_dod.medical a
	join dev.wc_optd_uth_claim b  --data_warehouse.dim_uth_claim_id b 
	   on b.member_id_src = a.mem_id_src 
	  and b.claim_id_src = a.clmid
	join reference_tables.ref_optum_cost_factor c
	   on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1)) 
	  and c.standard_price_year = a.std_cost_yr::int
	left outer join optum_dod.confinement d
	  on a.conf_id = d.conf_id
;
---------------------

---va
vacuum analyze dev.wc_claim_detail_optd;

--verify
select count(*) , year 
from dev.wc_claim_detail_optd
group by year 
order by year 


--delete from claim detail
delete from data_warehouse.claim_detail where data_source = 'optd';

--load new records
insert into data_warehouse.claim_detail 
select * from dev.wc_claim_detail_optd 
;

--verify dw
select count(*), year 
from data_warehouse.claim_detail cd 
where data_source = 'optd'
group by year 
order by year;

--cross verify original data
select count(*), year  
from optum_dod.medical m 
group by year 
order by year 

--OPTD final cleanup
drop table dev.wc_optd_medical;  drop table dev.wc_claim_detail_optd; drop table dev.wc_optd_uth_claim;


-------------------------

---**********************************************************************************************************
-------------------- optz -------------------------------------
---**********************************************************************************************************
drop table if exists dev.wc_claim_detail_optz;

create table dev.wc_claim_detail_optz
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_detail limit 0
distributed by (member_id_src);

---optz medical distributed on member 
drop table if exists dev.wc_optz_medical;

create table dev.wc_optz_medical
with(appendonly=true,orientation=column)
as select patid::text as mem_id_src, * from optum_zip.medical
distributed by (clmid, mem_id_src);

vacuum analyze dev.wc_optz_medical;

---uth claims for optd only
drop table if exists dev.wc_optz_uth_claim;

create table dev.wc_optz_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (claim_id_src , member_id_src);

--va work tables
vacuum analyze dev.wc_claim_detail_optz;

vacuum analyze dev.wc_optz_medical;

vacuum analyze dev.wc_optz_uth_claim;


---------------optz insert 
insert into dev.wc_claim_detail_optz(
	data_source, year, uth_claim_id, claim_sequence_number, 
	uth_member_id, from_date_of_service, to_date_of_service, month_year_id, 
	place_of_service, admit_date, discharge_date, cpt_hcpcs, 
	procedure_type, proc_mod_1, proc_mod_2,	revenue_cd,
	charge_amount, allowed_amount, paid_amount,	copay,
	deductible, coins, cob,	bill_type_inst,	
	bill_type_class, bill_type_freq, units,	drg_cd,
	claim_id_src, member_id_src, table_id_src, claim_sequence_number_src, 
	cob_type, fiscal_year, cost_factor_year, discharge_status
)	
select 'optz', extract(year from a.fst_dt) as year, b.uth_claim_id, null as claim_seq, 
      b.uth_member_id, a.fst_dt, a.lst_dt, get_my_from_date(a.fst_dt) as month_year, 
       a.pos, d.admit_date, d.disch_date, a.proc_cd, 
       null, substring(a.procmod, 1,1), substring(a.procmod, 2,1), a.rvnu_cd,
       (a.charge * c.cost_factor) as charge_amount, (a.std_cost * c.cost_factor) as allowed_amount, null as paid_amount, a.copay,
       a.deduct, a.coins, null as cob, substring(a.bill_type,1,1),
       substring(a.bill_type,2,1), substring(a.bill_type,3,1), a.units, a.drg, 
       a.clmid, a.patid::text, 'medical', a.clmseq, 
       a.cob as cob_type, a."year", c.standard_price_year, d.dstatus
from dev.wc_optz_medical a 
	join dev.wc_optz_uth_claim b
	   on b.claim_id_src = a.clmid
	  and b.member_id_src = a.mem_id_src
	join reference_tables.ref_optum_cost_factor c
		on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1)) 
	   and c.standard_price_year = a.std_cost_yr::int
	left outer join optum_zip.confinement d
		on d.conf_id = a.conf_id
	left outer join reference_tables.ref_optum_bill_type_from_tos e
		on e.tos = a.tos_cd
;



--validate before load
select count(*), year 
from dev.wc_claim_detail_optz 
group by year 
order by year ;

--validate counts of raw data vs load above
select count(*), year 
from optum_zip.medical 
group by year 
order by year;

---delete from dw 
delete from data_warehouse.claim_detail where data_source = 'optz';


--load new data
insert into data_warehouse.claim_detail 
select * from dev.wc_claim_detail_optz
;


--verify dw 
select year, count(*)
from data_warehouse.claim_detail
where data_source = 'optz'
group by year
order by year;



vacuum analyze data_warehouse.claim_detail;

--final check
select count(*), data_source, year 
from data_warehouse.claim_detail 
group by data_source , year 
order by data_source , year ;

--final cleanup
drop table dev.wc_optz_medical;  drop table dev.wc_claim_detail_optz; drop table dev.wc_optz_uth_claim;