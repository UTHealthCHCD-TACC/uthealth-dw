/*
 * Remove old records
 */
select count(*) from data_warehouse.claim_header where data_source in ('optd','optz');

delete from data_warehouse.claim_header where data_source in ('optd','optz');


vacuum analyze data_warehouse.claim_header;


vacuum analyze data_warehouse.claim_detail;
/
 * *
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
-- Full years = 8132 seconds = 2h15m

 select * from data_warehouse.claim_header where data_source = 'optd';

---create work tables distributed on the join value

---working table
drop table if exists dev.wc_claim_header_optum;

create table dev.wc_claim_header_optum
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_header limit 0
distributed by (member_id_src);


---uth claims for optd only
create table dev.wc_optd_uth_claim
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);


---drop if exist
drop table if exists dev.wc_optd_medical;


--optd med only
create table dev.wc_optd_medical 
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from optum_dod.medical
distributed by (patid);



--admit id for optd only
drop table dev.wc_uth_admission_id_optd; 

create table dev.wc_uth_admission_id_optd 
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_admission_id where data_source = 'optd'
distributed by (member_id_src);


--optd
--insert into data_warehouse.claim_header(
insert into dev.wc_claim_header_optum(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		year_adj, claim_type,
	    from_date_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, 
		fiscal_year, admission_id_src, uth_admission_id 
		)
	select distinct on(uthc.uth_claim_id)
	'optd', uthc.uth_member_id, m.patid::text, uthc.uth_claim_id, m.clmid, extract(year from (min(m.fst_dt) over(partition by uthc.uth_claim_id))),
	m.std_cost_yr::int,
	cf.claim_type_code,
	min(m.fst_dt) over(partition by uthc.uth_claim_id) as from_date_of_service,
	sum(m.charge) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum(m.std_cost) over(partition by uthc.uth_claim_id) as total_allowed_amount, 
	null as total_paid_amount,
	sum((m.charge * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum((m.std_cost * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_allowed_amount_adj, 
	null as total_paid_amount_adj,
	m.year, m.conf_id , adm.uth_admission_id 
from dev.wc_optd_medical m 
--from optum_dod.medical m
    join dev.wc_optd_uth_claim uthc 
	--join data_warehouse.dim_uth_claim_id uthc 
		on m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	join reference_tables.ref_optum_cost_factor cf 
		on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) 
		and cf.standard_price_year = m.std_cost_yr::int
    left outer join dev.wc_uth_admission_id_optd adm
       on adm.member_id_src = m.patid::text 
      and adm.admission_id_src = m.conf_id 
      and adm."year" = m."year" 
;



---drop if exist
drop table if exists dev.wc_optd_medical;

drop table if exists dev.wc_optd_uth_claim;


---drop if exist
drop table if exists dev.wc_optz_medical;
create table dev.wc_optz_medical 
with(appendonly=true,orientation=column)
as select * from optum_zip.medical
distributed by (patid);


create table dev.wc_optz_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (member_id_src);


create table dev.wc_uth_admission_id_optz 
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_admission_id where data_source = 'optz'
distributed by (member_id_src);


---optz claim header
insert into dev.wc_claim_header_optum(
--insert into data_warehouse.claim_header(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		year_adj, claim_type,
	    from_date_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj, 
		fiscal_year, admission_id_src ,uth_admission_id 
		)
	select distinct on(uthc.uth_claim_id)
	'optz', uthc.uth_member_id, m.patid::text, uthc.uth_claim_id, m.clmid, extract(year from (min(m.fst_dt) over(partition by uthc.uth_claim_id))),
	m.std_cost_yr::int,
	cf.claim_type_code,
	min(m.fst_dt) over(partition by uthc.uth_claim_id) as from_date_of_service,
	sum(m.charge) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum(m.std_cost) over(partition by uthc.uth_claim_id) as total_allowed_amount, 
	null as total_paid_amount,
	sum((m.charge * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_charge_amount, 
	sum((m.std_cost * cf.cost_factor)) over(partition by uthc.uth_claim_id) as total_allowed_amount_adj, 
	null as total_paid_amount_adj,
	m.year, m.conf_id , adm.uth_admission_id 
from dev.wc_optz_medical m
--from optum_zip.medical m
    join dev.wc_optz_uth_claim uthc 
	--join data_warehouse.dim_uth_claim_id uthc 
		on m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	join reference_tables.ref_optum_cost_factor cf 
	    on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) 
	   and cf.standard_price_year = m.std_cost_yr::int
    left outer join dev.wc_uth_admission_id_optz adm
       on adm.member_id_src = m.patid::text 
      and adm.admission_id_src = m.conf_id 
      and adm."year" = m."year" 
     ;

    
    --va
vacuum analyze dev.wc_claim_header_optum;

--remove existing records from claim header
delete from data_warehouse.claim_header where data_source in ('optd','optz');


---load new records into claim header
insert into data_warehouse.claim_header 
select * from dev.wc_claim_header_optum;

--va
vacuum analyze data_warehouse.claim_header;


--validate
select count(*), count(distinct uth_claim_id), data_source
from data_warehouse.claim_header 
group by data_source; 


----- *CLEANUP
drop table dev.wc_claim_header_optum;

drop table dev.wc_optd_uth_claim;

drop table dev.wc_optd_medical;

drop table dev.wc_uth_admission_id_optd ;

drop table dev.wc_optz_uth_claim;

drop table dev.wc_optz_medical;

drop table dev.wc_uth_admission_id_optz ;
