-- Claim Header
-- dw-claim-header-load-optum
-- DROP TABLE if exists dev.tu_claim_header;

CREATE TABLE dev.tu_claim_header (
	data_source bpchar(4) NULL,
	"year" int2 NULL,
	uth_claim_id numeric NULL,
	uth_member_id int8 NULL,
	from_date_of_service date NULL,
	claim_type text NULL,
	uth_admission_id numeric NULL,
	admission_id_src text NULL,
	total_charge_amount numeric(13,2) NULL,
	total_allowed_amount numeric(13,2) NULL,
	total_paid_amount numeric(13,2) NULL,
	-- NEW
	total_charge_amount_adj numeric(13,2) NULL,
	total_allowed_amount_adj numeric(13,2) NULL,
	total_paid_amount_adj numeric(13,2) NULL,
	year_adj int2 NULL,
	-- END NEW
	claim_id_src text NULL,
	member_id_src text NULL,
	table_id_src text NULL,
	bill_type bpchar(3) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib
)
DISTRIBUTED BY (uth_member_id);


delete from dev.tu_claim_header where data_source in ('optd','optz');

vacuum analyze dev.tu_claim_header;

insert into dev.tu_claim_header(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		-- NEW
		year_adj, claim_type,
		-- END NEW
	    from_date_of_service, -- Not present in DW place_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount
		-- NEW
		, total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj
		-- END NEW
		)
	select 'optd', uthc.uth_member_id, m.patid, uthc.uth_claim_id, m.clmid, extract(year from (min(m.fst_dt))),
	-- NEW
	m.std_cost_yr::int,
	cf.claim_type_code,
	-- END NEW
	min(m.fst_dt) as from_date_of_service,-- Not present in DW null as place_of_service,
	sum(m.charge) as total_charge_amount, 
	sum(m.std_cost) as total_allowed_amount, 
	null as total_paid_amount, 
	-- NEW
	sum((m.charge * cf.cost_factor)) as total_charge_amount, 
	sum((m.std_cost * cf.cost_factor)) as total_allowed_amount_adj, 
	null as total_paid_amount_adj
	-- END NEW
from optum_zip.medical m
	join dev.tu_dim_uth_claim_id uthc 
		on uthc.data_source = 'optd' 
		and m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	-- NEW
	join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) and cf.standard_price_year = m.std_cost_yr::int
	-- END NEW
group by 1, 2, 3, 4, 5
-- NEW
, m.std_cost_yr, cf.claim_type_code
-- END NEW
;


insert into dev.tu_claim_header(
		data_source, uth_member_id, member_id_src, uth_claim_id, claim_id_src, year,
		-- NEW
		year_adj, claim_type,
		-- END NEW
	    from_date_of_service, -- Not present in DW place_of_service,
		total_charge_amount, total_allowed_amount, total_paid_amount
		-- NEW
		, total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj
		-- END NEW
		)
	select 'optz', uthc.uth_member_id, m.patid, uthc.uth_claim_id, m.clmid, extract(year from (min(m.fst_dt))),
	-- NEW
	m.std_cost_yr::int,
	cf.claim_type_code,
	-- END NEW
	min(m.fst_dt) as from_date_of_service,-- Not present in DW null as place_of_service,
	sum(m.charge) as total_charge_amount, 
	sum(m.std_cost) as total_allowed_amount, 
	null as total_paid_amount, 
	-- NEW
	sum((m.charge * cf.cost_factor)) as total_charge_amount, 
	sum((m.std_cost * cf.cost_factor)) as total_allowed_amount_adj, 
	null as total_paid_amount_adj
	-- END NEW
from optum_zip.medical m
	join dev.tu_dim_uth_claim_id uthc 
		on uthc.data_source = 'optz' 
		and m.patid::text = uthc.member_id_src 
		and m.clmid = uthc.claim_id_src
	-- NEW
	join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) and cf.standard_price_year = m.std_cost_yr::int
	-- END NEW
group by 1, 2, 3, 4, 5
-- NEW
, m.std_cost_yr, cf.claim_type_code
-- END NEW
;


select count(*) from dev.tu_claim_header;


-- Claim Detail

--DROP TABLE if exists dev.tu_claim_detail;

CREATE TABLE dev.tu_claim_detail (
	data_source bpchar(4) NULL,
	"year" int2 NULL,
	year_adj int2 NULL,
	uth_claim_id numeric NULL,
	claim_sequence_number int4 NULL,
	uth_member_id int8 NULL,
	from_date_of_service date NULL,
	to_date_of_service date NULL,
	month_year_id int4 NULL,
	perf_provider_id text NULL,
	bill_provider_id text NULL,
	ref_provider_id text NULL,
	place_of_service text NULL,
	network_ind bool NULL,
	network_paid_ind bool NULL,
	admit_date date NULL,
	discharge_date date NULL,
	procedure_cd text NULL,
	procedure_type text NULL,
	proc_mod_1 bpchar(1) NULL,
	proc_mod_2 bpchar(1) NULL,
	revenue_cd bpchar(4) NULL,
	charge_amount numeric(13,2) NULL,
	allowed_amount numeric(13,2) NULL,
	paid_amount numeric(13,2) NULL,
	charge_amount_adj numeric(13,2) NULL,
	allowed_amount_adj numeric(13,2) NULL,
	paid_amount_adj numeric(13,2) NULL,
	copay numeric(13,2) NULL,
	deductible numeric(13,2) NULL,
	coins numeric(13,2) NULL,
	cob numeric(13,2) NULL,
	bill_type_inst bpchar(1) NULL,
	bill_type_class bpchar(1) NULL,
	bill_type_freq bpchar(1) NULL,
	units int4 NULL,
	drg_cd text NULL,
	claim_id_src text NULL,
	member_id_src text NULL,
	table_id_src text NULL,
	claim_sequence_number_src text NULL,
	cob_type text NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib
)
DISTRIBUTED BY (uth_member_id);

delete from dev.tu_claim_detail where data_source in ('optd','optz');

vacuum analyze dev.tu_claim_header;

vacuum analyze optum_zip.medical;


insert into dev.tu_claim_detail(
	data_source, year, 
	-- NEW
	year_adj,
	-- END NEW
	uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,	
	perf_provider_id, bill_provider_id, ref_provider_id, place_of_service,
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, 
	-- NEW
	charge_amount_adj, allowed_amount_adj, paid_amount_adj,
	-- END NEW
	copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd,
	claim_id_src, member_id_src, table_id_src)
select uth.data_source, uth.data_year, 
	-- NEW
	m.std_cost_yr::int,
	-- END NEW
	uth.uth_claim_id, uth.uth_member_id,
	trunc(m.clmseq::int4), m.clmseq,
	m.fst_dt, m.lst_dt, get_my_from_date(m.fst_dt),
	m.prov::text, m.bill_prov::text, m.refer_prov::text, m.pos,
	null, null, --No mappings for network fields
	conf.admit_date, conf.disch_date,
	m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
	m.rvnu_cd, 
	m.charge, m.std_cost, null, 
	-- NEW
	(m.charge * cf.cost_factor), (m.std_cost * cf.cost_factor), null,
	-- END NEW
	m.copay, null, m.coins, null, m.cob, --NOTE: cob is an int, but optum is varchar -> m.cob (Find where it is a numeric value, set other to zero), 	--NOTE: Left pad revenu_cd to 4 digits with leading zero
	bt.inst_code, bt.class_code, null, m.units, --NOTE: bill_type_freq is null for optum
	m.drg,
	uth.claim_id_src, uth.member_id_src, 'medical'
from --data_warehouse.claim_header ch join 
dev.tu_dim_uth_claim_id uth --on ch.uth_member_id = uth.uth_member_id and ch.uth_claim_id=uth.uth_claim_id
join optum_zip.medical m on uth.claim_id_src=m.clmid::text and uth.member_id_src=m.patid::text
-- NEW
join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) and cf.standard_price_year = m.std_cost_yr::int
-- END NEW
left outer join optum_zip.confinement conf on m.conf_id=conf.conf_id
left outer join reference_tables.ref_optum_bill_type_from_tos bt on m.tos_cd=bt.tos
where uth.data_source='optd';


-- RX imports

drop table if exists data_warehouse.pharmacy_claims;


create table data_warehouse.pharmacy_claims ( 
		data_source char(4),
		year int2, 
		-- NEW
		data_year int2,
		year_adj int2,
		-- END NEW
		uth_rx_claim_id int8,
		uth_member_id int8,
		fill_date date,
		ndc char(11) check (length(ndc)=11),
		days_supply int2,
		script_id text, 
		refill_count int2,
		month_year_id int4,
		generic_ind char(1),
		generic_name text,
		brand_name text,
		quantity int4, 
		provider_npi text,
		pharmacy_id text,
		total_charge_amount numeric(13,2),
		total_allowed_amount numeric(13,2),
		total_paid_amount numeric(13,2),
		-- NEW
		total_charge_amount_adj numeric(13,2),
		total_allowed_amount_adj numeric(13,2),
		total_paid_amount_adj numeric(13,2),
		-- END NEW
		deductible numeric(13,2),
		copay numeric(13,2),
		coins numeric(13,2),
		cob numeric(13,2),
		rx_claim_id_src text,
		member_id_src text
)
with (appendonly=true, orientation = column)
distributed by (uth_member_id);


vacuum analyze data_warehouse.pharmacy_claims;

---optum zip
insert into data_warehouse.pharmacy_claims (
		data_source, year, 
		-- NEW
		year_adj,
		-- END NEW
		uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, 
		total_charge_amount, total_allowed_amount, total_paid_amount,
		-- NEW
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj,
		-- END NEW
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src,
		-- NEW
		data_year
		-- END NEW
		)			
select 'optz', extract(year from a.fill_dt), a.std_cost_yr, b.uth_rx_claim_id, b.uth_member_id, patid::text || lpad(ndc, 11,'0') || a.fill_dt,
       lpad(ndc, 11,'0'), a.days_sup, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, 
       a.charge, a.std_cost, null, 
       (a.charge * cf.cost_factor), (a.std_cost * cf.cost_factor), null,
       a.deduct, a.copay, null, null, a.clmid, a.patid::text, a.year
from optum_zip.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optz' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
	-- NEW
	join reference_tables.ref_optum_cost_factor cf on cf.service_type = 'PHARM' and cf.standard_price_year = a.std_cost_yr::int
	-- END NEW    
 ;

select count(*)
from data_warehouse.pharmacy_claims pc 
where data_source = 'optz';



--optum dod
insert into data_warehouse.pharmacy_claims (
		data_source, year, 
		-- NEW
		year_adj,
		-- END NEW
		uth_rx_claim_id, uth_member_id, script_id, 
		ndc, days_supply, refill_count, fill_date, month_year_id, generic_ind, generic_name, brand_name, 
		quantity, provider_npi, pharmacy_id, 
		total_charge_amount, total_allowed_amount, total_paid_amount,
		-- NEW
		total_charge_amount_adj, total_allowed_amount_adj, total_paid_amount_adj,
		-- END NEW
		deductible, copay, coins, cob, rx_claim_id_src, member_id_src,
		-- NEW
		data_year
		-- END NEW
		)			
select 'optd', extract(year from a.fill_dt), a.std_cost_yr, b.uth_rx_claim_id, b.uth_member_id, patid::text || lpad(ndc, 11,'0') || a.fill_dt,
       lpad(ndc, 11,'0'), a.days_sup, a.rfl_nbr::numeric, a.fill_dt, c.month_year_id, a.gnrc_ind, a.gnrc_nm, a.brnd_nm,
       a.quantity, a.prescriber_prov, a.pharm, 
       a.charge, a.std_cost, null, 
       (a.charge * cf.cost_factor), (a.std_cost * cf.cost_factor), null,
       a.deduct, a.copay, null, null, a.clmid, a.patid::text, a.year
from optum_zip.rx a 
  join data_warehouse.dim_uth_rx_claim_id b 
     on b.data_source = 'optd' 
    and b.member_id_src = a.patid::text
    and b.rx_claim_id_src = a.clmid
  join reference_tables.ref_month_year c 
    on c.month_int = extract(month from a.fill_dt)
    and c.year_int = extract(year from a.fill_dt)
	-- NEW
	join reference_tables.ref_optum_cost_factor cf on cf.service_type = 'PHARM' and cf.standard_price_year = a.std_cost_yr::int
	-- END NEW    
 ;

-- Insert
--insert into optum_zip.confinement
--select 0, * from ext_confinement;
-- *** TCU ***
-- Using existing import table as fake external source.
--select c.std_cost, c.std_cost_yr, c.tos_cd, cf.cost_factor, left(c.tos_cd, (position('.' in c.tos_cd)-1)) as service_type_code, (c.std_cost * cf.cost_factor) as adj_cost
--from optum_zip.confinement c
--join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(c.tos_cd, (position('.' in c.tos_cd)-1)) and cf.standard_price_year = c.std_cost_yr 
--where c.std_cost_yr <> 2019
--limit 100;
-- Looking at records in medical table so we can find some non-2019 cost years.
--select c.std_cost, c.std_cost_yr::int, c.tos_cd, cf.cost_factor, left(c.tos_cd, (position('.' in c.tos_cd)-1)) as service_type_code, (c.std_cost * cf.cost_factor) as adj_cost
--from optum_zip.medical c
--join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(c.tos_cd, (position('.' in c.tos_cd)-1)) and cf.standard_price_year = c.std_cost_yr::int
--where c.std_cost_yr::int <> 2019
--limit 100;
-- Looking at records in rx table so we can find some non-2019 cost years.
--select c.std_cost, c.std_cost_yr, cf.cost_factor, 'PHARM' as service_type_code, (c.std_cost * cf.cost_factor) as adj_cost
--from optum_zip.rx c
--join reference_tables.ref_optum_cost_factor cf on cf.service_type = 'PHARM' and cf.standard_price_year = c.std_cost_yr
--where c.std_cost_yr::int <> 2019
--limit 100;

