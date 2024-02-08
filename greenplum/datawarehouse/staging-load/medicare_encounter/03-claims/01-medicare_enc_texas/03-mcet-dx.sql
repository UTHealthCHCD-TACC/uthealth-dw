/* ******************************************************************************************************
 *  load claim diag for medicare texas
 * ******************************************************************************************************
 *  Author   || Date      || Notes
 * ****************************************************************************************************** 
 *  Xiaorui  || 10/03/23  || Rewrote -- ETL first then load
 * ******************************************************************************************************
  *  iperez  || 02/06/2026 || modified existing medicare script for medicare enc tables
 * ****************************************************************************************************** 
 * */

select 'mcet claim diag script started at ' || current_timestamp as message;

/**************************
 * First make an ETL (to reduce grabbing blank diagnosis code fields)
 **************************/
--initialize table
drop table if exists dw_staging.mcet_diag_etl;

create table dw_staging.mcet_diag_etl as
select enc_join_key, bene_id, clm_from_dt::date, 0 as pos, icd_dgns_cd1 as dx,
	'version'::text as dx_version,
	clm_poa_ind_sw1 as poa, 'table_id_src'::text as table_id_src
from medicare_enc_texas.ip_base_enc
where 0 = 1
distributed by(bene_id);

--Get non-null diagnosis codes from facility tables
do $$
declare 
	--month_counter integer := 1;
	i int;
	table_names text[]:= array['ip', 'hha', 'snf', 'op'];
	table_name text;

begin
--loops through each table
	for table_name in select unnest(table_names) loop
		raise notice 'loading from % starting at %', table_name, now();
		--loops through positions 1-25
		for i in 1..25 loop
			if i % 5 = 0 then raise notice '     pos: %', i; end if; --raise notice every 5 positions
			--inpatient has POA flags
			if table_name = 'ip' then 
				execute 'insert into dw_staging.mcet_diag_etl
					select enc_join_key, bene_id, clm_from_dt::date, ' || i || ' as pos,
					 icd_dgns_cd' || i || ', null as dx_version, clm_poa_ind_sw' || i || ' ,
					''' || case when table_name = 'ip' then 'inpatient' when table_name = 'op' then 'outpatient' else table_name end || ''' as table_id_src
					from medicare_enc_texas.' || table_name || '_base_enc
					where icd_dgns_cd' || i || ' is not null and
					icd_dgns_cd' || i || ' != '''';';
			--other facility tables do not have POA flags
			else
				execute 'insert into dw_staging.mcet_diag_etl
					select enc_join_key, bene_id, clm_from_dt::date, ' || i || ' as pos,
					 icd_dgns_cd' || i || ', null as dx_version, null as poa,
					''' || case when table_name = 'ip' then 'inpatient' when table_name = 'op' then 'outpatient' else table_name end || ''' as table_id_src
					from medicare_enc_texas.' || table_name || '_base_enc
					where icd_dgns_cd' || i || ' is not null and
					icd_dgns_cd' || i || ' != '''';';
			end if;
		end loop;
	end loop;
end $$;

--get non-null diagnosis codes from bcarrier and dme tables
do $$
declare 
	--month_counter integer := 1;
	i int;
	table_names text[]:= array['carrier', 'dme'];
	table_name text;

begin
--loops through each table
	for table_name in select unnest(table_names) loop
		raise notice 'loading from % starting at %', table_name, now();
		for i in 1..13 loop
			if i % 4 = 0 then raise notice '     pos: %', i; end if; --raise notice every 4 positions
			execute 'insert into dw_staging.mcet_diag_etl
				select enc_join_key, bene_id, clm_from_dt::date, ' || i || ' as pos,
				 icd_dgns_cd' || i || ', icd_dgns_vrsn_cd' || i || ' as dx_version, null as poa,
				''' || table_name || ''' as table_id_src
				from medicare_enc_texas.' || table_name || '_base_enc
				where icd_dgns_cd' || i || ' is not null and
				icd_dgns_cd' || i || ' != '''';';
		end loop;
	end loop;
end $$;

vacuum analyze dw_staging.mcet_diag_etl;
--select * from dw_staging.mcet_diag_etl;
--select count(*) from dw_staging.mcet_diag_etl;
--1683751849

/*******************************
 * Load into claim_diag table
 */

drop table if exists dw_staging.mcet_claim_diag;

create table dw_staging.mcet_claim_diag 
(like data_warehouse.claim_diag including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);

insert into dw_staging.mcet_claim_diag(
     data_source, uth_member_id, uth_claim_id, from_date_of_service, diag_cd, diag_position, 
     poa_src, icd_version, load_date, year, fiscal_year, claim_id_src, member_id_src, table_id_src
)
select
    'mcet' as data_source, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    a.clm_from_dt as from_date_of_service, 
    a.dx as diag_cd, 
    pos as diag_position, 
    a.poa as poa_src, 
    dx_version as icd_version, 
    current_date as load_date, 
    extract(year from a.clm_from_dt) as year, 
    get_fy_from_date(a.clm_from_dt) as fiscal_year, 
    a.enc_join_key as claim_id_src, 
    a.bene_id as member_id_src, 
    table_id_src as table_id_src
from dw_staging.mcet_diag_etl a
left join data_warehouse.dim_uth_claim_id b
    on b.data_source = 'mcet'
    and a.enc_join_key = b.claim_id_src
    and a.bene_id = b.member_id_src;



vacuum analyze dw_staging.mcet_claim_diag;

--select * from dw_staging.mcet_claim_diag;

--drop temp table
drop table if exists dw_staging.mcet_diag_etl;

select 'mcet claim diag script finished at ' || current_timestamp as message;


