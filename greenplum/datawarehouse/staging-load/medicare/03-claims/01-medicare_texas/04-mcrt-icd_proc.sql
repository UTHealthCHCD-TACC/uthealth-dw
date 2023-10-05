
/* ******************************************************************************************************
 *  load claim icd proc for medicare texas
 * ******************************************************************************************************
 *  Author   || Date      || Notes
 * ****************************************************************************************************** 
 *  Xiaorui  || 10/03/23  || Rewrote -- ETL first then load
 * ******************************************************************************************************
 * */

/**************************
 * First make an ETL (to reduce grabbing blank proc code fields)
 * Note that only Inpatient, SNF, and Outpatient have ICD Proc codes
 **************************/
--initialize table
drop table if exists dw_staging.mcrt_proc_etl;

create table dw_staging.mcrt_proc_etl as
select clm_id, bene_id, prcdr_dt1::date as prcdr_dt, 0 as pos, icd_prcdr_cd1 as proc_cd,
	'table_id_src'::text as table_id_src
from medicare_texas.inpatient_base_claims_k
where 0 = 1
distributed by(bene_id);

--Get non-null proc codes from facility tables
do $$
declare 
	--month_counter integer := 1;
	i int;
	table_names text[]:= array['inpatient', 'snf', 'outpatient'];
	table_name text;
begin
--loops through each table
	for table_name in select unnest(table_names) loop
		raise notice 'loading from % starting at %', table_name, now();
		--loops through positions 1-25
		for i in 1..25 loop
			if i % 5 = 0 then raise notice '     pos: %', i; end if; --raise notice every 5 positions
			--inpatient has POA flags
			execute 'insert into dw_staging.mcrt_proc_etl
				select clm_id, bene_id, prcdr_dt' || i || '::date, ' || i || ' as pos,
				 icd_prcdr_cd' || i || ' as proc_cd,
				''' || table_name || ''' as table_id_src
				from medicare_texas.' || table_name || '_base_claims_k
				where icd_prcdr_cd' || i || ' is not null and
				icd_prcdr_cd' || i || ' != '''';';
		end loop;
	end loop;
end $$;

vacuum analyze dw_staging.mcrt_proc_etl;
--select * from dw_staging.mcrt_proc_etl;
--select count(*) from dw_staging.mcrt_proc_etl;
--9096292

/*******************************
 * Load into claim_icd_proc table
 */

drop table if exists dw_staging.mcrt_claim_icd_proc;

create table dw_staging.mcrt_claim_icd_proc 
(like data_warehouse.claim_icd_proc including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);

insert into dw_staging.mcrt_claim_icd_proc(
     data_source, uth_member_id, uth_claim_id, from_date_of_service, proc_cd, proc_position, 
     load_date, year, fiscal_year, claim_id_src, member_id_src, table_id_src
)
select
    'mcrt' as data_source, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    a.prcdr_dt as from_date_of_service, 
    a.proc_cd as proc_cd, 
    pos as proc_position, 
    current_date as load_date, 
    extract(year from a.prcdr_dt) as year, 
    get_fy_from_date(a.prcdr_dt) as fiscal_year, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    table_id_src as table_id_src
from dw_staging.mcrt_proc_etl a
left join data_warehouse.dim_uth_claim_id b
    on b.data_source = 'mcrt'
    and a.clm_id = b.claim_id_src
    and a.bene_id = b.member_id_src;

vacuum analyze dw_staging.mcrt_claim_icd_proc;

--select * from dw_staging.mcrt_claim_icd_proc;

--drop temp table
drop table if exists dw_staging.mcrt_proc_etl;

