/* ******************************************************************************************************
 * Loads dw_staging.claim_icd_proc with medicaid data
 * ******************************************************************************************************
 *  Author 		|| Date      || Notes
 * ******************************************************************************************************
 *  wc001  		|| 1/01/2021 || script created 
 * ------------------------------------------------------------------------------------------------------
 *  wallingTACC || 8/23/2021 || updated comments.
 * ------------------------------------------------------------------------------------------------------
 *  wcough	    || 1/07/2022 || add icd_version back to table
 * ------------------------------------------------------------------------------------------------------
 *  xiaorui		|| 07/13/2023 || modified code to get rid of leading zeroes on ICD 9 proc codes
 * 								 and also made it into a loop for easier troubleshooting
 * ------------------------------------------------------------------------------------------------------
 *  xiaorui		|| 09/05/2023 || changed mcd_icd_proc to mcd_icd_proc to be consistent
 * 								 and changed distribution key to match data_warehouse (uth_member_id)
 * 								 so that we can swap partitions
 * */

--create empty etl table
drop table if exists dw_staging.mcd_icd_proc_etl;

create table dw_staging.mcd_icd_proc_etl (
	year int,
	fy int2,
	my int4,
	icn varchar(50),
	pcn varchar(20),
	from_dos date,
	proc_cd varchar(20),
	pos int,
	icd_version varchar(8),
	proc_cd_trimmed varchar(20),
	src_table varchar(50)
) distributed by (pcn);

/***************************************
Load icd proc codes from clm tables
***************************************/

do $$
declare
	i int;
begin
	for i in 1..25
	loop
		execute 'insert into dw_staging.mcd_icd_proc_etl
				select extract(year from b.hdr_frm_dos::date) as year,
					b.year_fy,
					get_my_from_date(b.hdr_frm_dos::date) as my,
					a.icn,
					a.pcn,
				    b.hdr_frm_dos::date as from_dos, 
				    trim(a.proc_icd_cd_' || i || ') as proc_cd,
				    ' || i || ' as proc_icd_pos,
				    trim(a.proc_icd_qal_' || i || ') as icd_version,
					null as proc_cd_trimmed,
					''clm_proc'' as src_table
				from medicaid.clm_proc a join medicaid.clm_header b
					on b.icn = a.icn
				where trim(a.proc_icd_cd_' || i || ') != ''''';
		raise notice 'Inserting clm proc_cd_ %', i;
	end loop;
end $$;


--select * from dw_staging.mcd_icd_proc_etl where icd_version = '9';
analyze dw_staging.mcd_icd_proc_etl;

/***************************************
Load icd proc codes from encounter tables
***************************************/

--insert primary procedure code by itself
insert into dw_staging.mcd_icd_proc_etl
select extract(year from b.frm_dos::date) as year,
	b.year_fy,
	get_my_from_date(b.frm_dos::date) as my,
	a.derv_enc,
	a.mem_id,
    b.frm_dos::date as from_dos, 
    trim(a.prim_proc_cd) as proc_cd,
    1 as proc_icd_pos,
    trim(a.prim_proc_qal) as icd_version,
	null as proc_cd_trimmed,
	'enc_proc' as src_table
from medicaid.enc_proc a join medicaid.enc_header b 
	on b.derv_enc = a.derv_enc
where trim(a.prim_proc_cd) != '';

--then positions 1-24 (not 25)
do $$
declare
	i int;
begin
	for i in 1..24
	loop
		execute 'insert into dw_staging.mcd_icd_proc_etl
				select extract(year from b.frm_dos::date) as year,
					b.year_fy,
					get_my_from_date(b.frm_dos::date) as my,
					a.derv_enc,
					a.mem_id,
				    b.frm_dos::date as from_dos, 
				    trim(a.proc_icd_cd_' || i || ') as proc_cd,
				    ' || i + 1 || ' as proc_icd_pos,
				    trim(a.proc_icd_qal_' || i || ') as icd_version,
					null as proc_cd_trimmed,
					''enc_proc'' as src_table
				from medicaid.enc_proc a join medicaid.enc_header b 
					on b.derv_enc = a.derv_enc
				where trim(a.proc_icd_cd_' || i || ') != ''''';
		raise notice 'Inserting encounter proc_cd_ %', i;
	end loop;
end $$;

--select * from dw_staging.mcd_icd_proc_etl where icd_version = '9';
analyze dw_staging.mcd_icd_proc_etl;


/***************************************
Load icd proc codes from HTW table
***************************************/
do $$
declare
	i int;
begin
	for i in 1..25
	loop
		execute 'insert into dw_staging.mcd_icd_proc_etl
				select extract(year from b.hdr_frm_dos::date) as year,
					get_fy_from_date(b.hdr_frm_dos::date) as year_fy,
					get_my_from_date(b.hdr_frm_dos::date) as my,
					a.icn,
					a.pcn,
				    b.hdr_frm_dos::date as from_dos, 
				    trim(a.proc_icd_cd_' || i || ') as proc_cd,
				    ' || i || ' as proc_icd_pos,
				    trim(a.proc_icd_qal_' || i || ') as icd_version,
					null as proc_cd_trimmed,
					''htw_clm_proc'' as src_table
				from medicaid.htw_clm_proc a join medicaid.htw_clm_header b
					on b.icn = a.icn
				where trim(a.proc_icd_cd_' || i || ') != ''''';
		raise notice 'Inserting htw proc_cd_ %', i;
	end loop;
end $$;

analyze dw_staging.mcd_icd_proc_etl;
--select * from dw_staging.mcd_icd_proc_etl where src_table = 'htw_clm_proc';

/*******************************
 * Get just the distinct rows
 */

--select count(*) from dw_staging.mcd_icd_proc_etl;
--7557655
/*

drop table if exists dw_staging.mcd_icd_proc_etl2;

create table dw_staging.mcd_icd_proc_etl2 as
select distinct * from dw_staging.mcd_icd_proc_etl;
--Updated Rows	7557655

--no need for this code block, they are all distinct
*/

/*******************************
 * Clean table
 * 		remove leading 0 from when icd_ver = '9'
 *******************************/

--change ICD 9 proc codes to be either 3 or 4 digits
update dw_staging.mcd_icd_proc_etl
set proc_cd_trimmed = case when length(ltrim(proc_cd, '0')) < 3 then right(proc_cd, 3)
		else ltrim(proc_cd, '0') end
where icd_version = '9';

/* not needed - no rows updated
update dw_staging.mcd_icd_proc_etl
set icd_version = null where icd_version not in ('0', '9');
*/

/*check out some anomalies

select * from dw_staging.mcd_icd_proc_etl
where icd_version = '9' and length(proc_cd_trimmed) not between 3 and 5;
 */

/*********************************
 * Build final table
 * 
 * 'mdcd' is hard-coded as the data source - this will be fixed in post
 * ********************************/

drop table if exists dw_staging.mcd_claim_icd_proc;

create table dw_staging.mcd_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults)
with (
	appendonly = true,
	orientation = row,
	compresstype = zlib,
	compresslevel = 5)
distributed by (uth_member_id);

--select * from dw_staging.mcd_claim_icd_proc;

insert into dw_staging.mcd_claim_icd_proc (
	data_source, uth_member_id, uth_claim_id, from_date_of_service, proc_cd, proc_position, icd_version,
	load_date, year, fiscal_year, claim_id_src, member_id_src, table_id_src
) 
select 'mdcd' as data_source,
	b.uth_member_id as uth_member_id,
	c.uth_claim_id as uth_claim_id,
	a.from_dos as from_date_of_service,
	coalesce(a.proc_cd_trimmed, a.proc_cd) as proc_cd,
	a.pos as proc_position,
	a.icd_version as icd_version,
	current_date as load_date,
	a."year" as "year",
	a.fy as fiscal_year,
	a.icn as claim_id_src,
	a.pcn as member_id_src,
	a.src_table as table_id_src
from dw_staging.mcd_icd_proc_etl a
left join data_warehouse.dim_uth_member_id b on a.pcn = b.member_id_src and b.data_source = 'mdcd'
left join data_warehouse.dim_uth_claim_id c on a.icn = c.claim_id_src and b.data_source = 'mdcd'
;
/*
some light QA
select * from dw_staging.mcd_claim_icd_proc where table_id_src = 'htw_clm_proc';
select * from dw_staging.mcd_claim_icd_proc where data_source = 'mcpp' and table_id_src = 'enc_proc';
select count(*) from dw_staging.mcd_icd_proc_etl; --15848444
select count(*) from dw_staging.mcd_claim_icd_proc; --15848444

select length(proc_cd), count(*) from dw_staging.mcd_claim_icd_proc
where icd_version = '9'
group by length(proc_cd) order by length(proc_cd);

*/

vacuum analyze dw_staging.mcd_claim_icd_proc;




