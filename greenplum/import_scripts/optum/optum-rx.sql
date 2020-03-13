--Medical
drop table optum_dod_refresh.rx;
create table optum_dod_refresh.rx (
year smallint, patid int8, pat_planid int8,	ahfsclss bpchar(8),	avgwhlsl numeric,	brnd_nm bpchar(30),	charge numeric,	chk_dt date, clmid bpchar(19),
	copay numeric, daw bpchar(1),days_sup int2,	dea bpchar(9),	deduct numeric(8,2),	dispfee numeric,fill_dt date,form_ind bpchar(1),form_typ bpchar(2),
	fst_fill bpchar(1),gnrc_ind bpchar(1),gnrc_nm bpchar(50),mail_ind bpchar(1),ndc bpchar(11),npi bpchar(10),pharm bpchar(10),prc_typ bpchar(1),quantity numeric,
	rfl_nbr bpchar(2),spclt_ind bpchar(1),specclss bpchar(3),std_cost numeric,std_cost_yr int2,strength bpchar(10),extract_ym int4,version numeric,
	PRESCRIBER_PROV text, PRESCRIPT_ID text
) 
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed randomly;

drop external table ext_rx;
CREATE EXTERNAL TABLE ext_rx (
patid int8, pat_planid int8,ahfsclss bpchar(8),	avgwhlsl numeric,	brnd_nm bpchar(30),	charge numeric,	chk_dt date, clmid bpchar(19),
	copay numeric, daw bpchar(1),days_sup int2,	dea bpchar(9),	deduct numeric(8,2),	dispfee numeric,fill_dt date,form_ind bpchar(1),form_typ bpchar(2),
	fst_fill bpchar(1),gnrc_ind bpchar(1),gnrc_nm bpchar(50),mail_ind bpchar(1),ndc bpchar(11),npi bpchar(10),pharm bpchar(10),prc_typ bpchar(1),quantity numeric,
	rfl_nbr bpchar(2),spclt_ind bpchar(1),specclss bpchar(3),std_cost numeric,std_cost_yr int2,strength bpchar(10),extract_ym int4,version numeric,
	PRESCRIBER_PROV text, PRESCRIPT_ID text
) 
LOCATION ( 
'gpfdist://c252-136:8081//*_r2*.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_rx
limit 1000;
*/
-- Insert - 47 min
insert into optum_dod_refresh.rx
select 0, * from ext_rx;

-- Set Year/Quarter
select date_part('year', fill_dt), fill_dt
from optum_dod_refresh.rx
limit 10;

-- 318 secs
update optum_dod_refresh.rx set year=date_part('year', fill_dt);

-- Analyze
analyze optum_dod.rx;

-- Year & Quarter
select distinct extract(quarter from fill_dt)
from ext_rx;

--Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod_refresh.rx;


select year, count(*), min(fill_dt), max(fill_dt)
from optum_dod_refresh.rx
group by 1
order by 1;
