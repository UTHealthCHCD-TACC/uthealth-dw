/*******************************************************************
 * Update derived cost for Optum
 * 
 * Optum tables come with standardized cost (std_cost) and year of standardized cost (std_cost_yr)
 * std_cost is an estimate of the allowed amount and depends on the year of standardization.
 * 
 * To obtain the derived cost, we multiply the standardized cost by its cost_factor,
 * which is obtained from reference_tables.ref_optum_cost_factor and depends on
 * year and service type.
 * 
 * Medical tables contain a mix of service types, whereas Confinement tables are strictly
 * inpatient facility (FAC_IP) charges and rx tables are strictly pharmacy charges.
 * 
 *******************************************************************/

/************************
 * 
 *      OPTUM ZIP
 * 
 ************************/

/************************
 * Update optum_zip.medical
 ************************/

--set the service type
update optum_zip.medical
	set service_type = case
	when tos_cd like 'ANC%' then 'ANC'
	when tos_cd like 'FAC_IP%' then 'FAC_IP'
	when tos_cd like 'FAC_OP%' then 'FAC_OP'
	when tos_cd like 'PROF%' then 'PROF'
else null end;

--calculate derived cost, add year
update optum_zip.medical a
	set derv_cost = round((a.std_cost * b.cost_factor)::numeric, 2),
	derv_cost_yr = 2021
from reference_tables.ref_optum_cost_factor b
where a.std_cost_yr::int = b.standard_price_year and a.service_type = b.service_type;

vacuum analyze optum_zip.medical;

/************************
 * Update optum_zip.confinement
 ************************/

/* Check: is everything here FAC_IP?
 * Ans: YES
select * from optum_zip.confinement
where tos_cd not like 'FAC_IP%';
 */

--calculate derived cost, add year
update optum_zip.confinement a
	set derv_cost = round((a.std_cost * b.cost_factor)::numeric, 2),
	derv_cost_yr = 2021
from reference_tables.ref_optum_cost_factor b
where a.std_cost_yr::int = b.standard_price_year and b.service_type = 'FAC_IP';

vacuum analyze optum_zip.confinement;

/************************
 * Update optum_zip.rx
 ************************/

--calculate derived cost, add year
update optum_zip.rx a
	set derv_cost = round((a.std_cost * b.cost_factor)::numeric, 2),
	derv_cost_yr = 2021
from reference_tables.ref_optum_cost_factor b
where a.std_cost_yr::int = b.standard_price_year and b.service_type = 'PHARM';

vacuum analyze optum_zip.rx;

/************************
 * 
 *      OPTUM DOD
 * 
 ************************/

/************************
 * Update optum_dod.medical
 ************************/

--set the service type
update optum_dod.medical
	set service_type = case
	when tos_cd like 'ANC%' then 'ANC'
	when tos_cd like 'FAC_IP%' then 'FAC_IP'
	when tos_cd like 'FAC_OP%' then 'FAC_OP'
	when tos_cd like 'PROF%' then 'PROF'
else null end;

--calculate derived cost, add year
update optum_dod.medical a
	set derv_cost = round((a.std_cost * b.cost_factor)::numeric, 2),
	derv_cost_yr = 2021
from reference_tables.ref_optum_cost_factor b
where a.std_cost_yr::int = b.standard_price_year and a.service_type = b.service_type;

vacuum analyze optum_dod.medical;

/************************
 * Update optum_dod.confinement
 ************************/

/* Check: is everything here FAC_IP?
 * Ans: YES
select * from optum_dod.confinement
where tos_cd not like 'FAC_IP%';
 */

--calculate derived cost, add year
update optum_dod.confinement a
	set derv_cost = round((a.std_cost * b.cost_factor)::numeric, 2),
	derv_cost_yr = 2021
from reference_tables.ref_optum_cost_factor b
where a.std_cost_yr::int = b.standard_price_year and b.service_type = 'FAC_IP';

vacuum analyze optum_dod.confinement;

/************************
 * Update optum_dod.rx
 ************************/

--calculate derived cost, add year
update optum_dod.rx a
	set derv_cost = round((a.std_cost * b.cost_factor)::numeric, 2),
	derv_cost_yr = 2021
from reference_tables.ref_optum_cost_factor b
where a.std_cost_yr::int = b.standard_price_year and b.service_type = 'PHARM';

vacuum analyze optum_dod.rx;

/***********************
 * Update the update_log
 ***********************/
--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'derv_cost updated',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name like 'optum_%' and table_name in ('medical', 'confinement', 'rx');

select * from data_warehouse.update_log;


