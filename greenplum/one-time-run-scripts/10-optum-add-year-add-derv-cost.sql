/****************************************************************************
 * This script adds the following columns to the raw Optum tables
 * for both optum_zip and optum_dod
 * 
 * medical, confinement, rx:
 * 		add service_type (derived from TOS_CD)
 * 		add derv_cost (derived from std_cost, based on ref_optum_cost_factor)
 * 		add derv_cost_yr (based on ref_optum_cost_factor)
 * 
 ******************************************************************************/

/************************
 * 
 *      OPTUM ZIP
 * 
 ************************/

--add columns to medical table
alter table optum_zip.medical
add column service_type text,
add column derv_cost numeric,
add column derv_cost_yr int2;

--add columns to confinement table
alter table optum_zip.confinement
add column derv_cost numeric,
add column derv_cost_yr int2;

--add columns to rx table
alter table optum_zip.rx
add column derv_cost numeric,
add column derv_cost_yr int2;

/************************
 * 
 *      OPTUM DOD
 * 
 ************************/

--add columns to medical table
alter table optum_dod.medical
add column service_type text,
add column derv_cost numeric,
add column derv_cost_yr int2;

--add columns to confinement table
alter table optum_dod.confinement
add column service_type text, --do we want service type here?
add column derv_cost numeric,
add column derv_cost_yr int2;

--add columns to rx table
alter table optum_dod.rx
add column derv_cost numeric,
add column derv_cost_yr int2;




