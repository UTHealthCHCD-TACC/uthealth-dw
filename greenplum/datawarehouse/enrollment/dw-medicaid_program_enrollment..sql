/* ******************************************************************************************************
 *  creates and loads table to track medicaid program enrollment month to month
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 8/16/2021 || script created 
 * ******************************************************************************************************
 *  jw001  || 11/11/2021 || wrapped in function
 * ******************************************************************************************************
 */

CREATE OR REPLACE FUNCTION public.medicaid_enrollment()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

BEGIN 

--- create table in dw_staging
	
	raise notice 'building medicaid_program_enrollment';

create table dw_staging.medicaid_program_enrollment 
(like data_warehouse.medicaid_program_enrollment including all) 
;

raise notice 'medicaid_program_enrollment created';


raise notice 'mdcd enrl load start';     


--chip
insert into dw_staging.medicaid_program_enrollment (year_fy, uth_member_id, elig_date, mco_program_nm)
select a.year_fy, b.uth_member_id, a.elig_month, c.mco_program_nm 
from medicaid.chip_uth a
  join data_warehouse.dim_uth_member_id b 
     on b.member_id_src = a.client_nbr
  join reference_tables.medicaid_lu_contract c 
     on trim(a.plan_cd) = trim(c.plan_cd);


raise notice 'mdcd chip load complete';     

raise notice 'mdcd enrl load start';      
   
insert into dw_staging.medicaid_program_enrollment (
year_fy,
uth_member_id,
elig_date,
mco_program_nm,
sig,
smib,
base_plan,
mc_flag,
mc_sc,
me_cat,
me_code,
me_sd,
provider_id,
mco_id,
riskgrp_id,
cmp_rg_id,
perm_excl,
count_excl,
pure_rate
)
select 
a.year_fy,
b.uth_member_id,
a.elig_date,
c.mco_program_nm,
a.sig,
a.smib,
a.base_plan,
a.mc_flag,
a.mc_sc,
a.me_cat,
a.me_code,
a.me_sd,
a.provider_id,
a.mco_id,
a.riskgrp_id,
a.cmp_rg_id,
a.perm_excl,
a.count_excl,
a.pure_rate
from medicaid.enrl a
  join data_warehouse.dim_uth_member_id b 
     on b.member_id_src = a.client_nbr
  join reference_tables.medicaid_lu_contract c 
     on trim(a.base_plan) = trim(c.plan_cd);
    
raise notice 'mdcd enrl load complete';    
raise notice 'transferring ownership';
   
alter function public.medicaid_enrollment() owner to uthealth_dev;
grant all on function public.medicaid_enrollment() to uthealth_dev;

raise notice 'ownership transferred to uthealth_dev';

END 

$$
EXECUTE ON ANY;
