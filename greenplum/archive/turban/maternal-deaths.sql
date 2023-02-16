drop table dev.maternal_death_codes;
CREATE TABLE dev.maternal_death_codes (
	icd_type varchar not NULL,
	code varchar not null,
	description varchar not null
)
DISTRIBUTED RANDOMLY;

alter table dev.maternal_death_codes add column code_clean varchar;

update dev.maternal_death_codes
set code_clean = replace(code, '.', '')

--Check for codes
select *
from reference_tables.cms_proc_codes cms
where code like '772%'
join dev.maternal_death_codes mdc on cms.code=mdc.code

select *
from reference_tables.hcpcs h 
where code like '%death%'

select *
from reference_tables.icd_9_diags icd9
join dev.maternal_death_codes mdc on icd9.code=mdc.code_clean;

select *
from reference_tables.icd_10_diags icd10
join dev.maternal_death_codes mdc on icd10.code=mdc.code_clean;

select *
from reference_tables.icd_9_diags icd9
where code like '7616'

select *
from reference_tables.icd_10_diags icd10
where code like 'O95%' or code like 'O96%' or code like 'O97%';
-- Create aggregate table
