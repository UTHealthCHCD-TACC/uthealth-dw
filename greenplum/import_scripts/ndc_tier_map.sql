-- reference_tables.ndc_tier_map_imp2 definition

-- Drop table

-- DROP TABLE reference_tables.ndc_tier_map;

CREATE TABLE reference_tables.ndc_tier_map (
	ndc_code text NULL,
	main_multum_drug_code int4 NULL,
	drug_id text NULL,
	drg_id text NULL,
	drug_name text NULL,
	tier_1_category text NULL,
	tier_1_category_id int4 NULL,
	tier_2_category text NULL,
	tier_2_category_id int4 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=none
)
DISTRIBUTED RANDOMLY;

drop external table ext_ndc_tier_map;
CREATE EXTERNAL TABLE ext_ndc_tier_map (
ndc_code text, main_multum_drug_code int, drug_id text, drg_id text, drug_name text, 
tier_1_category text, tier_1_category_id int, tier_2_category text, tier_2_category_id int

) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/ndc_tier_map.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

insert into reference_tables.ndc_tier_map
select *
from ext_ndc_tier_map;


select count(*)
from reference_tables.ndc_tier_map;