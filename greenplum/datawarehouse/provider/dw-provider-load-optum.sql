
--create provider
drop table if exists dev.provider;

CREATE TABLE dev.provider (
	uth_provider_id bigserial NOT NULL,	
	data_source bpchar(4) NULL,
	provider_id_src text NOT null,
	provider_id_src_2 text null,
	npi varchar NULL,
	taxonomy1 varchar NULL,
	taxonomy2 varchar NULL,
	spclty_cd1 varchar NULL,
	spclty_cd2 varchar NULL,
	provcat varchar null,
	provider_type varchar NULL,
	address1 varchar NULL,
	address2 varchar NULL,
	address3 varchar NULL,
	city varchar NULL,
	state varchar NULL,
	zip varchar NULL,
	zip_5 varchar NULL
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (uth_provider_id);

------ provider optd
insert into dev.provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'optd' as data_source,
a.prov as provider_id_src,
b.prov_unique as provider_id_src_2,
a.npi as npi,
b.taxonomy1 as taxonomy1,
b.taxonomy2 as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
b.provcat as provcat,
b.prov_type as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
b.prov_state as state,
null as zip,
null as zip_5
from optum_dod.provider_bridge a
join optum_dod.provider b on a.prov_unique = b.prov_unique 
left join dev.provider c on c.provider_id_src = a.prov::text and data_source = 'optd'
where c.uth_provider_id is null 
and a.prov is not null
;



------------------------------------------------------------------------------------------
--Optum ZIP
------------------------------------------------------------------------------------------

insert into dev.provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'optz' as data_source,
a.prov as provider_id_src,
b.prov_unique as provider_id_src_2,
a.npi as npi,
b.taxonomy1 as taxonomy1,
b.taxonomy2 as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
b.provcat as provcat,
b.prov_type as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
b.prov_state as state,
null as zip,
null as zip_5
from optum_zip.provider_bridge a
join optum_zip.provider b on a.prov_unique = b.prov_unique 
left join dev.provider c on c.provider_id_src = a.prov::text and data_source = 'optz'
where c.uth_provider_id is null 
and a.prov is not null
;


select provid, npi, stdprov, phyflag 
from truven.ccaeo
where provid is not null;

