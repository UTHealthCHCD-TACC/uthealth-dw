


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

insert into data_warehouse.provider (
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
left join data_warehouse.provider  c on c.provider_id_src = a.prov::text and data_source = 'optz'
where c.uth_provider_id is null 
and a.prov is not null
;


select provid, npi, stdprov, phyflag 
from truven.ccaeo
where provid is not null;

