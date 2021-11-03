

------------------------------------------------------------------------------------------
--TRUVEN
------------------------------------------------------------------------------------------

-------ccaeo
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
'truv' as data_source,
a.provid as provider_id_src,
null as provider_id_src_2,
a.npi as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
a.stdprov as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaeo a
left join dev.provider c 
			on c.provider_id_src = a.provid::text 
			and c.npi = a.npi
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.provid is not null
;




-------ccaef
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
'truv' as data_source,
a.provid as provider_id_src,
null as provider_id_src_2,
a.npi as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
a.stdprov as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaef a
left join dev.provider c 
			on c.provider_id_src = a.provid::text 
			and c.npi = a.npi
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.provid is not null
;




-------ccaes
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
'truv' as data_source,
a.provid as provider_id_src,
null as provider_id_src_2,
a.npi as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
a.stdprov as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaes a
left join dev.provider c 
			on c.provider_id_src = a.provid::text 
			and c.npi = a.npi
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.provid is not null
;


-------ccaei ( no npi)
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
'truv' as data_source,
a.physid as provider_id_src,
null as provider_id_src_2,
null as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
null as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaei a
left join dev.provider c 
			on c.provider_id_src = a.physid::text 
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.physid is not null
;
