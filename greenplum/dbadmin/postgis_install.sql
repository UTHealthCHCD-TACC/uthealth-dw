CREATE extension postgis SCHEMA gis;

CREATE extension postgis_tiger_geocoder;


--Setup tiger

SELECT pprint_addy(normalize_address('202 East Fremont Street, Las Vegas, Nevada 89101')) As pretty_address;

-- Tiger Data
update tiger.loader_lookuptables
set load = 'true'
where lookup_name = 'zcta5_raw';

SELECT loader_generate_nation_script('sh'); 

select *
from tiger_data.county_all;

--Permissions
grant usage on schema gis to group uthealth_analyst; 
grant select on all tables in schema gis to group uthealth_analyst; 
grant select on all sequences in schema gis to uthealth_analyst;

grant usage on schema tiger to group uthealth_analyst; 
grant select on all tables in schema tiger to group uthealth_analyst; 
grant select on all sequences in schema tiger to uthealth_analyst;

grant usage on schema tiger_data to group uthealth_analyst; 
grant select on all tables in schema tiger_data to group uthealth_analyst; 
grant select on all sequences in schema tiger_data to uthealth_analyst;