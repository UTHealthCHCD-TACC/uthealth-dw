SELECT postgis_full_version();

CREATE extension postgis SCHEMA gis;
CREATE EXTENSION postgis_raster;
CREATE EXTENSION postgis_raster;

CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;
--this one is optional if you want to use the rules based standardizer (pagc_normalize_address)
CREATE EXTENSION address_standardizer;


--Setup tiger

SELECT na.address, na.streetname,na.streettypeabbrev, na.zip
	FROM normalize_address('1 Devonshire Place, Boston, MA 02109') AS na;

select *
from tiger.loader_platform;

INSERT INTO tiger.loader_platform(os, declare_sect, pgbin, wget, unzip_command, psql, path_sep,
		   loader, environ_set_command, county_process_command)
SELECT '', declare_sect, pgbin, wget, unzip_command, psql, path_sep,
	   loader, environ_set_command, county_process_command
  FROM tiger.loader_platform
  WHERE os = 'sh';
 
-- Tiger Data
update tiger.loader_lookuptables
set load = 'true'
where lookup_name = 'zcta5_raw';

SELECT loader_generate_nation_script('sh'); 

select *
from tiger_data.county_all;

--Test SQL
SELECT g.rating,
pprint_addy(g.addy),
ST_X(g.geomout)::numeric(8,5) AS lon,
ST_Y(g.geomout)::numeric(8,5) AS lat,
g.geomout,
(addy).address As num,
(addy).predirabbrev As pre,
(addy).streetname || ' ' || (addy).streettypeabbrev As street,
(addy).location As city,
(addy).stateabbrev As st
FROM geocode(normalize_address('2707 Rosedale Street,Houston,TX,77004'),1) AS g;

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