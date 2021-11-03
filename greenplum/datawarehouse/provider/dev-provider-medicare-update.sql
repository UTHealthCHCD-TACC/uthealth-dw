/*


--@@ -0,0 +1,132 @@
------------------------------------------------------------------------------------------
--Medicare
------------------------------------------------------------------------------------------
7/26/2021:      
       Leaving in non-numeric NPI's for now.... We may take out later, but not yet
       Not loading dme claims in
       Base claims are put in as line level '1' 
       Bcarrier base claims are put in at 0 and then line level for that claim starts at 1
------------------------------------------------------------------------------------------
*/

--------------------------------------------------------------------------
----------update provider table with reference table
-------------------------------------------------
update data_warehouse.provider
set taxonomy1 = b."Healthcare Provider Taxonomy Code_1" 
from reference_tables.npidata b
where provider.npi = b."NPI" 
;

update data_warehouse.provider
set taxonomy2 = b."Healthcare Provider Taxonomy Code_2" 
from reference_tables.npidata b
where provider.npi = b."NPI" 
;

update data_warehouse.provider
set spclty_cd2 = b."Healthcare Provider Taxonomy Code_2" 
from reference_tables.npidata b
where provider.npi = b."NPI" 
;

update data_warehouse.provider
set address1 = b."Provider First Line Business Practice Location Address"
from reference_tables.npidata b
where provider.npi = b."NPI" 
;

update data_warehouse.provider
set address2 = b."Provider Second Line Business Practice Location Address" 
from reference_tables.npidata b
where provider.npi = b."NPI" 
;

update data_warehouse.provider
set address3 = null
;

update data_warehouse.provider
set zip_3 = substring(zip,1,3)
;

update data_warehouse.provider
set zip_5 = substring(zip,1,5)
;

alter table data_warehouse.provider
add column zip_5 text;

alter table data_warehouse.provider
drop column address3;


update data_warehouse.provider
set zip_3 = substring(zip,1,3)
;

update data_warehouse.provider
set city = b."Provider Business Practice Location Address City Name" 
from reference_tables.npidata b
where provider.npi = b."NPI" 
;

select * from data_warehouse.provider where address1 is not null and address1 <> '';


select 
"Provider First Line Business Practice Location Address"
from reference_tables.npidata;

select 
"Provider Other Organization Name Type Code"
from reference_tables.npidata
group by "Provider Other Organization Name Type Code";



select uth_provider_id, data_source, state, npi,taxonomy1, taxonomy2, spclty_cd1, address1, address2, city, zip_5 
from data_warehouse.provider where taxonomy1 is not null and taxonomy2 is not null and taxonomy2 <> '';

update data_warehouse.provider
set taxonomy2 = null 
where taxonomy2 = 'N'
;













