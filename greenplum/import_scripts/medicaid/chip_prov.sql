--Medical
drop table medicaid.chip_prov;
create table medicaid.chip_prov (
year_fy smallint, file varchar, 
Address varchar,City varchar,CntyCode varchar,CurrentMonth varchar,
EffectiveDate varchar,FName varchar,LName varchar,LicsNo varchar,NPI varchar,
PrimaryTaxonomy varchar,ProviderType varchar,SPCCode varchar,State varchar,TaxID varchar,ZIP varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (npi);

drop external table ext_chip_prov;
CREATE EXTERNAL TABLE ext_chip_prov (
year_fy smallint, filename varchar,
Address varchar,City varchar,CntyCode varchar,CurrentMonth varchar,
EffectiveDate varchar,FName varchar,LName varchar,LicsNo varchar,NPI varchar,
PrimaryTaxonomy varchar,ProviderType varchar,SPCCode varchar,State varchar,TaxID varchar,ZIP varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/load/*/*_CHIP_PROV_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_chip_prov
limit 10;
*/
-- Insert
insert into medicaid.chip_prov
select * from ext_chip_prov;

-- 318 secs
update medicaid.chip_prov set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.chip_prov;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.chip_prov;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.chip_prov
group by 1, 2
order by 1, 2;
