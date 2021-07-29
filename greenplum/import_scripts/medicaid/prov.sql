--Medical
drop table medicaid.prov;
create table medicaid.prov (
year_fy smallint, file varchar, 
TPI varchar,Base_TPI varchar,Prov_Name varchar,Phys_Address varchar,Phys_City varchar,Phys_State varchar,Phys_Zip varchar,Phys_County varchar,Phys_Phone varchar,
LBL_Address varchar,LBL_City varchar,LBL_State varchar,LBL_Zip varchar,Mail_County varchar,Mail_Phone varchar,Bad_Address_Ind varchar,Provider_Type varchar,
Specialty varchar,Sanction_Ind varchar,License_Nbr varchar,Tax_Nbr varchar,Enroll_Dt varchar,Subspecialty varchar,NPI varchar,Primary_Taxonomy varchar,Table_Update varchar,SFY varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (tpi);

drop external table ext_prov;
CREATE EXTERNAL TABLE ext_prov (
year_fy smallint, filename varchar,
TPI varchar,Base_TPI varchar,Prov_Name varchar,Phys_Address varchar,Phys_City varchar,Phys_State varchar,Phys_Zip varchar,Phys_County varchar,Phys_Phone varchar,
LBL_Address varchar,LBL_City varchar,LBL_State varchar,LBL_Zip varchar,Mail_County varchar,Mail_Phone varchar,Bad_Address_Ind varchar,Provider_Type varchar,
Specialty varchar,Sanction_Ind varchar,License_Nbr varchar,Tax_Nbr varchar,Enroll_Dt varchar,Subspecialty varchar,NPI varchar,Primary_Taxonomy varchar,Table_Update varchar,SFY varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/load/*/*_Prov_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_prov
limit 10;
*/
-- Insert
insert into medicaid.prov
select * from ext_prov;

-- 318 secs
update medicaid.prov set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.prov;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.prov;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.prov
group by 1, 2
order by 1, 2;
