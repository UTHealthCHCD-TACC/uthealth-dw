--Medical
drop table medicaid.enrl;
create table medicaid.enrl (
year smallint, file varchar, 
client_nbr varchar,dob varchar,race varchar,zip varchar,fam_size varchar,fam_income varchar,education varchar,
case_nbr varchar,sig varchar,smib_from_dt varchar,smib_to_dt varchar,smib varchar,base_plan varchar,
elig_date varchar,contract_id varchar,county_id varchar,tx_hold varchar,mc_flag varchar,mc_sc varchar,me_cat varchar,me_code varchar,me_tp varchar,me_sd varchar,
sex varchar,age varchar,provider_id varchar,mc_from_date varchar,mc_to_date varchar,mco_id varchar,riskgrp_id varchar,cmp_rg_id varchar,
perm_excl varchar,count_excl varchar,pure_rate numeric,admin_rate numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (client_nbr);

drop external table ext_enrl;
CREATE EXTERNAL TABLE ext_enrl (
year smallint, filename varchar,
client_nbr varchar,dob varchar,race varchar,zip varchar,fam_size varchar,fam_income varchar,education varchar,
case_nbr varchar,sig varchar,smib_from_dt varchar,smib_to_dt varchar,smib varchar,base_plan varchar,
elig_date varchar,contract_id varchar,county_id varchar,tx_hold varchar,mc_flag varchar,mc_sc varchar,me_cat varchar,me_code varchar,me_tp varchar,me_sd varchar,
sex varchar,age varchar,provider_id varchar,mc_from_date varchar,mc_to_date varchar,mco_id varchar,riskgrp_id varchar,cmp_rg_id varchar,
perm_excl varchar,count_excl varchar,pure_rate numeric,admin_rate numeric
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/*/ENRL_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_enrl
limit 10;
*/
-- Insert
insert into medicaid.enrl
select * from ext_enrl;

-- 318 secs
update medicaid.enrl set year=date_part('year', FST_DT) where year=0;


-- Analyze
analyze medicaid.enrl;
 
-- Verify
select count(*), min(year), max(year), count(distinct year) from medicaid.enrl;


select year, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.enrl
group by 1, 2
order by 1, 2;
