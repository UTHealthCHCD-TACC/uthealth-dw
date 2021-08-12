--Medical
drop table medicaid.enrl;
create table medicaid.enrl (
year_fy smallint, file varchar, 
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
year_fy smallint, filename varchar,
client_nbr varchar,dob varchar,race varchar,zip varchar,fam_size varchar,fam_income varchar,education varchar,
case_nbr varchar,sig varchar,smib_from_dt varchar,smib_to_dt varchar,smib varchar,base_plan varchar,
elig_date varchar,contract_id varchar,county_id varchar,tx_hold varchar,mc_flag varchar,mc_sc varchar,me_cat varchar,me_code varchar,me_tp varchar,me_sd varchar,
sex varchar,age varchar,provider_id varchar,mc_from_date varchar,mc_to_date varchar,mco_id varchar,riskgrp_id varchar,cmp_rg_id varchar,
perm_excl varchar,count_excl varchar,pure_rate varchar,admin_rate varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/2020/ENRL_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_enrl
limit 10;

insert into medicaid.enrl
select year_fy, filename,
client_nbr,dob,race,zip,fam_size,fam_income,education,
case_nbr,sig,smib_from_dt,smib_to_dt,smib,base_plan,
elig_date,contract_id,county_id,tx_hold,mc_flag,mc_sc,me_cat,me_code,me_tp,me_sd,
sex,age,provider_id,mc_from_date,mc_to_date,mco_id,riskgrp_id,cmp_rg_id,
perm_excl,count_excl, REPLACE(pure_rate,',','')::numeric ,REPLACE(admin_rate,',','')::numeric
from ext_enrl;

select replace('1,123.01', ',','')::numeric


