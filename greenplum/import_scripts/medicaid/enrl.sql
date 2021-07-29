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
perm_excl varchar,count_excl varchar,pure_rate numeric,admin_rate numeric
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/load/*/ENRL_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );


