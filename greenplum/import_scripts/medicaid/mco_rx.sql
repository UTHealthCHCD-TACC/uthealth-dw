--Medical
drop table medicaid.mco_rx;
create table medicaid.mco_rx (
year_fy smallint, file varchar, 
PCN varchar,phmcy_nbr varchar,rx_nbr varchar,seq_nbr varchar,rx_dt date,
auth_refill varchar,prescriber_nbr varchar,rx_fill_dt date,ndc varchar,
claim_status varchar,rx_quantity numeric,rx_days_supply numeric,client_location varchar,
refill_nbr varchar,amount_paid varchar,payment_dt date,unlimited_flag varchar,
client_county varchar,phmcy_region varchar,DISP_EXP_AMT numeric,drug_cost numeric,
gcn_seq_nbr varchar,gross_amt_due numeric,hmo_plan_id varchar,client_dob varchar,client_sex varchar,
NPI varchar,sig varchar,cat varchar,med_cov varchar,tp varchar,sd varchar,bp varchar,
prescriber_npi varchar,TCN varchar,PREV_TCN varchar,qty_prescribed numeric,unit_of_meas varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (pcn);

alter table medicaid.mco_rx add column DAW_FLAG varchar;
alter table medicaid.mco_rx add column PREF_PROD_FLAG varchar;

drop external table ext_mco_rx;
CREATE EXTERNAL TABLE ext_mco_rx (
year_fy smallint, filename varchar,
PCN varchar,phmcy_nbr varchar,rx_nbr varchar,seq_nbr varchar,rx_dt date,
auth_refill varchar,prescriber_nbr varchar,rx_fill_dt date,ndc varchar,
claim_status varchar,rx_quantity numeric,rx_days_supply numeric,client_location varchar,
refill_nbr varchar,amount_paid varchar,payment_dt date,unlimited_flag varchar,
client_county varchar,phmcy_region varchar,DISP_EXP_AMT numeric,drug_cost numeric,
gcn_seq_nbr varchar,gross_amt_due numeric,hmo_plan_id varchar,client_dob varchar,client_sex varchar,
NPI varchar,sig varchar,cat varchar,med_cov varchar,tp varchar,sd varchar,bp varchar,
prescriber_npi varchar,TCN varchar,PREV_TCN varchar,qty_prescribed numeric,unit_of_meas varchar
--2020
, DAW_FLAG varchar, PREF_PROD_FLAG varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/2020/MCO_RX_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_mco_rx
limit 10;
*/
-- Insert
insert into medicaid.mco_rx
select * from ext_mco_rx;

-- 318 secs
update medicaid.mco_rx set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.mco_rx;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.mco_rx;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.mco_rx
group by 1, 2
order by 1, 2;
