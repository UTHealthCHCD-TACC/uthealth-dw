--Medical
drop table medicaid.chip_rx;
create table medicaid.chip_rx (
year_fy smallint, file varchar, 
PCN varchar, phmcy_nbr varchar, rx_nbr varchar, seq_nbr varchar, rx_dt date, auth_refill varchar, prescriber_nbr varchar, rx_fill_dt date,
ndc varchar, claim_status varchar, rx_quantity numeric, rx_days_supply numeric, client_location varchar, refill_nbr numeric,
amount_paid numeric, payment_dt date, unlimited_flag varchar, client_county varchar, phmcy_region varchar,
DISP_EXP_AMT numeric, drug_cost numeric, gcn_seq_nbr varchar, gross_amt_due numeric, hmo_plan_id varchar, client_dob date, client_sex varchar,
NPI varchar, prescriber_npi varchar, TCN varchar, PREV_TCN varchar, qty_prescribed numeric, unit_of_meas varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (pcn);

drop external table ext_chip_rx;
CREATE EXTERNAL TABLE ext_chip_rx (
year_fy smallint, filename varchar,
PCN varchar, phmcy_nbr varchar, rx_nbr varchar, seq_nbr varchar, rx_dt date, auth_refill varchar, prescriber_nbr varchar, rx_fill_dt date,
ndc varchar, claim_status varchar, rx_quantity numeric, rx_days_supply numeric, client_location varchar, refill_nbr numeric,
amount_paid numeric, payment_dt date, unlimited_flag varchar, client_county varchar, phmcy_region varchar,
DISP_EXP_AMT numeric, drug_cost numeric, gcn_seq_nbr varchar, gross_amt_due numeric, hmo_plan_id varchar, client_dob date, client_sex varchar,
NPI varchar, prescriber_npi varchar, TCN varchar, PREV_TCN varchar, qty_prescribed numeric, unit_of_meas varchar) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/load/*/CHIP_RX_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_chip_rx
limit 10;
*/
-- Insert
insert into medicaid.chip_rx
select * from ext_chip_rx;

-- 318 secs
update medicaid.chip_rx set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.chip_rx;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.chip_rx;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.chip_rx
group by 1, 2
order by 1, 2;
