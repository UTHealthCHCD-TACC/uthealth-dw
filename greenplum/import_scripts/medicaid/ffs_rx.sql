--Medical
drop table medicaid.ffs_rx;
create table medicaid.ffs_rx (
year_fy smallint, file varchar, 
PCN varchar,phmcy_nbr varchar,rx_nbr varchar,seq_nbr varchar,rx_dt date,
auth_refill varchar,prescriber_nbr varchar,rx_fill_dt date,ndc varchar,
claim_status varchar,rx_quantity varchar,rx_days_supply numeric,client_location varchar,
refill_nbr numeric,amount_paid numeric,payment_dt date,unlimited_flag varchar,
client_county varchar,phmcy_region varchar,DISP_EXP_AMT numeric,drug_cost numeric,
gcn_seq_nbr varchar,gross_amt_due numeric,hmo_plan_id varchar,client_dob varchar,client_sex varchar,
npi varchar,sig varchar,cat varchar,med_cov varchar,tp varchar,sd varchar,bp varchar,
prescriber_npi varchar,TCN varchar,PREV_TCN varchar,qty_prescribed varchar,unit_of_meas varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (PCN);

drop external table ext_ffs_rx;
CREATE EXTERNAL TABLE ext_ffs_rx (
year_fy smallint, filename varchar,
PCN varchar,phmcy_nbr varchar,rx_nbr varchar,seq_nbr varchar,rx_dt date,
auth_refill varchar,prescriber_nbr varchar,rx_fill_dt date,ndc varchar,
claim_status varchar,rx_quantity varchar,rx_days_supply numeric,client_location varchar,
refill_nbr numeric,amount_paid numeric,payment_dt date,unlimited_flag varchar,
client_county varchar,phmcy_region varchar,DISP_EXP_AMT numeric,drug_cost numeric,
gcn_seq_nbr varchar,gross_amt_due numeric,hmo_plan_id varchar,client_dob varchar,client_sex varchar,
npi varchar,sig varchar,cat varchar,med_cov varchar,tp varchar,sd varchar,bp varchar,
prescriber_npi varchar,TCN varchar,PREV_TCN varchar,qty_prescribed varchar,unit_of_meas varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/*/FFS_RX_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_ffs_rx
limit 10;
*/
-- Insert
insert into medicaid.ffs_rx
select * from ext_ffs_rx;

-- 318 secs
update medicaid.ffs_rx set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.ffs_rx;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.ffs_rx;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.ffs_rx
group by 1, 2
order by 1, 2;
