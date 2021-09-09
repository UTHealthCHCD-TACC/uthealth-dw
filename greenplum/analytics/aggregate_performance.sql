drop table dev.total_charge_amount_percentile;
create table dev.total_charge_amount_percentile
with(appendonly=true, orientation=column)
as
select total_charge_amount
from data_warehouse.claim_header
where random() < 0.01;

select percentile_disc(.95) within group(order by total_charge_amount) 
from dev.total_charge_amount_percentile;

select percentile_disc(.95) within group(order by total_charge_amount) 
from data_warehouse.claim_header
where random() < 0.01;

