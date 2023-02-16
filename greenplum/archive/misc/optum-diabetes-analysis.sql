create table tableau.optum_rx_brandname_by_year_gender
as 
select brnd_nm, date_part('year', fill_dt) as year, m.gdr_cd, count(*) as cnt, avg(charge) as avg_cost, sum(charge) as sum_charge
from optum.prescription p
join optum.member m on p.patid=m.patid
group by 1, 2, 3;



select brnd_nm, avg(cnt), avg(avg_cost)
from tableau.optum_rx_brandname_by_year
group by 1
order by 2 desc;