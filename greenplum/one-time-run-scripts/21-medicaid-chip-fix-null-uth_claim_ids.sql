/*************************************
 * Isrrael noticed some blank UTH CLAIM IDs in data_warehouse.claim_icd_proc
 * 
 * I (Xiaorui) verified that the only affected table was the proc table but 
 * couldn't identify the source of the issue in the code so I'm just putting in a hotfix
 * 
 * hotfix implemented: 04/25/24
 */

/********************************************
Problem:

select year, 
	sum(case when uth_claim_id is null then 1 else 0 end) * 1.0 / count(*)
from data_warehouse.claim_icd_proc
where data_source = 'mcpp'
group by 1 order by 1;

2015	0.00106394111696375624
2016	0.00373944374353553608
2017	0.00611998035857575721
2018	0.00528638693893608389
2019	0.00873426155125542852
2020	0.00442409361382086845
2021	0.00947123378782840040
2022	0.01014159599745011301
 */

update data_warehouse.claim_icd_proc a
set uth_claim_id = b.uth_claim_id
from data_warehouse.dim_uth_claim_id b
where a.data_source = 'mcpp'
	and a.uth_claim_id is null
	and b.data_source = 'mcpp'
	and a.data_source = b.data_source
	and a.claim_id_src = b.claim_id_src;

vacuum analyze data_warehouse.claim_icd_proc;

/********************************************
Verification of fix:

select year, 
	sum(case when uth_claim_id is null then 1 else 0 end) * 1.0 / count(*)
from data_warehouse.claim_icd_proc
where data_source = 'mcpp'
group by 1 order by 1;

2015	0.000000000000000000000000
2016	0.000000000000000000000000
2017	0.000000000000000000000000
2018	0.000000000000000000000000
2019	0.000000000000000000000000
2020	0.000000000000000000000000
2021	0.000000000000000000000000
2022	0.000000000000000000000000
 */














