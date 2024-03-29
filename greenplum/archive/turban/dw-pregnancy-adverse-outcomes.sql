-- Find the total number of unique members in 2018
select count(distinct uth_member_id)
from data_warehouse.claim_diag cd 
where year = 2018
and data_source in ('truv', 'optz'); --35,366,641

-- Find the number of pregnancy related DX codes in 2018
select count(*)
from data_warehouse.claim_diag cd 
where (diag_cd in ('Z3400','Z4301','Z3402','Z3403','Z3480','Z3481','Z3482','Z3483','Z3490','Z3491','Z3492','Z3493','O0900','O0901','O0902','O0903','O0910','O0911','O0912','O0913','O09A0','O09A1','O09A2','O09A3','O09211','O09212','O09213','O09219','O09291','O09292','O09293','O09299','O0930','O0931','O0932','O0933','O0940','O0941','O0942','O0943','O09511','O09512','O09513','O09519','O09521','O09522','O09523','O09529','O09611','O09612','O09613','O09619','O09621','O09622','O09623','O09629','O0970','O0971','O0972','O0973','O09811','O09812','O09813','O09819','O09821','O09822','O09823','O09824','O09891','O09892','O09893','O09899','O0990','O0991','O0992','O0993','Z371','Z373','Z374','Z3760','Z3761','Z3762','Z3763','Z3764','Z3769','Z377','P95','O6010X0','O6010X1','O6010X2','O6010X3','O6010X4','O6010X5','O6010X9','O6012X0','O6012X1','O6012X2','O6012X3','O6012X4','O6012X5','O6012X9','O6013X0','O6013X1','O6013X2','O6013X3','O6013X4','O6013X5','O6013X9','O6014X0','O6014X1','O6014X2','O6014X3','O6014X4','O6014X5','O6014X9','P9160','P9161','P9162','P9163','P91811','P91819','P53','P541','P542','P543','P544','P545','P546','P547','P548','P549','P612','P613')
or diag_cd in ('V220','V221','V230','V2342','V2341','V235','V237','V233','V2381','V2382','V2383','V2384','V2385','V2386','V2389','V239','V231','V232','V2349','V271','V273','V274','V276','V277','7799','6442','64421','7687','76871','76872','76873','7791','776','77211','77212','77213','77214','7722','7723','7724','7725','7726','7728','7729','7766','7765'))
and year = 2018
and data_source in ('truv', 'optz'); --6,866,424

-- Find the number of pregnancy patients in 2018 without complications
select distinct uth_member_id, false as complications
into dev.tu_pregnancy_patients_2018 
from data_warehouse.claim_diag cd 
where (diag_cd in ('Z3400','Z4301','Z3402','Z3403','Z3480','Z3481','Z3482','Z3483','Z3490','Z3491','Z3492','Z3493')
or diag_cd in ('V220','V221'))
and year = 2018
and data_source in ('truv', 'optz'); --545,120

-- Find the number of pregnancy related patients with complications 2018
insert into dev.tu_pregnancy_patients_2018 
select distinct uth_member_id, true as complications
from data_warehouse.claim_diag cd 
where (diag_cd in ('O0900','O0901','O0902','O0903','O0910','O0911','O0912','O0913','O09A0','O09A1','O09A2','O09A3','O09211','O09212','O09213','O09219','O09291','O09292','O09293','O09299','O0930','O0931','O0932','O0933','O0940','O0941','O0942','O0943','O09511','O09512','O09513','O09519','O09521','O09522','O09523','O09529','O09611','O09612','O09613','O09619','O09621','O09622','O09623','O09629','O0970','O0971','O0972','O0973','O09811','O09812','O09813','O09819','O09821','O09822','O09823','O09824','O09891','O09892','O09893','O09899','O0990','O0991','O0992','O0993','Z371','Z373','Z374','Z3760','Z3761','Z3762','Z3763','Z3764','Z3769','Z377','P95','O6010X0','O6010X1','O6010X2','O6010X3','O6010X4','O6010X5','O6010X9','O6012X0','O6012X1','O6012X2','O6012X3','O6012X4','O6012X5','O6012X9','O6013X0','O6013X1','O6013X2','O6013X3','O6013X4','O6013X5','O6013X9','O6014X0','O6014X1','O6014X2','O6014X3','O6014X4','O6014X5','O6014X9','P9160','P9161','P9162','P9163','P91811','P91819','P53','P541','P542','P543','P544','P545','P546','P547','P548','P549','P612','P613')
or diag_cd in ('V230','V2342','V2341','V235','V237','V233','V2381','V2382','V2383','V2384','V2385','V2386','V2389','V239','V231','V232','V2349','V271','V273','V274','V276','V277','7799','6442','64421','7687','76871','76872','76873','7791','776','77211','77212','77213','77214','7722','7723','7724','7725','7726','7728','7729','7766','7765'))
and year = 2018
and data_source in ('truv', 'optz'); --291,855

-- Find the total number of pregnancy related patients in 2018
select count(distinct uth_member_id)
from data_warehouse.claim_diag cd 
where (diag_cd in ('Z3400','Z4301','Z3402','Z3403','Z3480','Z3481','Z3482','Z3483','Z3490','Z3491','Z3492','Z3493','O0900','O0901','O0902','O0903','O0910','O0911','O0912','O0913','O09A0','O09A1','O09A2','O09A3','O09211','O09212','O09213','O09219','O09291','O09292','O09293','O09299','O0930','O0931','O0932','O0933','O0940','O0941','O0942','O0943','O09511','O09512','O09513','O09519','O09521','O09522','O09523','O09529','O09611','O09612','O09613','O09619','O09621','O09622','O09623','O09629','O0970','O0971','O0972','O0973','O09811','O09812','O09813','O09819','O09821','O09822','O09823','O09824','O09891','O09892','O09893','O09899','O0990','O0991','O0992','O0993','Z371','Z373','Z374','Z3760','Z3761','Z3762','Z3763','Z3764','Z3769','Z377','P95','O6010X0','O6010X1','O6010X2','O6010X3','O6010X4','O6010X5','O6010X9','O6012X0','O6012X1','O6012X2','O6012X3','O6012X4','O6012X5','O6012X9','O6013X0','O6013X1','O6013X2','O6013X3','O6013X4','O6013X5','O6013X9','O6014X0','O6014X1','O6014X2','O6014X3','O6014X4','O6014X5','O6014X9','P9160','P9161','P9162','P9163','P91811','P91819','P53','P541','P542','P543','P544','P545','P546','P547','P548','P549','P612','P613')
or diag_cd in ('V220','V221','V230','V2342','V2341','V235','V237','V233','V2381','V2382','V2383','V2384','V2385','V2386','V2389','V239','V231','V232','V2349','V271','V273','V274','V276','V277','7799','6442','64421','7687','76871','76872','76873','7791','776','77211','77212','77213','77214','7722','7723','7724','7725','7726','7728','7729','7766','7765'))
and year = 2018
and data_source in ('truv', 'optz'); --635,892

-- Find the total number of pregnancy related DXs in 2018 grouped by DX code
select diag_cd, dx.description, data_source, count(*)
from data_warehouse.claim_diag cd 
left join reference_tables.icd_10_diags dx on dx.code = cd.diag_cd 
where (diag_cd in ('Z3%','O0900','O0901','O0902','O0903','O0910','O0911','O0912','O0913','O09A0','O09A1','O09A2','O09A3','O09211','O09212','O09213','O09219','O09291','O09292','O09293','O09299','O0930','O0931','O0932','O0933','O0940','O0941','O0942','O0943','O09511','O09512','O09513','O09519','O09521','O09522','O09523','O09529','O09611','O09612','O09613','O09619','O09621','O09622','O09623','O09629','O0970','O0971','O0972','O0973','O09811','O09812','O09813','O09819','O09821','O09822','O09823','O09824','O09891','O09892','O09893','O09899','O0990','O0991','O0992','O0993','Z371','Z373','Z374','Z3760','Z3761','Z3762','Z3763','Z3764','Z3769','Z377','P95','O6010X0','O6010X1','O6010X2','O6010X3','O6010X4','O6010X5','O6010X9','O6012X0','O6012X1','O6012X2','O6012X3','O6012X4','O6012X5','O6012X9','O6013X0','O6013X1','O6013X2','O6013X3','O6013X4','O6013X5','O6013X9','O6014X0','O6014X1','O6014X2','O6014X3','O6014X4','O6014X5','O6014X9','P9160','P9161','P9162','P9163','P91811','P91819','P53','P541','P542','P543','P544','P545','P546','P547','P548','P549','P612','P613')
or diag_cd in ('V220','V221','V230','V2342','V2341','V235','V237','V233','V2381','V2382','V2383','V2384','V2385','V2386','V2389','V239','V231','V232','V2349','V271','V273','V274','V276','V277','7799','6442','64421','7687','76871','76872','76873','7791','776','77211','77212','77213','77214','7722','7723','7724','7725','7726','7728','7729','7766','7765'))
and year = 2018
and data_source in ('truv', 'optz')
group by diag_cd, dx.description, data_source;


-- Find the total number of pre-term birth related DXs in 2018 grouped by DX code
select diag_cd, dx.description, data_source, count(*)
from data_warehouse.claim_diag cd 
left join reference_tables.icd_10_diags dx on dx.code = cd.diag_cd 
where (diag_cd in ('O6012X0','O6012X1','O6012X2','O6012X3','O6012X5','O6012X9','O6012X4','O6013X0','O6013X1','O6013X2','O6013X9','O6013X3','O6013X4','O6014X1','O6014X2','O6014X3','O6014X9','O6014X4','O6014X0','O6014X5','O6010X1','O6010X2','O6010X3','O6010X4','O6010X5','O6010X0','O6010X9')
or diag_cd in ('6442','64421'))
and year = 2018
and data_source in ('truv', 'optz')
group by diag_cd, dx.description, data_source;


-- Find specific codes for pre-term birth related DXs in 2018 grouped by DX code
select diag_cd, dx.description, data_source, cd.uth_member_id, cd."date" 
from data_warehouse.claim_diag cd 
left join reference_tables.icd_10_diags dx on dx.code = cd.diag_cd 
where (diag_cd in ('O6012X0','O6012X1','O6012X2','O6012X3','O6012X5','O6012X9','O6012X4','O6013X0','O6013X1','O6013X2','O6013X9','O6013X3','O6013X4','O6014X1','O6014X2','O6014X3','O6014X9','O6014X4','O6014X0','O6014X5','O6010X1','O6010X2','O6010X3','O6010X4','O6010X5','O6010X0','O6010X9')
or diag_cd in ('6442','64421'))
and year = 2018
and data_source in ('truv', 'optz')
limit 10;

-- Find specific codes for pre-term birth related DXs in 2018 grouped by DX code
select distinct diag_cd, dx.description, data_source, cd.uth_member_id, cd."date"
from data_warehouse.claim_diag cd 
left join reference_tables.icd_10_diags dx on dx.code = cd.diag_cd 
where uth_member_id = 180319098;

select *
from data_warehouse.pharmacy_claims pc 
where uth_member_id = 180319098;


select *
from reference_tables.icd_10_diags dx
where dx.code like 'Z3%'
order by dx.code;