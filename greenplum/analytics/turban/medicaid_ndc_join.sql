create materialized view reference_tables.ref_ndcs as 
select  prod.productndc, prod.nonproprietaryname, pack.ndcpackagecode, pack.packagedescription, pack."11_digit_ndc", replace(pack."11_digit_ndc" , '-', '') as hyphenless_11_digit_ndc
from reference_tables.ref_ndc_product prod
join reference_tables.ref_ndc_package pack on pack.productndc = prod.productndc;


select rx.rx_quantity, rx.ndc, ndc.hyphenless_11_digit_ndc, ndc.nonproprietaryname, ndc.packagedescription 
from medicaid.chip_rx rx
join reference_tables.ref_ndcs ndc on rx.ndc = ndc.hyphenless_11_digit_ndc 
where ndc.nonproprietaryname ilike '%progesterone%'
limit 300;
