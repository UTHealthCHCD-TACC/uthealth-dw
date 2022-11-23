insert into dev.testing_psql_file1
select * from "reference_tables.ref_cms_icd_cm_codes" 
limit 200;

analyze dev.testing_psql_file1 ;