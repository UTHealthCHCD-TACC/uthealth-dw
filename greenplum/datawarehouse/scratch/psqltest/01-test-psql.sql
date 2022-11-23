drop table if exists dev.testing_psql_file1;

create table dev.testing_psql_file1 as 
select * from "reference_tables.ref_cms_icd_cm_codes" 
limit 10;

analyze dev.testing_psql_file1 ;
