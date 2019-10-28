---Tables loaded from external data files

create table reference_tables.ref_bill_type_classification ( bill_type_classification_cd char(1), bill_type_institution_cd_list text, bill_type_classification_desc text );

create table reference_tables.ref_bill_type_frequency ( bill_type_frequency_cd char(1), bill_type_frequency_desc text );

create table reference_tables.ref_bill_type_institution ( bill_type_institution_cd char(1), bill_type_institution_desc text );

create table data_warehouse.ref_optum_type_of_service (tos_cd_src text, level_1_desc text, level_2_desc text, bill_type_institution_cd char(1), bill_type_classification_cd char(1) );

-----

---bill type ref table created as aggregate of 3 bill_type tables
select bill_type_institution_cd || bill_type_classification_cd || bill_type_frequency_cd as bill_type_cd, 
       bill_type_institution_desc || ' ' || bill_type_classification_desc ||' ' || bill_type_frequency_desc as bill_type_desc,
       bill_type_institution_cd, bill_type_classification_cd, bill_type_frequency_cd, 
       bill_type_institution_desc, bill_type_classification_desc, bill_type_frequency_desc
into data_warehouse.ref_bill_type_cd
from data_warehouse.ref_bill_type_institution 
  left outer join data_warehouse.ref_bill_type_frequency
     on bill_type_frequency_cd is not null
  left outer join data_warehouse.ref_bill_type_classification
     on position( substring(bill_type_institution_cd,1,1) in bill_type_institution_cd_list ) > 0 
order by 1
 ;
 
select * from data_warehouse.ref_bill_type_cd;