/* ******************************************************************************************************
 *  Creates a bunch of reference tables.  Loaded with static files in google drive
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
  *  wallingTACC  || 8/23/2021 || archived
 * *******************************************************************************************************/

---Tables loaded from external data files

create table reference_tables.ref_bill_type_classification ( bill_type_classification_cd char(1), bill_type_institution_cd_list text, bill_type_classification_desc text );

create table reference_tables.ref_bill_type_frequency ( bill_type_frequency_cd char(1), bill_type_frequency_desc text );

create table reference_tables.ref_bill_type_institution ( bill_type_institution_cd char(1), bill_type_institution_desc text );

create table reference_tables.ref_optum_type_of_service (tos_cd_src text, level_1_desc text, level_2_desc text, bill_type_institution_cd char(1), bill_type_classification_cd char(1) );


create table reference_tables.ref_revenue_code ( revenue_cd char(4), revenue_cd_desc text );

create table reference_tables.ref_type_of_service ( type_of_service_cd char(2), type_of_service_desc text );

create table reference_tables.ref_provider_specialty ( provider_specialty_cd char(3), provider_specialty_desc text );

create table reference_tables.ref_place_of_treatment ( place_of_treatment_cd char(2), place_of_treatment_desc text );

create table reference_tables.ref_optum_bill_type_from_tos (tos text, inst_code int, class_code int);


-----

---bill type ref table created as aggregate of 3 bill_type tables
select bill_type_institution_cd || bill_type_classification_cd || bill_type_frequency_cd as bill_type_cd, 
       bill_type_institution_desc || ' ' || bill_type_classification_desc ||' ' || bill_type_frequency_desc as bill_type_desc,
       bill_type_institution_cd, bill_type_classification_cd, bill_type_frequency_cd, 
       bill_type_institution_desc, bill_type_classification_desc, bill_type_frequency_desc
into reference_tables.ref_bill_type_cd
from reference_tables.ref_bill_type_institution 
  left outer join reference_tables.ref_bill_type_frequency
     on bill_type_frequency_cd is not null
  left outer join reference_tables.ref_bill_type_classification
     on position( substring(bill_type_institution_cd,1,1) in bill_type_institution_cd_list ) > 0 
order by 1
 ;
 
select * from reference_tables.ref_bill_type_cd;


create table reference_tables.ref_zip_code (zip text, lat text, long text, city text, state text, countyname text, ziptype text, location_desc text);