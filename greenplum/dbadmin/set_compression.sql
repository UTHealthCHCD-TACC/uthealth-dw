create table data_warehouse.dim_uth_rx_claim_id_fixed (like data_warehouse.dim_uth_rx_claim_id)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.dim_uth_rx_claim_id_fixed
select * from data_warehouse.dim_uth_rx_claim_id;

ALTER sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq OWNED BY NONE;
ALTER TABLE data_warehouse.dim_uth_rx_claim_id_fixed ALTER COLUMN uth_rx_claim_id SET DEFAULT nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq');
ALTER sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq owner to walling;
ALTER sequence data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq OWNED BY data_warehouse.dim_uth_rx_claim_id_fixed.uth_rx_claim_id;

drop table data_warehouse.dim_uth_rx_claim_id;
alter table data_warehouse.dim_uth_rx_claim_id_fixed rename to dim_uth_rx_claim_id;

----

create table data_warehouse.dim_uth_claim_id_fixed (like data_warehouse.dim_uth_claim_id)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.dim_uth_claim_id_fixed
select * from data_warehouse.dim_uth_claim_id;

ALTER sequence data_warehouse.dim_uth_claim_id_uth_claim_id_seq OWNED BY NONE;
ALTER TABLE data_warehouse.dim_uth_claim_id_fixed ALTER COLUMN uth_claim_id SET DEFAULT nextval('data_warehouse.dim_uth_claim_id_uth_claim_id_seq');
ALTER sequence data_warehouse.dim_uth_claim_id_uth_claim_id_seq owner to walling;
ALTER sequence data_warehouse.dim_uth_claim_id_uth_claim_id_seq OWNED BY data_warehouse.dim_uth_claim_id_fixed.uth_claim_id;


drop table data_warehouse.dim_uth_claim_id;
alter table data_warehouse.dim_uth_claim_id_fixed rename to dim_uth_claim_id;

----

create table data_warehouse.dim_uth_admission_id_fixed (like data_warehouse.dim_uth_admission_id)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.dim_uth_admission_id_fixed
select * from data_warehouse.dim_uth_admission_id_old;

--CREATE SEQUENCE data_warehouse.dim_uth_admission_id_uth_admission_id_seq START 1113381015;
ALTER sequence data_warehouse.dim_uth_admission_id_uth_admission_id_seq OWNED BY NONE;
ALTER TABLE data_warehouse.dim_uth_admission_id_fixed ALTER COLUMN uth_admission_id SET DEFAULT nextval('data_warehouse.dim_uth_admission_id_uth_admission_id_seq');
ALTER sequence data_warehouse.dim_uth_admission_id_uth_admission_id_seq owner to walling;
ALTER sequence data_warehouse.dim_uth_admission_id_uth_admission_id_seq OWNED BY data_warehouse.dim_uth_admission_id_fixed.uth_admission_id;

drop table data_warehouse.dim_uth_admission_id;
alter table data_warehouse.dim_uth_admission_id_fixed rename to dim_uth_admission_id;

----

create table data_warehouse.claim_detail_fixed (like data_warehouse.claim_detail)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.claim_detail_fixed
select * from data_warehouse.claim_detail;

drop table data_warehouse.claim_detail;
alter table data_warehouse.claim_detail_fixed rename to claim_detail;

----

create table data_warehouse.claim_diag_fixed (like data_warehouse.claim_diag)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.claim_diag_fixed
select * from data_warehouse.claim_diag;

drop table data_warehouse.claim_diag;
alter table data_warehouse.claim_diag_fixed rename to claim_diag;

----

create table data_warehouse.claim_header_fixed (like data_warehouse.claim_header)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.claim_header_fixed
select * from data_warehouse.claim_header;

drop table data_warehouse.claim_header;
alter table data_warehouse.claim_header_fixed rename to claim_header;

----

create table data_warehouse.claim_icd_proc_fixed (like data_warehouse.claim_icd_proc)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.claim_icd_proc_fixed
select * from data_warehouse.claim_icd_proc;

drop table data_warehouse.claim_icd_proc;
alter table data_warehouse.claim_icd_proc_fixed rename to claim_icd_proc;

----

create table data_warehouse.claim_provider_fixed (like data_warehouse.claim_provider)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.claim_provider_fixed
select * from data_warehouse.claim_provider;

drop table data_warehouse.claim_provider;
alter table data_warehouse.claim_provider_fixed rename to claim_provider;

----

create table data_warehouse.member_enrollment_yearly_fixed (like data_warehouse.member_enrollment_yearly)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.member_enrollment_yearly_fixed
select * from data_warehouse.member_enrollment_yearly;

drop table data_warehouse.member_enrollment_yearly;
alter table data_warehouse.member_enrollment_yearly_fixed rename to member_enrollment_yearly;

----

create table data_warehouse.medicaid_program_enrollment_fixed (like data_warehouse.medicaid_program_enrollment)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.medicaid_program_enrollment_fixed
select * from data_warehouse.medicaid_program_enrollment;

drop table data_warehouse.medicaid_program_enrollment;
alter table data_warehouse.medicaid_program_enrollment_fixed rename to medicaid_program_enrollment;

----

create table data_warehouse.provider_fixed (like data_warehouse.provider)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into data_warehouse.provider_fixed
select * from data_warehouse.provider;

drop table data_warehouse.provider;
alter table data_warehouse.provider_fixed rename to provider;

----
create table truven.ccaea_fixed (like truven.ccaea)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.ccaea_fixed
select * from truven.ccaea;

drop table truven.ccaea;
alter table truven.ccaea_fixed rename to ccaea;

----

create table truven.ccaed_fixed (like truven.ccaed)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.ccaed_fixed
select * from truven.ccaed;

drop table truven.ccaed;
alter table truven.ccaed_fixed rename to ccaed;

----

create table truven.ccaef_fixed (like truven.ccaef)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.ccaef_fixed
select * from truven.ccaef;

drop table truven.ccaef;
alter table truven.ccaef_fixed rename to ccaef;

----

create table truven.ccaei_fixed (like truven.ccaei)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.ccaei_fixed
select * from truven.ccaei;

drop table truven.ccaei;
alter table truven.ccaei_fixed rename to ccaei;

----

create table truven.ccaeo_fixed (like truven.ccaeo)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.ccaeo_fixed
select * from truven.ccaeo;

drop table truven.ccaeo;
alter table truven.ccaeo_fixed rename to ccaeo;

----

create table truven.mdcra_fixed (like truven.mdcra)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.mdcra_fixed
select * from truven.mdcra;

drop table truven.mdcra;
alter table truven.mdcra_fixed rename to mdcra;

----

create table truven.mdcrd_fixed (like truven.mdcrd)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.mdcrd_fixed
select * from truven.mdcrd;

drop table truven.mdcrd;
alter table truven.mdcrd_fixed rename to mdcrd;

----

create table truven.mdcrf_fixed (like truven.mdcrf)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.mdcrf_fixed
select * from truven.mdcrf;

drop table truven.mdcrf;
alter table truven.mdcrf_fixed rename to mdcrf;

----

create table truven.mdcri_fixed (like truven.mdcri)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.mdcri_fixed
select * from truven.mdcri;

drop table truven.mdcri;
alter table truven.mdcri_fixed rename to mdcri;

----

create table truven.mdcro_fixed (like truven.mdcro)
WITH (appendonly=true, orientation=column, compresstype=zlib);

insert into truven.mdcro_fixed
select * from truven.mdcro;

drop table truven.mdcro;
alter table truven.mdcro_fixed rename to mdcro;

