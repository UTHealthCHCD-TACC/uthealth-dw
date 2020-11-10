create table reference_tables.regions
( region char(4),
  state varchar
);

create table reference_tables.hcpcs
( code varchar,
  short_desc varchar,
  long_desc varchar
);

create table reference_tables.cms_proc_codes
( code varchar,
  mod varchar,
  desc varchar,
  xcode varchar
);


create table optum_dod.ref_admit_type (
key int,
value varchar
);

create table optum_dod.ref_admit_type 
as select *
from optum_dod.ref_admit_type;

create table optum_dod.ref_admit_channel (
type_id int,
key varchar,
value_derived varchar,
value_original varchar,
category varchar
);

create table optum_dod.ref_admit_channel
as select *
from optum_dod.ref_admit_channel;
