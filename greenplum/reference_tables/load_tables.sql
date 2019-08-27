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