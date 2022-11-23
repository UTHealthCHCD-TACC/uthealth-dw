drop  table if exists dev.dim_uth_rx_claim_id;

CREATE TABLE dev.dim_uth_rx_claim_id (
	data_source bpchar(4) NULL,
	"year" int2 NULL,
	uth_rx_claim_id bigserial NOT NULL,
	rx_claim_id_src text NULL,
	uth_member_id int8 NULL,
	member_id_src text NULL
)
DISTRIBUTED BY (uth_member_id);

/*
 * Build table of distinct rx id srcs to populate dim table
 */

drop table if exists dw_staging.dim_rx_load_mdcd;

create table dw_staging.dim_rx_load_mdcd as 
select distinct * from (
select a.year_fy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.chip_rx a
union all
select a.year_fy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.mco_rx a
union all
select  dev.fiscal_year_func(a.rx_fill_dt) as year_fy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.ffs_rx a
union all
select  dev.fiscal_year_func(a.rx_fill_dt) as year_fy,
       pcn || ndc || replace(rx_fill_dt::text, '-','') as rx_claim_id_src,
       a.pcn::text as member_id_src
  from medicaid.htw_ffs_rx a
  ) a
  distributed by (member_id_src)
;

/*
 * Insert records into dim table
 */

insert into dev.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )
select 'mdcd', 
       year_fy ,
       nextval('dev.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       a.rx_claim_id_src ,
       b.uth_member_id, 
       a.member_id_src
  from dw_staging.dim_rx_load_mdcd a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = a.member_id_src 
   and b.data_source = 'mdcd' 
  left outer join dev.dim_uth_rx_claim_id c 
    on c.member_id_src = a.member_id_src 
   and c.rx_claim_id_src = a.rx_claim_id_src 
   and c.data_source = 'mdcd' 
 where c.uth_rx_claim_id is null 
;

select count(distinct pcn || ndc || replace(rx_fill_dt::text, '-','')  ) from medicaid.ffs_rx

Updated Rows	52336354

select * from dev.dim_uth_rx_claim_id;

vacuum full analyze dev.dim_uth_rx_claim_id;

select count(*) from dev.dim_uth_rx_claim_id; --331489584

select count(distinct uth_rx_claim_id) from dev.dim_uth_rx_claim_id; --331489584

select count(distinct rx_claim_id_src) from dev.dim_uth_rx_claim_id; --331489584