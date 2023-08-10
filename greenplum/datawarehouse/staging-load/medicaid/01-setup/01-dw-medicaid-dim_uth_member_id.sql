/**********************************************
 * Purpose: Generate UTH_MEMBER_IDs for client_nbrs in Medicaid
 * 
 * Author 		| Date 	| Change
 * ------------------------------------------
 * Will/David/Joe | ?? 	| Wrote this script
 * Xiaorui | 04/07/2023 | Edited for chip_enrl
 * Xiaorui | 08/03/2023 | Modified so that mhtw and mcpp get uth_member_ids too
 *********************************************/

/*********************************************
* Create temp table that figures out what data source members are supposed to be in
*/

drop table if exists dw_staging.mcd_enrl_temp;

create table dw_staging.mcd_enrl_temp (
	data_source bpchar(4),
	client_nbr varchar(20),
	me_code varchar(1),
	chip_per_fl varchar(2)
);

--insert data from general enrollment table
insert into dw_staging.mcd_enrl_temp(client_nbr, me_code)
select distinct client_nbr, me_code from medicaid.enrl;

--insert data from chip enrollment table
insert into dw_staging.mcd_enrl_temp(client_nbr, chip_per_fl)
select distinct client_nbr, chip_per_fl from medicaid.chip_enrl;

--insert data from htw enrollment table
insert into dw_staging.mcd_enrl_temp(client_nbr, me_code)
select distinct client_nbr, 'W' as me_code from medicaid.htw_enrl;

--fill in data_source
update dw_staging.mcd_enrl_temp
set data_source =
	case when chip_per_fl = 'CP' then 'mcpp'
		when me_code = 'W' then 'mhtw'
		else 'mdcd' end
where data_source is null;

--select * from dw_staging.mcd_enrl_temp where me_code is not null;

vacuum analyze dw_staging.mcd_enrl_temp;

/***********************************
 * Insert new mdcd members into dim
 */

--mdcd
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mdcd' as v_raw_data 
	from dw_staging.mcd_enrl_temp a
    left outer join data_warehouse.dim_uth_member_id b
      on b.data_source = 'mdcd' 
     and a.client_nbr = b.member_id_src
    where a.data_source = 'mdcd'
     and b.member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

--mhtw
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mhtw' as v_raw_data 
	from dw_staging.mcd_enrl_temp a
    left outer join data_warehouse.dim_uth_member_id b
      on b.data_source = 'mhtw' 
     and a.client_nbr = b.member_id_src
    where a.data_source = 'mhtw'
     and b.member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

--mcpp
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id )
with cte_distinct_member as ( 
   select distinct client_nbr as v_member_id, 'mcpp' as v_raw_data 
	from dw_staging.mcd_enrl_temp a
    left outer join data_warehouse.dim_uth_member_id b
      on b.data_source = 'mcpp' 
     and a.client_nbr = b.member_id_src
    where a.data_source = 'mcpp'
     and b.member_id_src is null 
) 
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

vacuum analyze data_warehouse.dim_uth_member_id;


/*
---------member ids claim created 
vacuum analyze data_warehouse.dim_uth_member_id;

insert into data_warehouse.dim_uth_member_id  (member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as ( 
	select distinct pcn as v_member_id, 'mdcd' as v_data_source
	from ( 
			select a.pcn
			from medicaid.clm_proc a
		union 
			select a.mem_id as pcn
			from medicaid.enc_proc a   
		 ) raw_clms
   left outer join data_warehouse.dim_uth_member_id b 
   		 on b.member_id_src = raw_clms.pcn 
  		and b.data_source = 'mdcd'
	where b.member_id_src is null 
 )
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created
from cte_distinct_member ;
*/

/******************
 * Hotfix: I accidentally inserted everyone from FY22 as mdcd, so let's fix that
 * 
 * TO DO: Delete people who are assigned medicaid who are ONLY chip peri or mhtw from dim
 */

delete from data_warehouse.dim_uth_member_id
where data_source = 'mdcd' and
	member_id_src in (
		select a.client_nbr 
		from 
			(select client_nbr from dw_staging.mcd_enrl_temp
			where data_source != 'mdcd'
			) a left join 
			(select client_nbr from dw_staging.mcd_enrl_temp 
			where data_source = 'mdcd') b
		on a.client_nbr = b.client_nbr
		where b.client_nbr is null
);

--does anyone have > 1 uth_member_id?

select count(*) from (
	select data_source, member_id_src, count(*)
	from data_warehouse.dim_uth_member_id
	where data_source in ('mdcd', 'mhtw', 'mcpp')
	group by 1, 2
	having count(*) > 1) t;

--31493 rows are dupes

--delete dupes from table 
delete from data_warehouse.dim_uth_member_id
where uth_member_id in (
	select uth_member_id from
		(select *,
			row_number() over (partition by data_source, member_id_src
				order by uth_member_id) as rn
		from data_warehouse.dim_uth_member_id
		where member_id_src in (
			select member_id_src from data_warehouse.dim_uth_member_id
			where data_source in ('mdcd', 'mhtw', 'mcpp')
			group by data_source, member_id_src
			having count(*) > 1
		) ) b
	where rn != 1
);

--ok that fixed it


