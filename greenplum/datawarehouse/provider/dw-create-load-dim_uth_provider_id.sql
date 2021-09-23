/* ******************************************************************************************************
 *  This generates a uth_provider_id for all data sources 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 9/23/2021 || consolidated into one script
 * ******************************************************************************************************
*/


-----------------------------------  BEGIN SCRIPT  ------------------------------------------


------------------------------------------------------------------------------------------
--Optum Zip
------------------------------------------------------------------------------------------
--add records to dim_uth_provider_id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    
select distinct 'optz' data_source, p.prov_unique::text 
	from optum_zip.provider p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = p.prov_unique::text and d.data_source = 'optz'
	where d.uth_provider_id is null
		and p.prov_unique is not null
		and p.prov_unique > 0;
	
	

------------------------------------------------------------------------------------------
--Optum DoD
------------------------------------------------------------------------------------------
--add records to dim_uth_provider_id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    
select distinct 'optd' data_source, p.prov_unique::text 
	from optum_dod.provider p
	left join data_warehouse.dim_uth_provider_id d 
	       on d.provider_id_src = p.prov_unique::text 
	      and d.data_source = 'optd'
where d.uth_provider_id is null
  and p.prov_unique is not null
  and p.prov_unique > 0;	
 
 
 ------------------------------------------------------------------------------------------
--Truven
------------------------------------------------------------------------------------------
--add records to dim_uth_provider_id
--ccaef -- get provider id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text 
	from truven.ccaef p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0; 
	
--ccaes -- get provider id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text  	
	from truven.ccaes p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0;

--ccaeo -- get provider id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text 
	from truven.ccaeo p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text  and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0;	

--ccaei -- get physician id	
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.physid as numeric(10,0))::text 
	from truven.ccaei p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.physid as numeric(10,0))::text  and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.physid is not null
		and p.physid > 0;			

--mdcrf -- get provider id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text 
	from truven.mdcrf p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0; 
	
--mdcrs -- get provider id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text  	
	from truven.mdcrs p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0;

--mdcro -- get provider id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text 
	from truven.mdcro p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text  and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0;	

--mdcri -- get physician id	
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.physid as numeric(10,0))::text 
	from truven.mdcri p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = cast(p.physid as numeric(10,0))::text  and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.physid is not null
		and p.physid > 0;			
 	
	
 
------------------------------------------------------------------------------------------
--Medicare National
	-- has no provider_id aside from npi to generate uth id, so using that as both provider_id_src and npi
------------------------------------------------------------------------------------------
with providers as 
(
        select org_npi_num as provider_id_src
        from  medicare_national.inpatient_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_national.inpatient_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_national.inpatient_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_national.inpatient_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_national.inpatient_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_national.outpatient_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_national.outpatient_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_national.outpatient_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_national.outpatient_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_national.outpatient_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_national.hospice_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_national.hospice_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_national.hospice_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_national.hospice_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_national.hospice_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_national.snf_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_national.snf_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_national.snf_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_national.snf_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_national.snf_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_national.hha_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_national.hha_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_national.hha_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_national.hha_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_national.hha_base_claims_k
union all
        select carr_clm_blg_npi_num as provider_id_src
          from medicare_national.bcarrier_claims_k
          union all
        select rfr_physn_npi as provider_id_src
          from medicare_national.bcarrier_claims_k
union all
        select prf_physn_npi as provider_id_src
        from medicare_national.bcarrier_line_k
),
dstnc_providers as (
    select distinct provider_id_src as provider_id_src from providers 
)
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)
select distinct 'mcrn' as data_source, p.provider_id_src
  from dstnc_providers p
  left join data_warehouse.dim_uth_provider_id d 
    on d.provider_id_src = p.provider_id_src
   and d.data_source = 'mcrn' 
 where d.uth_provider_id is null
   and p.provider_id_src is not null;
  
  
  
 
------------------------------------------------------------------------------------------
--Medicare Texas
------------------------------------------------------------------------------------------  
 with providers as 
(
        select org_npi_num as provider_id_src
        from  medicare_texas.inpatient_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_texas.inpatient_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_texas.inpatient_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_texas.inpatient_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_texas.inpatient_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_texas.outpatient_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_texas.outpatient_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_texas.outpatient_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_texas.outpatient_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_texas.outpatient_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_texas.hospice_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_texas.hospice_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_texas.hospice_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_texas.hospice_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_texas.hospice_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_texas.snf_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_texas.snf_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_texas.snf_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_texas.snf_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_texas.snf_base_claims_k
union all
        select org_npi_num as provider_id_src
        from  medicare_texas.hha_base_claims_k 
        union all
        select at_physn_npi as provider_id_src
        from  medicare_texas.hha_base_claims_k 
        union all
        select op_physn_npi as provider_id_src
        from  medicare_texas.hha_base_claims_k 
        union all
        select ot_physn_npi as provider_id_src
        from  medicare_texas.hha_base_claims_k 
        union all
        select rndrng_physn_npi as provider_id_src
        from  medicare_texas.hha_base_claims_k
union all
        select carr_clm_blg_npi_num as provider_id_src
          from medicare_texas.bcarrier_claims_k
          union all
        select rfr_physn_npi as provider_id_src
          from medicare_texas.bcarrier_claims_k
union all
        select prf_physn_npi as provider_id_src
        from medicare_texas.bcarrier_line_k
),
dstnc_providers as (
    select distinct provider_id_src as provider_id_src from providers 
)
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)
select distinct 'mcrt' as data_source, p.provider_id_src
  from dstnc_providers p
  left join data_warehouse.dim_uth_provider_id d 
    on d.provider_id_src = p.provider_id_src
   and d.data_source = 'mcrt' 
 where d.uth_provider_id is null
   and p.provider_id_src is not null;
  
  
  
  
---finalize
 vacuum analyze data_warehouse.dim_uth_provider_id; 
  
---validate
select count(*), data_source 
from data_warehouse.dim_uth_provider_id 
group by data_source order by data_source;
  
  
  
  
------------------------------------ END SCRIPT ------------------------------------------


/*
original create table statement, run only if table has to be dropped 

--create dim_uth_provider_id
drop table if exists data_warehouse.dim_uth_provider_id;

create table data_warehouse.dim_uth_provider_id (
	uth_provider_id bigserial,
	data_source bpchar(4),
	provider_id_src text not null 
)
with (appendonly=true, orientation=column, compresstype=zlib)
distributed by (provider_id_src);

alter sequence data_warehouse.dim_uth_provider_id_uth_provider_id_seq restart with 100000000;

alter sequence data_warehouse.dim_uth_provider_id_uth_provider_id_seq cache 200;

vacuum analyze data_warehouse.dim_uth_provider_id;\
*/