/* ******************************************************************************************************
 *  This generates a uth_provider_id for all data sources 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 9/23/2021 || consolidated into one script
 * 
 * jw001 || 11/12/2021  || wrapped in function and replaced
 * ******************************************************************************************************
*/

CREATE OR REPLACE FUNCTION dw_staging.load_uth_provider()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

begin

-----------------------------------  BEGIN SCRIPT  ------------------------------------------

------------------------------------------------------------------------------------------
--Optum DOD
------------------------------------------------------------------------------------------
raise notice 'begin script';
raise notice 'load optd begin';

------ provider optd
insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'optd' as data_source,
a.p as provider_id_src,
b.prov_unique as provider_id_src_2,
a.npi as npi,
b.taxonomy1 as taxonomy1,
b.taxonomy2 as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
b.provcat as provcat,
b.prov_type as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
b.prov_state as state,
null as zip,
null as zip_5
from optum_dod.provider_bridge a
join optum_dod.provider b on a.prov_unique = b.prov_unique 
left join data_warehouse.uth_provider c on c.provider_id_src = a.prov::text and data_source = 'optd'
where c.uth_provider_id is null 
and a.prov is not null
;

raise notice 'load optd finished';

------------------------------------------------------------------------------------------
--Optum Zip
------------------------------------------------------------------------------------------

raise notice 'load optz begin';

insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'optz' as data_source,
a.prov as provider_id_src,
b.prov_unique as provider_id_src_2,
a.npi as npi,
b.taxonomy1 as taxonomy1,
b.taxonomy2 as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
b.provcat as provcat,
b.prov_type as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
b.prov_state as state,
null as zip,
null as zip_5
from optum_zip.provider_bridge a
join optum_zip.provider b on a.prov_unique = b.prov_unique 
left join data_warehouse.uth_provider c on c.provider_id_src = a.prov::text and data_source = 'optz'
where c.uth_provider_id is null 
and a.prov is not null
;

raise notice 'load optz finished';


------------------------------------------------------------------------------------------
--TRUVEN
------------------------------------------------------------------------------------------

raise notice 'load truven begin';
-------ccaeo
insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'truv' as data_source,
a.provid as provider_id_src,
null as provider_id_src_2,
a.npi as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
a.stdprov as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaeo a
left join data_warehouse.uth_provider c 
			on c.provider_id_src = a.provid::text 
			and c.npi = a.npi
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.provid is not null
;




-------ccaef
insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'truv' as data_source,
a.provid as provider_id_src,
null as provider_id_src_2,
a.npi as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
a.stdprov as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaef a
left join data_warehouse.uth_provider c 
			on c.provider_id_src = a.provid::text 
			and c.npi = a.npi
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.provid is not null
;




-------ccaes
insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'truv' as data_source,
a.provid as provider_id_src,
null as provider_id_src_2,
a.npi as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
a.stdprov as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaes a
left join data_warehouse.uth_provider c 
			on c.provider_id_src = a.provid::text 
			and c.npi = a.npi
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.provid is not null
;


-------ccaei ( no npi)
insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select
distinct
'truv' as data_source,
a.physid as provider_id_src,
null as provider_id_src_2,
null as npi,
null as taxonomy1,
null as taxonomy2,
null as spclty_cd1,
null as spclty_cd2,
null as provcat,
null as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
null as state,
null as zip,
null as zip_5
from truven.ccaei a
left join data_warehouse.uth_provider c 
			on c.provider_id_src = a.physid::text 
			and data_source = 'truv'
where c.uth_provider_id is null 
and a.physid is not null
;

raise notice 'load truven finished';



----------------------------------------------------------
-------------Medicare------------------------------
----------------------------------------------------------

raise notice 'load mcrn begin';

with bcarrier_line 
    as (
        select prf_physn_npi as npi,
        prvdr_spclty as specialty_cd,
        prvdr_state_cd as state,
        prvdr_zip as zip
        from medicare_national.bcarrier_line_k  
    ),
    spec_st 
     as (
         select carr_clm_blg_npi_num as npi,
         null as specialty_cd,
         null as state,
         null as zip
         from medicare_national.bcarrier_claims_k bck 
   union all
         select rfr_physn_npi as npi,
         null as specialty_cd,
         null as state,
         null as zip
         from medicare_national.bcarrier_claims_k  
union all
         select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.inpatient_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.inpatient_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.inpatient_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.inpatient_base_claims_k
union all
    select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.outpatient_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.outpatient_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.outpatient_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.outpatient_base_claims_k
union all
        select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hospice_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hospice_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hospice_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hospice_base_claims_k
union all
        select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.snf_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.snf_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.snf_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.snf_base_claims_k
union all
        select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hha_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hha_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hha_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_national.hha_base_claims_k
union all
        select  *
        from bcarrier_line
         ),
spec_st2 
    as  (
    select distinct npi as npi,
           max(specialty_cd) as specialty_cd,
           max(state) as state,
           max(zip) as zip
      from spec_st
  group by 1
    ) 
insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select 'mcrn' as data_source,
s.npi as provider_id_src,
null as provider_id_src_2,
s.npi as npi,
null as taxonomy1,
null as taxonomy2,
s.specialty_cd as spclty_cd1,
null as spclty_cd2,
null as provcat,
null as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
st.state as state,
s.zip as zip,
substring(s.zip,1,5) as zip_5
  from spec_st2 s
  left join data_warehouse.uth_provider d
   	on  s.npi = d.provider_id_src 
   	and d.data_source = 'mcrn'
  left join reference_tables.ref_medicare_state_codes st 
  	on st.medicare_state_cd = s.state
 where d.provider_id_src is null
     and s.npi is not null
 ;
 
raise notice 'load mcrn finished';


------------------------------------------------------------------------------------------
--Medicare Texas
------------------------------------------------------------------------------------------

----------------------------------------------------------
-------------provider table------------------------------
----------------------------------------------------------

raise notice 'load mcrt begin';

with bcarrier_line 
    as (
        select prf_physn_npi as npi,
        prvdr_spclty as specialty_cd,
        prvdr_state_cd as state,
        prvdr_zip as zip
        from medicare_texas.bcarrier_line_k  
    ),
    spec_st 
     as (
         select carr_clm_blg_npi_num as npi,
         null as specialty_cd,
         null as state,
         null as zip
         from medicare_texas.bcarrier_claims_k bck 
   union all
         select rfr_physn_npi as npi,
         null as specialty_cd,
         null as state,
         null as zip
         from medicare_texas.bcarrier_claims_k  
union all
         select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.inpatient_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.inpatient_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.inpatient_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.inpatient_base_claims_k
union all
    select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.outpatient_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.outpatient_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.outpatient_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.outpatient_base_claims_k
union all
        select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hospice_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hospice_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hospice_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hospice_base_claims_k
union all
        select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.snf_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.snf_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.snf_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.snf_base_claims_k
union all
        select at_physn_npi as npi,
           at_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hha_base_claims_k
    union all
    select op_physn_npi as npi,
           op_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hha_base_claims_k
    union all
    select ot_physn_npi as npi,
           ot_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hha_base_claims_k
    union all
    select rndrng_physn_npi as npi,
           rndrng_physn_spclty_cd as specialty_cd,
           prvdr_state_cd as state,
           null as zip
      from medicare_texas.hha_base_claims_k
union all
        select  *
        from bcarrier_line
         ),
spec_st2 
    as  (
    select distinct npi as npi,
           max(specialty_cd) as specialty_cd,
           max(state) as state,
           max(zip) as zip
      from spec_st
  group by 1
    ) 
insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select 'mcrt' as data_source,
s.npi as provider_id_src,
null as provider_id_src_2,
s.npi as npi,
null as taxonomy1,
null as taxonomy2,
s.specialty_cd as spclty_cd1,
null as spclty_cd2,
null as provcat,
null as provider_type,
null as address1,
null as address2,
null as address3,
null as city,
st.state as state,
s.zip as zip,
substring(s.zip,1,5) as zip_5
  from spec_st2 s
  left join data_warehouse.uth_provider d
   	on  s.npi = d.provider_id_src 
   	and d.data_source = 'mcrt'
  left join reference_tables.ref_medicare_state_codes st 
  	on st.medicare_state_cd = s.state
 where d.provider_id_src is null
     and s.npi is not null
 ;

raise notice 'load mcrt finished';

------------------------------------------------------------------------------------------
--Medicaid 
------------------------------------------------------------------------------

----------------eliminate old loads of same provider-------------------------------------
drop table if exists dev.ranked_medicaid_providers;

with ranking_prov 
     as (select *, 
                row_number() 
                  over( 
                    partition by base_tpi 
                    order by substring(table_update, 1, 9)::date desc) as 
                rank_prov 
         from   medicaid.prov) 
select * 
into   dev.ranked_medicaid_providers 
from   ranking_prov 
where  rank_prov = 1;  


-------------load from cleaned table--------------------------------

insert into data_warehouse.uth_provider (
data_source,
provider_id_src,
provider_id_src_2,
npi,
taxonomy1,
taxonomy2,
spclty_cd1,
spclty_cd2,
provcat,
provider_type,
address1,
address2,
address3,
city,
state,
zip,
zip_5
)
select 
'mdcd' as data_source,
s.base_tpi as provider_id_src,
null as provider_id_src_2,
s.npi as npi,
s.primary_taxonomy as taxonomy1,
null as taxonomy2,
s.specialty as spclty_cd1,
spclty_cd2,
null as provcat,
s.provider_type as provider_type,
s.lbl_address as address1,
null as address2,
null as address3,
s.phys_city as city,
s.lbl_state as state,
s.lbl_zip as zip,
s.lbl_zip as zip_5
from   dev.ranked_medicaid_providers s  
       left join data_warehouse.uth_provider d2 
              on d2.provider_id_src = s.base_tpi 
              and d2.data_source = 'mdcd'
where d2.uth_provider_id is null 
and s.base_tpi is not null    ;

   
update data_warehouse.uth_provider
set npi = null where npi = '';


raise notice 'load mdcd finished';

raise notice 'analyze uth_provider';

analyze data_warehouse.uth_provider;

alter function dw_staging.load_uth_provider() owner to uthealth_dev;
grant all on function dw_staging.load_uth_provider() to uthealth_dev;

raise notice 'ownership transferred to uthealth_dev';
raise notice 'end script';

  
end $$
;





