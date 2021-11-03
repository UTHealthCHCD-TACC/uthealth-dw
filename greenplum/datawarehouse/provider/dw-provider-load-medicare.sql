/*
------------------------------------------------------------------------------------------
--Medicare National 
------------------------------------------------------------------------------------------
7/26/2021:      
       Leaving in non-numeric NPI's for now.... We may take out later, but not yet
       Not loading dme claims in
       Base claims are put in as line level '1' 
       Bcarrier base claims are put in at 0 and then line level for that claim starts at 1
------------------------------------------------------------------------------------------
*/

----------------------------------------------------------
-------------provider table------------------------------
----------------------------------------------------------
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
insert into dev.provider (
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
  left join dev.provider d
   	on  s.npi = d.provider_id_src 
   	and d.data_source = 'mcrn'
  left join reference_tables.ref_medicare_state_codes st 
  	on st.medicare_state_cd = s.state
 where d.provider_id_src is null
     and s.npi is not null
 ;
 
vacuum analyze dev.provider;

select * from dev.provider ;



------------------------------------------------------------------------------------------
--Medicare Texas
------------------------------------------------------------------------------------------

----------------------------------------------------------
-------------provider table------------------------------
----------------------------------------------------------

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
insert into dev.provider (
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
  left join dev.provider d
   	on  s.npi = d.provider_id_src 
   	and d.data_source = 'mcrt'
  left join reference_tables.ref_medicare_state_codes st 
  	on st.medicare_state_cd = s.state
 where d.provider_id_src is null
     and s.npi is not null
 ;
 
vacuum analyze dev.provider;


