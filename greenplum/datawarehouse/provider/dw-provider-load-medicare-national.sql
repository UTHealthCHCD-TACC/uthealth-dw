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
insert into data_warehouse.provider 
(
            data_source, uth_provider_id, provider_id_src,
            npi, spclty_cd1, state, zip
)
    select d.data_source, d.uth_provider_id, d.provider_id_src,
           s.npi as npi, s.specialty_cd, st.state_cd, s.zip
      from data_warehouse.dim_uth_provider_id d
      left join spec_st2 s
        on d.provider_id_src  = s.npi
      left join data_warehouse.provider d2
       on  d2.provider_id_src = d.provider_id_src
      left join reference_tables.ref_medicare_state_codes st on st.medicare_state_cd = s.state
     where d.data_source = 'mcrn'
       and d2.provider_id_src is null
   ;
 
vacuum analyze data_warehouse.provider;


--------------------------------------------------------------------------
-----------claim details
-------------------------------------------------


 -----------inpatient_base_claims_k-----------------
 -----------whole claim one line so using 1--------
 

insert into data_warehouse.claim_provider
      (data_source,
       data_year,
       uth_member_id,
       uth_claim_id,
       claim_sequence_number,
       from_date_of_service,
       bill_provider,
       ref_provider,
       other_provider,
       perf_rn_provider,
       perf_at_provider,
       perf_op_provider)
select 'mcrn' as data_source,
       a."year"::int as data_year,
       c.uth_member_id as uth_member_id,
       c.uth_claim_id as uth_claim_id,
       '1' as claim_sequence_number,
       a.clm_from_dt::date as from_date_of_service,
       bill_prov.uth_provider_id as bill_provider,
       null as ref_provider,
       other_prov.uth_provider_id as other_provider,
       rn_prov.uth_provider_id as perf_rn_provider,
       at_prov.uth_provider_id as perf_at_provider,
       op_prov.uth_provider_id as perf_op_provider
  from medicare_national.inpatient_base_claims_k a
  join data_warehouse.dim_uth_claim_id c
    on c.claim_id_src  = a.clm_id
left outer join data_warehouse.dim_uth_provider_id other_prov on other_prov.provider_id_src = a.ot_physn_npi 
left outer join data_warehouse.dim_uth_provider_id rn_prov on rn_prov.provider_id_src = a.rndrng_physn_npi
left outer join data_warehouse.dim_uth_provider_id at_prov on at_prov.provider_id_src = a.at_physn_npi
left outer join data_warehouse.dim_uth_provider_id op_prov on op_prov.provider_id_src = a.op_physn_npi 
left outer join data_warehouse.dim_uth_provider_id bill_prov on bill_prov.provider_id_src = a.org_npi_num
where c.data_source = 'mcrn';


 -----------outpatient_base_claims_k-----------------
 -----------whole claim one line so using 1--------
insert into data_warehouse.claim_provider
      (data_source,
       data_year,
       uth_member_id,
       uth_claim_id,
       claim_sequence_number,
       from_date_of_service,
       bill_provider,
       ref_provider,
       other_provider,
       perf_rn_provider,
       perf_at_provider,
       perf_op_provider)
select 'mcrn' as data_source,
       a."year"::int as data_year,
       c.uth_member_id as uth_member_id,
       c.uth_claim_id as uth_claim_id,
       '1' as claim_sequence_number,
       a.clm_from_dt::date as from_date_of_service,
       bill_prov.uth_provider_id as bill_provider,
       null as ref_provider,
       other_prov.uth_provider_id as other_provider,
       rn_prov.uth_provider_id as perf_rn_provider,
       at_prov.uth_provider_id as perf_at_provider,
       op_prov.uth_provider_id as perf_op_provider
  from medicare_national.outpatient_base_claims_k a
  join data_warehouse.dim_uth_claim_id c
    on c.claim_id_src  = a.clm_id
left outer join data_warehouse.dim_uth_provider_id other_prov on other_prov.provider_id_src = a.ot_physn_npi 
left outer join data_warehouse.dim_uth_provider_id rn_prov on rn_prov.provider_id_src = a.rndrng_physn_npi
left outer join data_warehouse.dim_uth_provider_id at_prov on at_prov.provider_id_src = a.at_physn_npi
left outer join data_warehouse.dim_uth_provider_id op_prov on op_prov.provider_id_src = a.op_physn_npi 
left outer join data_warehouse.dim_uth_provider_id bill_prov on bill_prov.provider_id_src = a.org_npi_num
left outer join data_warehouse.claim_provider clmprov on clmprov.uth_claim_id = c.uth_claim_id 
where c.data_source = 'mcrn'
and clmprov.uth_claim_id is null;


 -----------hospice_base_claims_k-----------------
 -----------whole claim one line so using 1--------
insert into data_warehouse.claim_provider
      (data_source,
       data_year,
       uth_member_id,
       uth_claim_id,
       claim_sequence_number,
       from_date_of_service,
       bill_provider,
       ref_provider,
       other_provider,
       perf_rn_provider,
       perf_at_provider,
       perf_op_provider)
select 'mcrn' as data_source,
       a."year"::int as data_year,
       c.uth_member_id as uth_member_id,
       c.uth_claim_id as uth_claim_id,
       '1' as claim_sequence_number,
       a.clm_from_dt::date as from_date_of_service,
       bill_prov.uth_provider_id as bill_provider,
       null as ref_provider,
       other_prov.uth_provider_id as other_provider,
       rn_prov.uth_provider_id as perf_rn_provider,
       at_prov.uth_provider_id as perf_at_provider,
       op_prov.uth_provider_id as perf_op_provider
  from medicare_national.hospice_base_claims_k a
  join data_warehouse.dim_uth_claim_id c
    on c.claim_id_src  = a.clm_id
left outer join data_warehouse.dim_uth_provider_id other_prov on other_prov.provider_id_src = a.ot_physn_npi 
left outer join data_warehouse.dim_uth_provider_id rn_prov on rn_prov.provider_id_src = a.rndrng_physn_npi
left outer join data_warehouse.dim_uth_provider_id at_prov on at_prov.provider_id_src = a.at_physn_npi
left outer join data_warehouse.dim_uth_provider_id op_prov on op_prov.provider_id_src = a.op_physn_npi 
left outer join data_warehouse.dim_uth_provider_id bill_prov on bill_prov.provider_id_src = a.org_npi_num
left outer join data_warehouse.claim_provider clmprov on clmprov.uth_claim_id = c.uth_claim_id 
where c.data_source = 'mcrn'
and clmprov.uth_claim_id is null;


 -----------snf_base_claims_k-----------------
 -----------whole claim one line so using 1--------
insert into data_warehouse.claim_provider
      (data_source,
       data_year,
       uth_member_id,
       uth_claim_id,
       claim_sequence_number,
       from_date_of_service,
       bill_provider,
       ref_provider,
       other_provider,
       perf_rn_provider,
       perf_at_provider,
       perf_op_provider)
select 'mcrn' as data_source,
       a."year"::int as data_year,
       c.uth_member_id as uth_member_id,
       c.uth_claim_id as uth_claim_id,
       '1' as claim_sequence_number,
       a.clm_from_dt::date as from_date_of_service,
       bill_prov.uth_provider_id as bill_provider,
       null as ref_provider,
       other_prov.uth_provider_id as other_provider,
       rn_prov.uth_provider_id as perf_rn_provider,
       at_prov.uth_provider_id as perf_at_provider,
       op_prov.uth_provider_id as perf_op_provider
  from medicare_national.snf_base_claims_k a
  join data_warehouse.dim_uth_claim_id c
    on c.claim_id_src  = a.clm_id
left outer join data_warehouse.dim_uth_provider_id other_prov on other_prov.provider_id_src = a.ot_physn_npi 
left outer join data_warehouse.dim_uth_provider_id rn_prov on rn_prov.provider_id_src = a.rndrng_physn_npi
left outer join data_warehouse.dim_uth_provider_id at_prov on at_prov.provider_id_src = a.at_physn_npi
left outer join data_warehouse.dim_uth_provider_id op_prov on op_prov.provider_id_src = a.op_physn_npi 
left outer join data_warehouse.dim_uth_provider_id bill_prov on bill_prov.provider_id_src = a.org_npi_num
left outer join data_warehouse.claim_providerclmprov on clmprov.uth_claim_id = c.uth_claim_id 
where c.data_source = 'mcrn'
and clmprov.uth_claim_id is null;


 -----------hha_base_claims_k-----------------
 -----------whole claim one line so using 1 as claim line number--------


insert into data_warehouse.claim_provider
      (data_source,
       data_year,
       uth_member_id,
       uth_claim_id,
       claim_sequence_number,
       from_date_of_service,
       bill_provider,
       ref_provider,
       other_provider,
       perf_rn_provider,
       perf_at_provider,
       perf_op_provider)
select 'mcrn' as data_source,
       a."year"::int as data_year,
       c.uth_member_id as uth_member_id,
       c.uth_claim_id as uth_claim_id,
       '1' as claim_sequence_number,
       a.clm_from_dt::date as from_date_of_service,
       bill_prov.uth_provider_id as bill_provider,
       null as ref_provider,
       other_prov.uth_provider_id as other_provider,
       rn_prov.uth_provider_id as perf_rn_provider,
       at_prov.uth_provider_id as perf_at_provider,
       op_prov.uth_provider_id as perf_op_provider
  from medicare_national.hha_base_claims_k a
  join data_warehouse.dim_uth_claim_id c
    on c.claim_id_src  = a.clm_id
left outer join data_warehouse.dim_uth_provider_id other_prov on other_prov.provider_id_src = a.ot_physn_npi 
left outer join data_warehouse.dim_uth_provider_id rn_prov on rn_prov.provider_id_src = a.rndrng_physn_npi
left outer join data_warehouse.dim_uth_provider_id at_prov on at_prov.provider_id_src = a.at_physn_npi
left outer join data_warehouse.dim_uth_provider_id op_prov on op_prov.provider_id_src = a.op_physn_npi 
left outer join data_warehouse.dim_uth_provider_id bill_prov on bill_prov.provider_id_src = a.org_npi_num
left outer join data_warehouse.claim_provider clmprov on clmprov.uth_claim_id = c.uth_claim_id 
where c.data_source = 'mcrn'
and clmprov.uth_claim_id is null;


 -----------bcarrier_claims_k-----------------
 -----------bcarrier does have lines, so using 0 for single claim before line level data--------
insert into data_warehouse.claim_provider
      (data_source,
       data_year,
       uth_member_id,
       uth_claim_id,
       claim_sequence_number,
       from_date_of_service,
       bill_provider,
       ref_provider,
       other_provider,
       perf_rn_provider,
       perf_at_provider,
       perf_op_provider)
select 'mcrn' as data_source,
       a."year"::int as data_year,
       c.uth_member_id as uth_member_id,
       c.uth_claim_id as uth_claim_id,
       '0' as claim_sequence_number,
       a.clm_from_dt::date as from_date_of_service,
       bill_prov.uth_provider_id as bill_provider,
       refer_prov.uth_provider_id as ref_provider,
       null as other_provider,
       null as perf_rn_provider,
       null as perf_at_provider,
       null as perf_op_provider
  from medicare_national.bcarrier_claims_k a
  join data_warehouse.dim_uth_claim_id c
    on c.claim_id_src  = a.clm_id
left outer join data_warehouse.dim_uth_provider_id bill_prov on bill_prov.provider_id_src = a.carr_clm_blg_npi_num
left outer join data_warehouse.dim_uth_provider_id refer_prov on refer_prov.provider_id_src = a.rfr_physn_npi
left outer join data_warehouse.claim_provider clmprov on clmprov.uth_claim_id = c.uth_claim_id 
where c.data_source = 'mcrn'
and clmprov.uth_claim_id is null;


-------------------------------------------------------------------------
-----------bcarrier_line_k is only one with actual sequence numbers------
-------------------------------------------------------------------------
insert into data_warehouse.claim_provider
      (data_source,
       data_year,
       uth_member_id,
       uth_claim_id,
       claim_sequence_number,
       from_date_of_service,
       bill_provider,
       ref_provider,
       other_provider,
       perf_rn_provider,
       perf_at_provider,
       perf_op_provider)
select 'mcrn' as data_source,
       a."year"::int as data_year,
       c.uth_member_id as uth_member_id,
       c.uth_claim_id as uth_claim_id,
       a.line_num as claim_sequence_number,
       a.line_1st_expns_dt::date as from_date_of_service,
       b.uth_provider_id as bill_provider,
       null as ref_provider,
       null as other_provider,
       null as perf_rn_provider,
       null as perf_at_provider,
       null as perf_op_provider
  from medicare_national.bcarrier_line_k a
  join data_warehouse.dim_uth_claim_id c
    on c.claim_id_src  = a.clm_id
  left join data_warehouse.dim_uth_provider_id b
    on a.prf_physn_npi = b.provider_id_src
 where c.data_source = 'mcrn';
          

   
vacuum analyze data_warehouse.claim_provider;



   
  /* 
select count(*) from data_warehouse.provider where data_source = 'mcrn';

select count(*) from data_warehouse.dim_uth_provider_id where data_source = 'mcrn';
   
select count(*) from data_warehouse.claim_providerwhere data_source = 'mcrn';

select 
    count(*) as count,
    'bcarrier_claims_k' as src_table
    from 
    medicare_national.bcarrier_claims_k
union all
    select 
    count(*) as count,
    'bcarrier_line_k' as src_table
    from 
    medicare_national.bcarrier_line_k 
union all
    select 
    count(*) as count,
    'hha_base_claims_k' as src_table
    from 
    medicare_national.hha_base_claims_k 
union all 
    select 
    count(*) as count,
    'hospice_base_claims_k' as src_table
    from 
    medicare_national.hospice_base_claims_k 
union all 
    select 
    count(*) as count,
    'inpatient_base_claims_k' as src_table
    from 
    medicare_national.inpatient_base_claims_k 
union all 
    select 
    count(*) as count,
    'outpatient_base_claims_k' as src_table
    from 
    medicare_national.outpatient_base_claims_k 
union all 
    select 
    count(*) as count,
    'snf_base_claims_k' as src_table
    from 
    medicare_national.snf_base_claims_k 
    ;   
   


select count(distinct id) 
from (
    select bill_provider as id
    from data_warehouse.claim_provider
    union all 
    select ref_provider as id
    from data_warehouse.claim_provider
    union all 
    select other_provider as id
    from data_warehouse.claim_provider
    union all 
    select perf_rn_provider as id
    from data_warehouse.claim_provider
    union all 
    select perf_at_provider as id
    from data_warehouse.claim_provider
    union all 
    select perf_op_provider as id
    from data_warehouse.claim_provider
) a ;



select count(uth_provider_id) from data_warehouse.provider;


select count(uth_provider_id) from data_warehouse.dim_uth_provider_id;







   */










