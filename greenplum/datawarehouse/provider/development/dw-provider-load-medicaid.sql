/*


--@@ -0,0 +1,132 @@
------------------------------------------------------------------------------------------
--Medicaid providers
------------------------------------------------------------------------------------------
jw001 - create script 
            notes: for now we are leaving out chip we will ask trudy about chip providers without base_tpi when she gets back from vacation
------------------------------------------------------------------------------------------

*/



drop table if exists dev.jw_dim_uth_provider_id;

CREATE TABLE dev.jw_dim_uth_provider_id (
    uth_provider_id bigserial NOT NULL,
    data_source bpchar(4) NULL,
    provider_id_src text NOT NULL
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (uth_provider_id);

alter sequence dev.jw_dim_uth_provider_id_uth_provider_id_seq restart with 100000000;

alter sequence dev.jw_dim_uth_provider_id_uth_provider_id_seq cache 200;

table dev.jw_dim_uth_provider_id;





-----------------------------------------------------------------------------------------------------------
-------Make uth ids --add records to dim_uth_provider_id
-----------------------------------------------------------------------------------------------------------
-- has no provider_id aside from npi to generate uth id, so using that as both provider_id_src and npi ... 
----------------------------------------------------------------------------------------------------------------------------------



insert into dev.jw_dim_uth_provider_id (data_source, provider_id_src)
select distinct 'mdcd', base_tpi as provider_id_src
from    medicaid.prov p
  left  join dev.jw_dim_uth_provider_id d 
    on  d.provider_id_src = p.base_tpi 
   and  d.data_source = 'mdcd' 
 where  d.uth_provider_id is null
   and  p.base_tpi is not null;
   
--delete from dev.jw_dim_uth_provider_id;
   
table dev.jw_dim_uth_provider_id;

vacuum analyze dev.jw_dim_uth_provider_id;


   
----------------------------
---provider table ----
----------------------------

   
drop table if exists dev.jw_providers;

create table dev.jw_providers
with(appendonly=true,orientation=column)
as select * from data_warehouse.provider limit 0
distributed by (provider_id_src);

table dev.jw_providers;

vacuum analyze dev.jw_providers;
   


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

vacuum analyze dev.ranked_medicaid_providers;


-------------load from cleaned table--------------------------------
insert into dev.jw_providers 
            (data_source, 
             uth_provider_id, 
             provider_id_src, 
             address1,
             city,
             npi, 
             spclty_cd1, 
             state, 
             zip, 
             zip_3
             --, 
             --provider_type
             )
select d.data_source, 
       d.uth_provider_id, 
       d.provider_id_src, 
       s.phys_address,
       s.phys_city, 
       s.npi          as npi, 
       s.specialty, 
       s.phys_state, 
       s.phys_zip, 
       substring(s.phys_zip, 1, 3) 
       --,s.provider_type 
from   dev.jw_dim_uth_provider_id d 
       left join dev.ranked_medicaid_providers s 
              on d.provider_id_src = s.base_tpi 
       left join dev.jw_providers d2 
              on d2.provider_id_src = d.provider_id_src 
where  d.data_source = 'mdcd' 
       and d2.provider_id_src is null;     

   
update dev.jw_providers
set npi = null where npi = '';


table dev.jw_providers;
-------------------------

drop table if exists dev.jw_providers_claim;

create table dev.jw_providers_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_provider limit 0
distributed by (uth_claim_id);

table dev.jw_providers_claim;

vacuum analyze dev.jw_providers_claim;  
   
   
-----------------------------------
insert into dev.jw_providers_claim   
(
data_source,
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
perf_op_provider
)
select 
'mdcd' as data_source,
extract(year from a.from_dos) as data_year,
c.uth_member_id as uth_member_id,
c.uth_claim_id as uth_claim_id ,
a.clm_dtl_nbr as claim_sequence_number,
a.from_dos as from_date_of_service,
bill_prov.uth_provider_id as bill_provider,
ref_prov.uth_provider_id as ref_provider,
null as other_provider,
perf_prov.uth_provider_id as perf_rn_provider,
null as  perf_at_provider,
null as  perf_op_provider
from medicaid.clm_header d 
    join medicaid.clm_detail a
      on d.icn = a.icn 
     and d.year_fy = a.year_fy 
    join data_warehouse.dim_uth_claim_id c 
      on c.claim_id_src = a.icn 
left outer join dev.jw_dim_uth_provider_id bill_prov on bill_prov.provider_id_src = d.bill_prov_id   
left outer join dev.jw_providers ref_prov on ref_prov.npi = a.ref_prov_npi 
left outer join dev.jw_dim_uth_provider_id perf_prov on perf_prov.provider_id_src = a.perf_prov_id;

vacuum analyze dev.jw_providers_claim;   

table dev.jw_providers_claim;

