/*


--@@ -0,0 +1,132 @@
------------------------------------------------------------------------------------------
--Medicaid providers
------------------------------------------------------------------------------------------
jw001 - create script 
            notes: for now we are leaving out chip we will ask trudy about chip providers without base_tpi when she gets back from vacation
------------------------------------------------------------------------------------------

*/




-----------------------------------------------------------------------------------------------------------
-------
-----------------------------------------------------------------------------------------------------------
-- 
----------------------------------------------------------------------------------------------------------------------------------

   
----------------------------
---provider table ----
----------------------------


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

insert into data_warehouse.provider (
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
       left join data_warehouse.provider d2 
              on d2.provider_id_src = s.base_tpi 
              and d2.data_source = 'mdcd'
where d2.uth_provider_id is null 
and s.base_tpi is not null    ;

   
update data_warehouse.provider
set npi = null where npi = '';