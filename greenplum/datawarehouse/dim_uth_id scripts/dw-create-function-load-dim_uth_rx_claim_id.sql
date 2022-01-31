/* ******************************************************************************************************
 *  This table is used to generate a de-identified pharmacy claim id that will be used to populate pharmacy_claims
 *	The uth_rx_claim_id column will be a sequence that is initially set to a 100,000,000
 *  This code can be re-run as new data comes in, logic is in place to prevent duplicate entries into table
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 8/31/2021 ||comments added
 * ******************************************************************************************************
 *  wc002  || 9/20/2021 || medicaid added
 * ******************************************************************************************************
 *  jw001  || 11/12/2021 || wrap in function
 * ****************************************************************************************************** */

select dw_staging.load_dim_uth_rx_claim_id();

select count(*), data_source
from data_warehouse.dim_uth_rx_claim_id 
group by data_source
order by data_source 
;



CREATE OR REPLACE FUNCTION dw_staging.load_dim_uth_rx_claim_id()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$


begin

	
raise notice 'begin script';
raise notice 'load truven com begin';


---truven commercial
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truv'
      ,a.year 
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.member_id_src || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.member_id_src
from truven.ccaed  a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.member_id_src
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'truv'
 and c.member_id_src = a.member_id_src
 and c.rx_claim_id_src = a.member_id_src || ndcnum::text || svcdate::text
where c.uth_rx_claim_id is null 
  and a.member_id_src is not null 
;


raise notice 'load truven com finished';
raise notice 'load truven mdcr begin';


--truven medicare
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src )					
select 'truv'
      ,a.year 
	  ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	  ,a.member_id_src || ndcnum::text || svcdate::text
	  ,b.uth_member_id	  
      ,a.member_id_src
from truven.mdcrd  a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and b.member_id_src = a.member_id_src
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'truv'
 and c.member_id_src = a.member_id_src
 and c.rx_claim_id_src = a.member_id_src || ndcnum::text || svcdate::text
where c.uth_rx_claim_id is null 
  and a.member_id_src is not null 
;


raise notice 'load mdcr finished';
raise notice 'load mcrt begin';

--medicare texas 
with uthealth_medicare_national_cte as (  
    select distinct on (pde_id) 
        year, pde_id, bene_id 
    from uthealth.medicare_national.pde_file
    )
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 					
select 'mcrt'
       ,a.year::int
       ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	   ,a.pde_id
	   ,b.uth_member_id
	   ,a.bene_id 
from uthealth_medicare_national_cte a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = a.bene_id
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'mcrt'
 and c.member_id_src = a.bene_id 
 and c.rx_claim_id_src = a.pde_id
where c.uth_rx_claim_id is null 
  and a.bene_id is not null
 ;

raise notice 'load mcrt finished';
raise notice 'load mcrn begin';

---Medicare National
with medicare_cte as (  
    select distinct on (pde_id) 
        year, pde_id, bene_id 
    from medicare_national.pde_file
    )
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 					
select 'mcrn'
       ,a.year::int
       ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	   ,a.pde_id
	   ,b.uth_member_id
	   ,a.bene_id 
from medicare_cte a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = a.bene_id
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'mcrn'
 and c.member_id_src = a.bene_id 
 and c.rx_claim_id_src = a.pde_id
where c.uth_rx_claim_id is null 
  and a.bene_id is not null
 ;

raise notice 'load mcrn finished';
raise notice 'load optd begin';


--optum dod   12min
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
with cte_distinct_rx as
     (
	select distinct 
	          'optd' as v_data_source
		      ,a.year as v_data_year 
		      ,a.clmid as v_rx_claim_id_src
		      ,b.uth_member_id as v_uth_member_id
		      ,a.member_id_src as v_member_id_src
	from optum_dod.rx a
      join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'optd'
		   and b.member_id_src = a.member_id_src 
	  )
select v_data_source, 
       v_data_year, 
       v_rx_claim_id_src, 
       v_uth_member_id, 
       v_member_id_src
from cte_distinct_rx a 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = v_data_source
 and c.member_id_src = v_member_id_src
 and c.rx_claim_id_src = v_rx_claim_id_src
 and c."year" = v_data_year
where c.uth_rx_claim_id is null 
 ;

raise notice 'load optd finished';
raise notice 'load optz begin';

--optum zip 
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
with cte_distinct_rx as
     (
	select distinct 
	          'optz' as v_data_source
		      ,a.year as v_data_year 
		      ,a.clmid as v_rx_claim_id_src
		      ,b.uth_member_id as v_uth_member_id
		      ,a.member_id_src as v_member_id_src
	from optum_zip.rx a
      join data_warehouse.dim_uth_member_id b 
		    on b.data_source = 'optz'
		   and b.member_id_src = a.member_id_src 
	  )
select v_data_source, 
       v_data_year, 
       v_rx_claim_id_src, 
       v_uth_member_id, 
       v_member_id_src
from cte_distinct_rx a 
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = v_data_source
 and c.member_id_src = v_member_id_src
 and c.rx_claim_id_src = v_rx_claim_id_src
 and c."year" = v_data_year
where c.uth_rx_claim_id is null 
 ;


raise notice 'load optz finished';
analyze data_warehouse.dim_uth_rx_claim_id;


--*****Medicaid*****  wcc002 
raise notice 'load mdcd chip begin';

---chip rx 
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 				
select 'mdcd', 
       a.year_fy,
       nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       pcn || ndc || replace(rx_fill_dt::text, '-',''),
       b.uth_member_id, 
       a.pcn
from medicaid.chip_rx a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = pcn
   and b.data_source = 'mdcd' 
  left outer join data_warehouse.dim_uth_rx_claim_id c 
    on c.member_id_src = a.pcn 
   and c.rx_claim_id_src = pcn || ndc || replace(rx_fill_dt::text, '-','')
   and c."year" = a.year_fy 
   and c.data_source = 'mdcd' 
where c.uth_rx_claim_id is null 
;


raise notice 'load mdcd chip finished';
raise notice 'load mdcd ffs begin';



--medicaid ffs rx   
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'mdcd', 
       a.year_fy,
       nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       pcn || ndc || replace(rx_fill_dt::text, '-',''),
       b.uth_member_id, 
       a.pcn
from medicaid.ffs_rx a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = pcn
   and b.data_source = 'mdcd' 
  left outer join data_warehouse.dim_uth_rx_claim_id c 
    on c.member_id_src = a.pcn 
   and c.rx_claim_id_src = pcn || ndc || replace(rx_fill_dt::text, '-','')
   and c."year" = a.year_fy 
   and c.data_source = 'mdcd' 
where c.uth_rx_claim_id is null 
;

raise notice 'load mdcd ffs finished';
raise notice 'load mdcd mco begin';

--medicaid mco rx   
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 	
select 'mdcd', 
       a.year_fy,
       nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq'),
       pcn || ndc || replace(rx_fill_dt::text, '-',''),
       b.uth_member_id, 
       a.pcn
from medicaid.mco_rx a
  join data_warehouse.dim_uth_member_id b  
    on b.member_id_src = pcn
   and b.data_source = 'mdcd' 
  left outer join data_warehouse.dim_uth_rx_claim_id c 
    on c.member_id_src = a.pcn 
   and c.rx_claim_id_src = pcn || ndc || replace(rx_fill_dt::text, '-','')
   and c."year" = a.year_fy 
   and c.data_source = 'mdcd' 
where c.uth_rx_claim_id is null 
;

raise notice 'load mdcd mco finished';

analyze data_warehouse.dim_uth_rx_claim_id;

alter function dw_staging.load_dim_uth_rx_claim_id() owner to uthealth_dev;
grant all on function dw_staging.load_dim_uth_rx_claim_id() to uthealth_dev;

raise notice 'ownership transferred to uthealth_dev';
raise notice 'end script';

end $$
;



