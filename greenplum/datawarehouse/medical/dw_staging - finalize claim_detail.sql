/* ******************************************************************************************************
 *  The script should be run as the final step for ALL data sources 
 *  once dw_staging.claim_detail has been updated. It will populate claim sequence and then 
 *  distribute claim_detail on uth_member_id.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 10/12/2021 || script created 
 * ******************************************************************************************************
 * 
*/

---confirm all seq nums are populated.  if not then a filler value needs to be inserted, maybe 999?
select count(*) from dw_staging.claim_detail cd where claim_sequence_number_src is null;


---- get row id of duplicate rows
drop table if exists  dev.temp_dupe_claim_detail_rows;

create table dev.temp_dupe_claim_detail_rows
with (appendonly=true, orientation=column) as 
	select row_id::bigint as row_id 
	from
	(		
		select row_number() over(partition by uth_member_id, uth_claim_id, claim_sequence_number_src, from_date_of_service) as rn
			      ,*
			from dw_staging.claim_detail 			
	) sub
	where rn > 1
distributed by (row_id);	

--remote dupe records so only one per member per month - runtime: 12min
delete from dw_staging.claim_detail a 
  using dev.temp_dupe_claim_detail_rows b 
   where a.row_id = b.row_id 
; 



---update claim sequence  
 drop table if exists dev.claim_seq_build ;

--- 60min
create table dev.claim_seq_build 
with (appendonly=true, orientation=column, compresstype=zlib) as 
select uth_claim_id,
       uth_member_id,
       claim_sequence_number_src,
       from_date_of_service,
       row_id,
       rank() over ( partition by uth_claim_id
                           order by claim_sequence_number_src::numeric, from_date_of_service
                          ) as rownum 
       from dw_staging.claim_detail
distributed by (row_id);
       		     
	
vacuum analyze dev.claim_seq_build ;
		
select count(*) from dev.claim_seq_build;

select count(*) from dw_staging.claim_detail;


--update claim sequence 58min
update dw_staging.claim_detail a set claim_sequence_number = rownum       
from dev.claim_seq_build b 
where a.row_id = b.row_id
;
		  

---finalize
vacuum analyze dw_staging.claim_detail;

--validate, should be 0 
select count(*) from dw_staging.claim_detail cd where claim_sequence_number is null;

--spot check
select * from dw_staging.claim_detail cd where data_source = 'mcrt';

select * from dw_staging.claim_detail cd where uth_claim_id = 26080305188;

---cleanup work tables
drop table if exists dev.claim_seq_build;

drop table if exists dev.temp_dupe_claim_detail_rows;


---****redistribute on uth_member_id

create table dw_staging.claim_detail_temp 
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5) as 
select * from dw_staging.claim_detail 
distributed by (uth_member_id)
;

---replace 
drop table dw_staging.claim_detail;

alter table dw_staging.claim_detail_temp rename to claim_detail;

alter table dw_staging.claim_detail drop column row_id;

vacuum analyze dw_staging.claim_detail;

select * from dw_staging.claim_detail;

------------------


------------- END SCRIPT 

