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

do $$ 

begin 
	
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

raise notice 'dupe working table';

--remote dupe records so only one per member per month - runtime: 12min
delete from dw_staging.claim_detail a 
  using dev.temp_dupe_claim_detail_rows b 
   where a.row_id = b.row_id 
; 

raise notice 'dupes removed';

end $$ 
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
       		     
	
analyze dev.claim_seq_build ;
		
raise notice 'clm seq working table';

--update claim sequence 58min
update dw_staging.claim_detail a set claim_sequence_number = rownum       
from dev.claim_seq_build b 
where a.row_id = b.row_id
;		 

raise notice 'clm seq updated';

---****redistribute on uth_member_id

create table dw_staging.claim_detail_temp 
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5) as 
select * from dw_staging.claim_detail 
distributed by (uth_member_id)
;

analyze dw_staging.claim_detail_temp;

select count(*), data_source from dw_staging.claim_detail_temp cdt group by data_source ;

raise notice 'redistributing claim detail';

---replace 
drop table dw_staging.claim_detail;

alter table dw_staging.claim_detail_temp rename to claim_detail;

alter table dw_staging.claim_detail drop column row_id;

analyze dw_staging.claim_detail;

alter table dw_staging.claim_detail  owner to uthealth_dev;

raise notice 'done';

end $$
;

------------- END SCRIPT 

select * 
from ( 
select count(*) cnt, row_id 
from dw_staging.claim_detail 
group by row_id 
) inr where cnt > 1 
;

