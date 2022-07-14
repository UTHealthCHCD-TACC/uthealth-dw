/* ******************************************************************************************************
 *  The script should be run as the final step for ALL data sources 
 *  once dw_staging.claim_detail has been updated. It will populate claim sequence and then 
 *  distribute claim_detail on uth_member_id.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 10/12/2021 || script created 
 * ******************************************************************************************************
 *  wc004  || 01/26/2022 || add partitions
 *  ****************************************************************************************************** 
*/


--37min
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


select * 
from dw_Staging.claim_detail



do $$ 

begin 
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
(like dw_staging.claim_detail including defaults) 
with (
		appendonly=true, 
		orientation=column, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;

insert into dw_staging.claim_detail_temp 
select * 
from dw_staging.claim_detail
;


drop table if exists dev.claim_seq_build;

raise notice 'redistributing claim detail';

---replace 
drop table dw_staging.claim_detail cascade;

alter table dw_staging.claim_detail_temp rename to claim_detail;

alter table dw_staging.claim_detail drop column row_id;

analyze dw_staging.claim_detail;

alter table dw_staging.claim_detail  owner to uthealth_dev;

raise notice 'done';

end $$
;

------------- END MAIN SCRIPT  CONTINUE FOR TRUVEN BILLTYPE ONLY IF UPDATING TRUVEN DATA  

select * 
from ( 
select count(*) cnt, row_id 
from dw_staging.claim_detail 
group by row_id 
) inr where cnt > 1 
;

do $$ 

begin 

drop table if exists dev.wc_truv_billtype;

create table dev.wc_truv_billtype 
with (appendonly = true, orientation=column, compresstype = zlib) as 
select distinct * from (
	select substring(f.billtyp,1,1) as billtypeinst,
	       substring(f.billtyp,2,1) as billtypeclass, 
	       substring(f.billtyp,3,1) as billtypefreq, 
	       c.uth_member_id,
	       c.uth_claim_id, 
	       rank() over ( partition by uth_claim_id
	                           order by seqnum, svcdate
	                      ) as seq_num
	from truven.mdcrf f 
	   join dev.truven_dim_uth_claim_id c
	     on c.member_id_src = f.member_id_src 
	    and c.claim_id_src = f.msclmid::text
	    and c.data_year = f."year" 
	union 
	select substring(f.billtyp,1,1) as billtypeinst,
	       substring(f.billtyp,2,1) as billtypeclass, 
	       substring(f.billtyp,3,1) as billtypefreq, 
	       c.uth_member_id,
	       c.uth_claim_id, 
	       rank() over ( partition by uth_claim_id
	                           order by seqnum, svcdate 
	                      ) as seq_num
	from truven.ccaef f 
	   join dev.truven_dim_uth_claim_id c
	     on c.member_id_src = f.member_id_src 
	    and c.claim_id_src = f.msclmid::text
	    and c.data_year = f."year"
) inr 
distributed by (uth_member_id)
;

raise notice 'billtype created'; 

analyze  dev.wc_truv_billtype;

create table dev.wc_truv_billtype_dedupe 
with (appendonly = true, orientation=column, compresstype = zlib) 
as  ---- joe: added cte to get rid of duplicates
		select max(billtypeinst) as billtypeinst,
		 max(billtypeclass) as billtypeclass,
		 max(billtypefreq) as billtypefreq,
		uth_member_id, uth_claim_id, seq_num
		from dev.wc_truv_billtype a
		group by uth_member_id, uth_claim_id, seq_num
distributed by (uth_member_id);

--4minutes
update dw_staging.claim_detail a 
set bill_type_inst = billtypeinst, bill_type_class = billtypeclass, bill_type_freq = billtypefreq 
from dev.wc_truv_billtype_dedupe  b 
where a.uth_member_id = b.uth_member_id 
  and a.uth_claim_id = b.uth_claim_id 
  and a.claim_sequence_number = b.seq_num
  and a.data_source = 'truv'
;

raise notice 'billtype loaded';

drop table  dev.wc_truv_billtype;

end $$;

