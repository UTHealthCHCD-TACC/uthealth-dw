/*

Member Enrollment Raw Claims Check

--- the purpose of this file is to verify that members and claims in raw data exist inside DW / DW staging 
--- we want to look in the dimensions table on data_warehouse to see if claims that exist in the raw data have matching entries in the DW

*/

select * from data_warehouse.dim_uth_claim_id  ; 
/*
 * 

claim_id_src will always join to source claim id, same with member_id_src for member id

|uth_claim_id  |uth_member_id|data_source|claim_id_src|member_id_src|data_year|
|--------------|-------------|-----------|------------|-------------|---------|
|27,293,629,000|1,140,300,896|optd       |100006358   |33071592041  |2,021    |
|27,293,629,001|1,112,057,363|optd       |100012657   |33180455931  |2,018    |
|27,293,629,002|1,112,434,562|optd       |100013344   |33060284804  |2,020    |
|27,293,629,003|1,115,104,040|optd       |100016367   |33161430981  |2,016    |
|27,293,629,004|1,112,255,715|optd       |100017957   |33224846680  |2,019    |
|27,293,629,005|1,140,539,036|optd       |100018149   |33238090467  |2,020    |
|27,293,629,006|1,138,245,181|optd       |1000188516  |33034094826  |2,007    |
|27,293,629,007|1,134,039,755|optd       |100019458   |33170446959  |2,017    |
|27,293,629,008|1,112,759,626|optd       |100020798   |33100858646  |2,019    |
|27,293,629,009|1,113,142,583|optd       |1000238455  |33012798220  |2,007    |
|27,293,629,010|1,167,516,255|optd       |100024108   |33167497319  |2,015    |

*/

-----------------------------------------------------
------------ TRUVEN ---------------------------------
-----------------------------------------------------
--- join keys for truven are enrolid and msclmid

select enrolid, msclmid 
  from truven.mdcrs ;
 
--- example:

select a.enrolid, b.uth_member_id, a.msclmid, b.uth_claim_id  
  from truven.mdcrs a 
 inner join data_warehouse.dim_uth_claim_id b  --- inner join keeps only 
    on a.enrolid::text = b.member_id_src ---- always join on both member and claim id
   and a.msclmid::text = b.claim_id_src  ---- you may need to change one of the ids to text, if you gives you an error about data types '::text' means take this column and make it into text 
 where b.data_source = 'truv' --- if you add the data_source condition on where, it will run faster 
;
/*
|enrolid       |uth_member_id|msclmid    |uth_claim_id  |
|--------------|-------------|-----------|--------------|
|4,326,105,602 |568,217,443  |3,609,984  |36,285,437,093|
|2,538,309,002 |541,724,709  |818,926,666|36,285,520,032|
|4,179,976,401 |686,840,877  |312,826    |36,285,520,066|
|29,458,471,401|614,911,224  |519,955,085|36,285,622,526|
|2,850,715,401 |566,749,661  |653,661,668|36,286,034,332|
*/


--- you can use a left outer join to find what is missing in one table from another --- keep all from "from" (left) table and whatever matches in join (right) table 
--- you create a where condition on the second table for where it is null... if the right column is null, it means that the keys for join were not found in 2nd table 

select a.enrolid, b.uth_member_id, a.msclmid, b.uth_claim_id  
  from truven.mdcrs a 
  left outer join data_warehouse.dim_uth_claim_id b 
    on a.enrolid::text = b.member_id_src 
   and a.msclmid::text = b.claim_id_src  
   and b.uth_claim_id is null -- adding the null filter condition 
;
/*
 * So now we have claims in mdcrs that do not exist in the DW 
|enrolid       |uth_member_id|msclmid    |uth_claim_id|
|--------------|-------------|-----------|------------|
|1,123,835,604 |             |759,979,666|            |
|2,479,194,501 |             |519,225,666|            |
|29,936,007,101|             |266,430,416|            |
|28,145,776,704|             |617,958,022|            |
|1,544,275,604 |             |206,400,719|            |
|1,928,364,501 |             |551,938,665|            |
|1,280,635,901 |             |1,843,211  |            |
|3,403,772,201 |             |11,929,639 |            |
|4,029,516,903 |             |2,589,084  |            |
|1,373,990,104 |             |904,712,166|            |
|28,344,739,403|             |408,953,965|            |
|1,539,766,403 |             |10,872,478 |            |

 */


---Now we want to find counts - so as to assess if there are missing claims 
select count(distinct a.enrolid::text || msclmid)
  from truven.mdcrs a 
  left outer join data_warehouse.dim_uth_claim_id b 
    on a.enrolid::text = b.member_id_src 
   and a.msclmid::text = b.claim_id_src  
   and b.data_year = a."year" 
   and b.data_source = 'truv' 
 where b.uth_claim_id is null --- adding the null filter condition 
;



---- build QA table
drop table if exists qa_reporting.raw_claims_check;

create table qa_reporting.raw_claims_check(
	data_source text null,
	raw_table text null,
	data_year int null,
	total_raw int null,
	count_found int null,
	count_missing int null,
	pct_missing numeric null
	);


-----------one option is to use CTE ---- but it is doing a lot of work 25 min or so 
with getids as 
  (
select distinct a."year", enrolid::text, msclmid, b.uth_member_id, b.uth_claim_id
  from truven.mdcrs a 
  left outer join data_warehouse.dim_uth_claim_id b 
    on a.enrolid::text = b.member_id_src 
   and a.msclmid::text = b.claim_id_src  
   and a."year" = b.data_year 
   and b.data_source = 'truv'
   )
   insert into qa_reporting.raw_claims_check
  select 'truv' as data_source, 
  		 'mdcrs' as raw_table,  
  		 "year" as data_year,
  		 count(*) as total_raw,
  		 count(*) filter (where uth_claim_id is not null) as count_found,
  		 count(*) filter (where uth_claim_id is null) as count_missing
    from getids
   group by "year" ;
   
  
  ------------ splitting it up into two steps took about 1/3 the time:
  ---- 1) get records with left join to capture missings ids in DW 
  ----- drop table if exists dev.delete_temp_rawclaims;
  
  select distinct a."year", enrolid::text, msclmid, b.uth_member_id, b.uth_claim_id
  into dev.delete_temp_rawclaims
  from truven.mdcrs a 
  left outer join data_warehouse.dim_uth_claim_id b 
    on a.enrolid::text = b.member_id_src 
   and a.msclmid::text = b.claim_id_src  
   and a."year" = b.data_year 
   and b.data_source = 'truv'
   ;
  
---- 2) aggregate counts from temp table   
insert into qa_reporting.raw_claims_check
   select 'truv' as data_source, 
  		 'mdcrs' as raw_table,  
  		 "year" as data_year,
  		 count(*) as total_raw,
  		 count(*) filter (where uth_claim_id is not null) as count_found,
  		 count(*) filter (where uth_claim_id is null) as count_missing
    from  dev.delete_temp_rawclaims 
   group by "year" ;
   
--delete from qa_reporting.raw_claims_check ;

  select * from qa_reporting.raw_claims_check;