select 'Add provider type: ' || current_timestamp as message;
--- add provider type 

update dw_staging.truc_claim_header a 
   set provider_specialty = to_char(b.stdprov , 'FM9999')
  from staging_clean.truv_ccaef_etl b
 where a.member_id_src::bigint = b.enrolid 
   and a.claim_id_src = b.claim_id_derv 
   and substring(table_id_src,1,2) = 'cc';
  
select 'Analyze: ' || current_timestamp as message;

vacuum analyze dw_staging.truc_claim_header;
   

select 'Truven CCAE Claim Header script completed at ' || current_timestamp as message;