select 'Analyze: ' || current_timestamp as message;

analyze staging_clean.ccaed_etl;

--output completed message
select 'Truven RX ETL script completed at ' || current_timestamp as message;