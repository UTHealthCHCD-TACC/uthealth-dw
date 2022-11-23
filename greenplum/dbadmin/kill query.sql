select usename, 
      (current_timestamp - query_start)::time as runtime, query, pid, *
  from pg_stat_activity 
 where state = 'active'; 


select pg_cancel_backend(170016);

analyze dev.ccaeo_etl ;

select count(*) from dev.ccaeo_etl ;

select distinct year from truven.ccaeo ;

------
