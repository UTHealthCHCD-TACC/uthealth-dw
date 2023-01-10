select usename, 
      (current_timestamp - query_start)::time as runtime, query, pid, * 
  from pg_stat_activity 
 where state = 'active'; 


select pg_terminate_backend(169902);
select pg_cancel_backend(279531);

