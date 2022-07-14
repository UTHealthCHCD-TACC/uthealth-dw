select usename, (current_timestamp - query_start)::time as runtime, query, pid, *
from pg_stat_activity 
where state = 'active'


select pg_cancel_backend(136638);

--select pg_terminate_backend(86270);

