drop external table ext_query_history;
CREATE EXTERNAL TABLE ext_query_history (
username varchar, tstart date, tend date, exec_time interval(6), query text
)  
LOCATION ( 
'gpfdist://greenplum01:8081/covid/logs/query_history.log'
)
FORMAT 'text' (DELIMITER '|' null as '' escape 'OFF' FILL MISSING FIELDS
);

select * 
from ext_query_history
where username like '%assafgo%'
--where exec_time > 

select count(*), avg(exec_time) as avg_runtime, max(exec_time) as max_runtime
from ext_query_history;

select username, count(*), min(tend), max(tend), 
avg(exec_time) as avg_runtime, max(exec_time) as max_runtime
from ext_query_history
group by 1
order by 2 desc;

