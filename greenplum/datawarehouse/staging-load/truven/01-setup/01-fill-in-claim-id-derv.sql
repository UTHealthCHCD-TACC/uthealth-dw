/*********************************************************
 * Fills in the CLAIM_ID_DERV for F, O, and S tables in Truven 
 * which is the concatenation of ENROLID || MSCLMID || FACPROF
 * Fill in only for rows where CLAIM_ID_DERV is null
 * 
 * (Run right after a data refresh)
***********************************************************/

--TABLES ARE LISTED IN ASCENDING SIZE ORDER SO THE FIRST FEW SHOULD RUN AUTOMATICALLY
--AND THEN THE LAST FEW YOU SHOULD RUN AND WALK AWAY

/*************************************
 * Check if claim_id_derv is null in various tables
 * 
select year, sum(case when claim_id_derv is null then 1 else 0 end) as clm_null,
	sum(case when claim_id_derv is not null then 1 else 0 end) clm_not_null
from truven.mdcrs group by year order by year;

select year, sum(case when claim_id_derv is null then 1 else 0 end) as clm_null,
	sum(case when claim_id_derv is not null then 1 else 0 end) clm_not_null
from truven.ccaes group by year order by year;
 * 
 * --2022 is null as of 7/19/23, let's fix that
 */

select 'Truven claim_id_derv script started at ' || current_timestamp as message;

/**************************************
 * MDCR tables: S, F, O
 **************************************/
select 'mdcrs start: ' || current_timestamp as message;
--mdcrs
update truven.mdcrs
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null and enrolid is not null and msclmid is not null;

--vacuum analyze it
vacuum analyze truven.mdcrs;

select 'mdcrf start: ' || current_timestamp as message;
--mdcrf
update truven.mdcrf
set claim_id_derv = enrolid || '-' || msclmid || '-F'
where claim_id_derv is null and enrolid is not null and msclmid is not null;
--no facprof column in this table, but assume all claims here are facility claims
--rationale: f tables = facility header tables, and all claims here have a fachdid

--vacuum analyze it
vacuum analyze truven.mdcrf;

select 'mdcro start: ' || current_timestamp as message;
--mdcro
update truven.mdcro
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null and enrolid is not null and msclmid is not null;

--vacuum analyze it
vacuum analyze truven.mdcro;



/**************************************
 * CCAE tables: S, F, O
 **************************************/
select 'mdcr tables completed, ccaes start: ' || current_timestamp as message;
--ccaes
update truven.ccaes
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null and enrolid is not null and msclmid is not null;

--vacuum analyze it
vacuum analyze truven.ccaes;

select 'ccaef start: ' || current_timestamp as message;
--ccaef
update truven.ccaef
set claim_id_derv = enrolid || '-' || msclmid || '-F'
where claim_id_derv is null and enrolid is not null and msclmid is not null;
--no facprof column in this table, but assume all claims here are facility claims
--rationale: f tables = facility header tables, and all claims here have a fachdid

--vacuum analyze it
vacuum analyze truven.ccaef;

select 'ccaeo start: ' || current_timestamp as message;
--ccaeo - run last b/c it huge
update truven.ccaeo
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null and enrolid is not null and msclmid is not null;

--vacuum analyze it
vacuum analyze truven.ccaeo;

select 'Truven claim_id_derv script completed at ' || current_timestamp as message;

/****************************
* Get last vacuum analyze date for all tables in a schema_name
* --this acts as a proxy for which tables have been updated
***************************/

select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'truven'
	and (relname like 'ccae%' or relname like 'mdcr%')
	and (relname like '%f' or relname like '%s' or relname like '%o')
order by last_vacuum;


/*QA
select year, count(*) from truven.ccaeo where claim_id_derv is null and year between 2019 and 2022 group by year order by year;

select year, count(*) from truven.ccaeo where enrolid is null and year between 2019 and 2022 group by year order by year;
 */


