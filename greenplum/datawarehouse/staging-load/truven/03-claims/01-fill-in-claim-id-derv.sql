/*********************************************************
 * Fills in the CLAIM_ID_DERV for F, O, and S tables in Truven 
 * which is the concatenation of ENROLID || MSCLMID || FACPROF
 * Fill in only for rows where CLAIM_ID_DERV is null
 * 
 * (Run right after a data refresh)
***********************************************************/

--TABLES ARE LISTED IN ASCENDING SIZE ORDER SO THE FIRST FEW SHOULD RUN AUTOMATICALLY
--AND THEN THE LAST FEW YOU SHOULD RUN AND WALK AWAY

/**************************************
 * MDCR tables: S, F, O
 **************************************/

--mdcrs
update truven.mdcrs
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null;

--vacuum analyze it
vacuum analyze truven.mdcrs;


--mdcrf
update truven.mdcrf
set claim_id_derv = enrolid || '-' || msclmid || '-F'
where claim_id_derv is null;
--no facprof column in this table, but assume all claims here are facility claims
--rationale: f tables = facility header tables, and all claims here have a fachdid

--vacuum analyze it
vacuum analyze truven.mdcrf;


--mdcro
update truven.mdcro
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null;

--vacuum analyze it
vacuum analyze truven.mdcro;



/**************************************
 * CCAE tables: S, F, O
 **************************************/

--ccaes
update truven.ccaes
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null;

--vacuum analyze it
vacuum analyze truven.ccaes;


--ccaef
update truven.ccaef
set claim_id_derv = enrolid || '-' || msclmid || '-F'
where claim_id_derv is null;
--no facprof column in this table, but assume all claims here are facility claims
--rationale: f tables = facility header tables, and all claims here have a fachdid

--vacuum analyze it
vacuum analyze truven.ccaef;


--ccaeo - run last b/c it huge
update truven.ccaeo
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof
where claim_id_derv is null;

--vacuum analyze it
vacuum analyze truven.ccaeo;



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

select count(*) from truven.ccaeo where claim_id_derv is null; --5070519
select count(*) from truven.ccaeo; --8242839889
