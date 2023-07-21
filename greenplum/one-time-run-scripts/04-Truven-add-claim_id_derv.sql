/*********************************************************
 * This adds the column CLAIM_ID_DERV to F, O, and S tables in Truven 
 * which is the concatenation of ENROLID || MSCLMID || FACPROF
 * 
 * 7/13/23: QA'd - When re-building DIM tables, wanted to QA this column.
 * Findings: FACPROF is never left null, but ENROLID and MSCLMID can be null
 * As we are concatenating these columns, the column will be null if EITHER of these fields
 * is null.
 * 
 * I think this is okay as
 * 		1) if ENROLID is null, the claim is useless to us
 * 		2) if CLMID is null, there's some usage but it's impossible to link it to anything else
 * 		3) The percentage is VERY SMALL. Less than 0.1%
***********************************************************/

/**************************************
 * F tables: assume that FACPROF = 'F'
 **************************************/
--mdcrf table
--add column claim_id_derv
alter table truven.mdcrf
add column claim_id_derv text;

--populate claim_id_derv with concatenation of enrolid-msclmid-facprof
update truven.mdcrf
set claim_id_derv = enrolid || '-' || msclmid || '-F';
--no facprof column in this table, but assume all claims here are facility claims
--rationale: f tables = facility header tables, and all claims here have a fachdid

--vacuum analyze it
vacuum analyze truven.mdcrf;

--ccaef table
--add column claim_id_derv
alter table truven.ccaef
add column claim_id_derv text;

--populate claim_id_derv with concatenation of enrolid-msclmid-facprof
update truven.ccaef
set claim_id_derv = enrolid || '-' || msclmid || '-F';
--no facprof column in this table, but assume all claims here are facility claims
--rationale: f tables = facility header tables, and all claims here have a fachdid

--vacuum analyze it
vacuum analyze truven.ccaef;

/**************************************
 * S tables
 **************************************/

/*QA
 * 
select sum(case when enrolid is null then 1 else 0 end) as enrolid_null,
	sum(case when msclmid is null then 1 else 0 end) as msclmid_null,
	sum(case when facprof is null then 1 else 0 end) as facprof_null
from truven.mdcrs;

--31937	18349	0

select sum(case when enrolid is null then 1 else 0 end) as enrolid_null,
	sum(case when msclmid is null then 1 else 0 end) as msclmid_null,
	sum(case when facprof is null then 1 else 0 end) as facprof_null
from truven.ccaes;

--788441	373925	0
 */

--mdcrs table
--add column claim_id_derv
alter table truven.mdcrs
add column claim_id_derv text;

--populate claim_id_derv with concatenation of enrolid-msclmid-facprof
update truven.mdcrs
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof;

--vacuum analyze it
vacuum analyze truven.mdcrs;


--ccaes table
--add column claim_id_derv
alter table truven.ccaes
add column claim_id_derv text;

--populate claim_id_derv with concatenation of enrolid-msclmid-facprof
update truven.ccaes
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof;

--vacuum analyze it
vacuum analyze truven.ccaes;




/**************************************
 * O tables
 **************************************/

/*QA
 * 
select sum(case when enrolid is null then 1 else 0 end) as enrolid_null,
	sum(case when msclmid is null then 1 else 0 end) as msclmid_null,
	sum(case when facprof is null then 1 else 0 end) as facprof_null
from truven.mdcro;

--221137	88837	0

select sum(case when enrolid is null then 1 else 0 end) as enrolid_null,
	sum(case when msclmid is null then 1 else 0 end) as msclmid_null,
	sum(case when facprof is null then 1 else 0 end) as facprof_null
from truven.ccaeo;

--5175542	2121423	0
 */

--mdcro table
--add column claim_id_derv
alter table truven.mdcro
add column claim_id_derv text;

--populate claim_id_derv with concatenation of enrolid-msclmid-facprof
update truven.mdcro
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof;

--vacuum analyze it
vacuum analyze truven.mdcro;


--save ccaeo table for last b/c it is the biggest
--ccaeo table
--add column claim_id_derv
alter table truven.ccaeo
add column claim_id_derv text;

--populate claim_id_derv with concatenation of enrolid-msclmid-facprof
update truven.ccaeo
set claim_id_derv = enrolid || '-' || msclmid || '-' || facprof;

--vacuum analyze it
vacuum analyze truven.ccaeo;


