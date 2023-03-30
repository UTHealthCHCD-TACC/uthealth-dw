/*********************************************************
 * This adds the column CLAIM_ID_DERV to F, O, and S tables in Truven 
 * which is the concatenation of ENROLID || MSCLMID || FACPROF
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


