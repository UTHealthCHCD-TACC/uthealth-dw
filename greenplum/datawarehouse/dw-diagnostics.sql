
--Main table
drop table data_warehouse.diagnostics;
create table data_warehouse.diagnostics (
id bigserial NOT NULL,
	"source" bpchar(2) NOT NULL,
	medical_id bigint not NULL,
	position int2,
	code varchar, 
	poa bool
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

--Greenplum performance optimization for serial/sequence
alter sequence data_warehouse.diagnostics_id_seq cache 200;

--Optum load: 
--Notes: 
insert into data_warehouse.diagnostics(source, medical_id, position, code)
select 'od', m.id, 
charge, null, null, null, std_cost, null, null, copay, coins, null, paid_dt
from optum_dod.medical;

select *
from optum_dod.medical
limit 10;

select *
from optum_dod.diagnostic
limit 10;

--4943418493, 1791399067
select count(*), count(distinct clmid)
from optum_dod.medical;

/*
1823753471         	8391
178344857          	8283
210952586          	3580
1821760239         	3434
*/
select clmid, count(*) as cnt
from optum_dod.medical
group by 1
having count(*) > 1
order by 2 desc;

select patid, clmid, clmseq, prov, proc_cd, procmod, tos_cd, fst_dt
from optum_dod.medical
where clmid='1823753471'
and patid=33011106907
order by fst_dt;

select patid, clmid, diag, diag_position, icd_flag, loc_cd, poa, fst_dt 
from optum_dod.diagnostic
where clmid='1823753471'
and patid=33011106907
order by fst_dt;

select count(*), min(d.patid)
from optum_dod.diagnostic d
left join optum_dod.medical m on d.patid=m.patid and d.clmid=m.clmid and d.fst_dt=m.fst_dt
where m.patid is null;


select count(*)
from data_warehouse."diagnostics";


-- Diagnostics


/*
 * Truven 'medical' data is split between inpatient and outpatient data tables (ex. ccaei and ccaeo). 
 */
--Truven load Inpatient
insert into data_warehouse.medical(source, mbr_id, claim_typ, claim_no, case_link_key, adm_dt, dc_dt, dc_stat, drg_cd,
tot_chgs, tot_alwd, tot_paid, billed_amt, allowed_amt, paid_amt, ded_amt, copay_amt, coins_amt, cob_amt, adjud_date)
select 'to', enrolid, facprof, msclmid, caseid, admdate, disdate, dstatus, drg,
null, null, null, null, null, netpay, deduct, copay, coins, cob, pddate
from truven.ccaes;

update data_warehouse.medical
set source='ts' where source='to';

--Truven load Outpatient (Skipping for now)
insert into data_warehouse.medical(source, mbr_id, claim_typ, claim_no, case_link_key, adm_dt, dc_dt, dc_stat, drg_cd,
tot_chgs, tot_alwd, tot_paid, billed_amt, allowed_amt, paid_amt, ded_amt, copay_amt, coins_amt, cob_amt, adjud_date)
select 'to', enrolid, facprof, msclmid, null, null, null, null, null,
null, null, null, null, null, netpay, deduct, copay, coins, cob, pddate
from truven.ccaeo;


