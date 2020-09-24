---a
update truven.mdcra 
set enrolid = trunc(enrolid,0), 
	efamid = trunc(efamid,0), 
	"year" = trunc("year",0), 
	seqnum = trunc(seqnum,0),
	dobyr = trunc(dobyr,0)
;

vacuum analyze truven.mdcra; 

---d
update truven.mdcrd
set enrolid = trunc(enrolid,0), 
	efamid = trunc(efamid,0), 
	"year" = trunc("year",0), 
	seqnum = trunc(seqnum,0),
	dobyr = trunc(dobyr,0)
;

vacuum analyze truven.mdcrd; 
	
---f
update truven.mdcrf 
set enrolid = trunc(enrolid,0), 
	efamid = trunc(efamid,0), 
	"year" = trunc("year",0), 
	seqnum = trunc(seqnum,0),
	dobyr = trunc(dobyr,0),
	caseid = trunc(caseid,0)
;

vacuum analyze truven.mdcrf;


--i
update truven.mdcri 
set enrolid = trunc(enrolid,0), 
	efamid = trunc(efamid,0), 
	"year" = trunc("year",0), 
	seqnum = trunc(seqnum,0),
	dobyr = trunc(dobyr,0),
	caseid = trunc(caseid,0)
;

vacuum analyze truven.mdcri; 


--o
update truven.mdcro 
set enrolid = trunc(enrolid,0), 
	efamid = trunc(efamid,0), 
	"year" = trunc("year",0), 
	seqnum = trunc(seqnum,0),
	dobyr = trunc(dobyr,0),
	msclmid = trunc(msclmid,0)
;

vacuum analyze truven.mdcro;

select * from truven.mdcro where year = 2018 and enrolid is not null;


--s
update truven.mdcrs 
set enrolid = trunc(enrolid,0), 
	efamid = trunc(efamid,0), 
	"year" = trunc("year",0), 
	seqnum = trunc(seqnum,0),
	dobyr = trunc(dobyr,0),
	caseid = trunc(caseid,0),
	msclmid = trunc(msclmid,0)
;

vacuum analyze truven.mdcrs;

select * from truven.mdcrs where year = 2012;

--t
update truven.mdcrt 
set enrolid = trunc(enrolid,0), 
	efamid = trunc(efamid,0), 
	"year" = trunc("year",0), 
	seqnum = trunc(seqnum,0),
	dobyr = trunc(dobyr,0)
;

vacuum analyze truven.mdcrt;

select * from truven.mdcrt;


select distinct data_source from data_warehouse.dim_uth_admission_id duai 


delete from data_warehouse.dim_uth_admission_id  where data_source = 'truv';
