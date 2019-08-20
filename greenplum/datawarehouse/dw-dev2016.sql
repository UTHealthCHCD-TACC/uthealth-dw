create schema dev2016;

SET search_path TO dev2016;

--Optum
create table optum_medical
WITH (appendonly=true, orientation=column)
as select * from optum_dod.medical where year=2016
distributed by (patid);

create table optum_diagnostic
WITH (appendonly=true, orientation=column)
as select * from optum_dod.diagnostic where year=2016
distributed by (patid);

create table optum_procedure
WITH (appendonly=true, orientation=column)
as select * from optum_dod.procedure where year=2016
distributed by (patid);

create table optum_member
WITH (appendonly=true, orientation=column)
as select * from optum_dod.member
distributed by (patid);

create table optum_member_detail
WITH (appendonly=true, orientation=column)
as select * from optum_dod.member_detail where year=2016
distributed by (patid);

--Truven
create table truven_ccaei
WITH (appendonly=true, orientation=column)
as select * from truven.ccaei where year=2016
distributed by (enrolid);

create table truven_ccaeo
WITH (appendonly=true, orientation=column)
as select * from truven.ccaeo where year=2016
distributed by (enrolid);

create table truven_ccaes
WITH (appendonly=true, orientation=column)
as select * from truven.ccaes where year=2016
distributed by (enrolid);

create table truven_ccaea
WITH (appendonly=true, orientation=column)
as select * from truven.ccaea where year=2016
distributed by (enrolid);

create table truven_ccaed
WITH (appendonly=true, orientation=column)
as select * from truven.ccaed where year=2016
distributed by (enrolid);

create table truven_ccaef
WITH (appendonly=true, orientation=column)
as select * from truven.ccaef where year=2016
distributed by (enrolid);

create table truven_ccaet
WITH (appendonly=true, orientation=column)
as select * from truven.ccaet where year=2016
distributed by (enrolid);

--NOTE: No 2016 records for ccaep
--create table truven_ccaep
--WITH (appendonly=true, orientation=column)
--as 
select count(*) from truven.ccaep where year=2016;
--distributed by (plankey);

create table truven_mdcri
WITH (appendonly=true, orientation=column)
as select * from truven.mdcri where year=2016
distributed by (enrolid);

create table truven_mdcro
WITH (appendonly=true, orientation=column)
as select * from truven.mdcro where year=2016
distributed by (enrolid);

create table truven_mdcrs
WITH (appendonly=true, orientation=column)
as select * from truven.mdcrs where year=2016
distributed by (enrolid);

create table truven_mdcra
WITH (appendonly=true, orientation=column)
as select * from truven.mdcra where year=2016
distributed by (enrolid);

create table truven_mdcrd
WITH (appendonly=true, orientation=column)
as select * from truven.mdcrd where year=2016
distributed by (enrolid);

create table truven_mdcrf
WITH (appendonly=true, orientation=column)
as select * from truven.mdcrf where year=2016
distributed by (enrolid);

create table truven_mdcrt
WITH (appendonly=true, orientation=column)
as select * from truven.mdcrt where year=2016
distributed by (enrolid);


