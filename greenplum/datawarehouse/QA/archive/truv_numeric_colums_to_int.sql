ALTER TABLE truven.ccaea ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.ccaea ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.ccaea set distributed randomly;
ALTER TABLE truven.ccaea ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.ccaea set distributed by (enrolid);

ALTER TABLE truven.mdcra ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.mdcra ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.mdcra set distributed randomly;
ALTER TABLE truven.mdcra ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.mdcra set distributed by (enrolid);

ALTER TABLE truven.ccaed ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.ccaed ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.ccaed ALTER COLUMN pharmid TYPE int4 USING pharmid::int4;
ALTER TABLE truven.ccaed set distributed randomly;
ALTER TABLE truven.ccaed ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.ccaed set distributed by (enrolid);

ALTER TABLE truven.mdcrd ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.mdcrd ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.mdcrd ALTER COLUMN pharmid TYPE int4 USING pharmid::int4;
ALTER TABLE truven.mdcrd set distributed randomly;
ALTER TABLE truven.mdcrd ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.mdcrd set distributed by (enrolid);


ALTER TABLE truven.ccaef ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.ccaef ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.ccaef ALTER COLUMN fachid TYPE int4 USING fachid::int4;
ALTER TABLE truven.ccaef set distributed randomly;
ALTER TABLE truven.ccaef ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.ccaef set distributed by (enrolid);

ALTER TABLE truven.mdcrf ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.mdcrf ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.mdcrf ALTER COLUMN fachid TYPE int4 USING fachid::int4;
ALTER TABLE truven.mdcrf set distributed randomly;
ALTER TABLE truven.mdcrf ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.mdcrf set distributed by (enrolid);

ALTER TABLE truven.ccaei ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.ccaei ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.ccaei ALTER COLUMN physid TYPE int4 USING physid::int4;
ALTER TABLE truven.ccaei ALTER COLUMN caseid TYPE int4 USING caseid::int4;
ALTER TABLE truven.ccaei ALTER COLUMN year TYPE int4 USING year::int4;
ALTER TABLE truven.ccaei set distributed randomly;
ALTER TABLE truven.ccaei ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.ccaei set distributed by (enrolid);

ALTER TABLE truven.mdcri ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.mdcri ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.mdcri ALTER COLUMN physid TYPE int4 USING physid::int4;
ALTER TABLE truven.mdcri ALTER COLUMN caseid TYPE int4 USING caseid::int4;
ALTER TABLE truven.mdcri ALTER COLUMN year TYPE int4 USING year::int4;
ALTER TABLE truven.mdcri set distributed randomly;
ALTER TABLE truven.mdcri ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.mdcri set distributed by (enrolid);

ALTER TABLE truven.ccaeo ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.ccaeo ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.ccaeo ALTER COLUMN fachid TYPE int4 USING fachid::int4;
ALTER TABLE truven.ccaeo set distributed randomly;
ALTER TABLE truven.ccaeo ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.ccaeo set distributed by (enrolid);

ALTER TABLE truven.mdcro ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.mdcro ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.mdcro ALTER COLUMN fachid TYPE int4 USING fachid::int4;
ALTER TABLE truven.mdcro set distributed randomly;
ALTER TABLE truven.mdcro ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.mdcro set distributed by (enrolid);

ALTER TABLE truven.ccaes ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.ccaes ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.ccaes ALTER COLUMN provid TYPE int4 USING provid::int4;
ALTER TABLE truven.ccaes ALTER COLUMN fachid TYPE int4 USING fachid::int4;
ALTER TABLE truven.ccaes ALTER COLUMN caseid TYPE int4 USING caseid::int4;
ALTER TABLE truven.ccaes set distributed randomly;
ALTER TABLE truven.ccaes ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.ccaes set distributed by (enrolid);

ALTER TABLE truven.mdcrs ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.mdcrs ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.mdcrs ALTER COLUMN provid TYPE int4 USING provid::int4;
ALTER TABLE truven.mdcrs ALTER COLUMN fachid TYPE int4 USING fachid::int4;
ALTER TABLE truven.mdcrs ALTER COLUMN caseid TYPE int4 USING caseid::int4;
ALTER TABLE truven.mdcrs set distributed randomly;
ALTER TABLE truven.mdcrs ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.mdcrs set distributed by (enrolid);

ALTER TABLE truven.ccaet ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.ccaet ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.ccaet set distributed randomly;
ALTER TABLE truven.ccaet ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.ccaet set distributed by (enrolid);

ALTER TABLE truven.mdcrt ALTER COLUMN seqnum TYPE int4 USING seqnum::int4;
ALTER TABLE truven.mdcrt ALTER COLUMN efamid TYPE int4 USING efamid::int4;
ALTER TABLE truven.mdcrt set distributed randomly;
ALTER TABLE truven.mdcrt ALTER COLUMN enrolid TYPE int8 USING enrolid::int8;
ALTER TABLE truven.mdcrt set distributed by (enrolid);