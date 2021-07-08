select min(enrolid), max(enrolid)
from truven.ccaea;

ALTER TABLE truven.ccaea
ALTER COLUMN seqnum TYPE int;

ALTER TABLE truven.ccaea
ALTER COLUMN efamid TYPE int;

ALTER TABLE truven.ccaea
ALTER COLUMN enrolid TYPE bigint;