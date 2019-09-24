/* Create and populate reference look-up table for
 * Truven Admit Type
 * Table: ccaes
 * Field: admtyp
 * Ref table: truven.ref_admit_type
 * DW table: medical
 * DW field: adm_typ
 * Note: The type of an admission (i.e. elective, urgent, transfer from another facility).
*/

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

drop table if exists truven.ref_admit_type;

CREATE TABLE truven.ref_admit_type (
	"key" int4 NULL,
	value varchar NULL
)
DISTRIBUTED BY ("key");


INSERT INTO truven.ref_admit_type ("key", value) VALUES(1, 'Surgical');
INSERT INTO truven.ref_admit_type ("key", value) VALUES(2, 'Medical');
INSERT INTO truven.ref_admit_type ("key", value) VALUES(3, 'Maternity & Newborn');
INSERT INTO truven.ref_admit_type ("key", value) VALUES(4, 'Psych & Substance Abuse');
INSERT INTO truven.ref_admit_type ("key", value) VALUES(5, 'Unknown');

commit;

-- Test join
select distinct lu.value, t.admtyp
from truven.ccaes as t 
join truven.ref_admit_type lu on (t.admtyp = lu."key")
limit 100;