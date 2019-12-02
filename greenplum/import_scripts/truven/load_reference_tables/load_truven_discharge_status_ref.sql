/* Create and populate reference look-up table for
 * Truven Discharge Status
 * Tables: ccaei, ccaef
 * Field: dstatus
 * Ref table: truven.ref_discharge_status
 * DW table: medical
 * DW field: dc_stat
 * Note: Discharge Status
*/

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

drop table if exists truven.ref_discharge_status;

CREATE TABLE truven.ref_discharge_status (
	"key" int4 NULL,
	value varchar NULL
)
DISTRIBUTED BY ("key");

INSERT INTO truven.ref_discharge_status ("key", value) VALUES(1, 'Discharged to home self care');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(2, 'Transfer to short term hospital');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(3, 'Transfer to SNF');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(4, 'Transfer to ICF');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(5, 'Transfer to other facility');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(6, 'Discharged home under care');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(7, 'Left against medical advice');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(8, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(9, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(10, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(11, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(12, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(13, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(14, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(15, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(16, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(17, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(18, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(19, 'Other alive status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(20, 'Died');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(21, 'Discharged/transferred to court/law enforcement');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(30, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(31, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(32, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(33, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(34, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(35, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(36, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(37, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(38, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(39, 'Still patient');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(40, 'Other died status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(41, 'Other died status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(42, 'Other died status');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(43, 'Discharged/transferred to federal hospital');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(50, 'Discharged to home (from Hospice)');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(51, 'Transfer to med fac (from Hospice)');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(61, 'Transfer to Medicare approved swing bed');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(62, 'Transferred to inpatient rehab facility (IRF)');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(63, 'Transferred to long term care hospital (LTCH)');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(64, 'Transferred to nursing facility Medicaid certified');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(65, 'Transferred to psychiatric hospital or unit');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(66, 'Transferred to critical access hospital');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(70, 'Transfer to another facility NEC');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(71, 'Transfer/referred to other facility for outpt svcs');
INSERT INTO truven.ref_discharge_status ("key", value) VALUES(72, 'Transfer/referred to this facility for outpt svcs');

commit;

-- Test joins
select distinct lu.value, t.dstatus
from truven.ccaei as t 
join truven.ref_discharge_status lu on (t.dstatus = lu."key")
limit 100;

select distinct lu.value, t.dstatus
from truven.ccaef as t 
join truven.ref_discharge_status lu on (t.dstatus = lu."key")
limit 100;