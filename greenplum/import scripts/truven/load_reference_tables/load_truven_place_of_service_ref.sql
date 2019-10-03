/* Create and populate reference look-up table for
 * Truven Place of Service
 * Table: ccaes, ccaeo
 * Field: stdplac
 * Ref table: truven.ref_place_of_service
 * DW table: medical
 * DW field: pos
 * Note: A code that identifies the place of service or relates to the 
 * place of treatment where health related services were rendered.
*/

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

drop table if exists truven.ref_place_of_service;

CREATE TABLE truven.ref_place_of_service (
	"key" int4 NULL,
	value varchar NULL
)
DISTRIBUTED BY ("key");

INSERT INTO truven.ref_place_of_service ("key", value) VALUES(1, 'Pharmacy');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(3, 'School');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(4, 'Homeless Shelter');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(11, 'Office');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(12, 'Patient Home');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(13, 'Assisted Living Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(14, 'Group Home');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(15, 'Mobile Unit');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(16, 'Temporary Lodging');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(17, 'Walk-in Retail Health Clinic');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(20, 'Urgent Care Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(21, 'Inpatient Hospital');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(22, 'Outpatient Hospital');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(23, 'Emergency Room - Hospital');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(24, 'Ambulatory Surgical Center');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(25, 'Birthing Center');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(26, 'Military Treatment Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(27, 'Inpatient Long-Term Care (NEC)');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(28, 'Other Inpatient Care (NEC)');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(31, 'Skilled Nursing Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(32, 'Nursing Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(33, 'Custodial Care Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(34, 'Hospice');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(35, 'Adult Living Care Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(41, 'Ambulance (land)');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(42, 'Ambulance (air or water)');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(49, 'Independent Clinic');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(50, 'Federally Qualified Health Ctr');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(51, 'Inpatient Psychiatric Facility');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(52, 'Psych Facility Partial Hosp');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(53, 'Community Mental Health Center');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(54, 'Intermed Care/Mental Retarded');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(55, 'Residential Subst Abuse Facil');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(56, 'Psych Residential Treatmnt Ctr');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(57, 'Non-resident Subst Abuse Facil');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(60, 'Mass Immunization Center');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(61, 'Comprehensive Inpt Rehab Fac');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(62, 'Comprehensive Outpt Rehab Fac');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(65, 'End-Stage Renal Disease Facil');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(71, 'State/Local Public Health Clin');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(72, 'Rural Health Clinic');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(81, 'Independent Laboratory');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(95, 'Outpatient (NEC)');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(98, 'Pharmacy');
INSERT INTO truven.ref_place_of_service ("key", value) VALUES(99, 'Other Unlisted Facility');

commit;

-- Test join
select distinct lu.value, t.stdplac
from truven.ccaes as t 
join truven.ref_place_of_service lu on (t.stdplac = lu."key")
limit 100;

select distinct lu.value, t.stdplac
from truven.ccaeo as t 
join truven.ref_place_of_service lu on (t.stdplac = lu."key")
limit 100;