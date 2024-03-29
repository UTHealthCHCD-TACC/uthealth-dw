/* ******************************************************************************************************
 *  Custom script to manually expore each numeric field and convert int if applicable
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2019 || script created
 * ******************************************************************************************************
 */
select min(enrolid), max(enrolid)
from truven.ccaea;

ALTER TABLE truven.ccaea
ALTER COLUMN seqnum TYPE int;

ALTER TABLE truven.ccaea
ALTER COLUMN efamid TYPE int;

ALTER TABLE truven.ccaea
ALTER COLUMN enrolid TYPE bigint;