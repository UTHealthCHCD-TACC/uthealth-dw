-- State
select distinct egeoloc
from truven.ccaet;

-- Need lookup table for Truven state codes
drop external table truven_state_codes;
create external table truven_state_codes
(
truven_code smallint, state varchar, abbr varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/truven-state-codes.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

drop table truven.state_codes_ref;
create table truven.state_codes_ref
as 
select truven_code, state, upper(abbr) as abbr
from truven_state_codes;

