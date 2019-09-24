create table truven.ccaeo_fix
WITH (appendonly=true, orientation=column)
as
select distinct *
from truven.ccaeo
distributed randomly;

select count(*)
from truven_ccaeo;