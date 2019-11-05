--this query seems to correctly renumber the claim sequence numbers
with cte1
	as (
select s.caseid, s.msclmid, s.enrolid, s."year", 
	case s.msclmid
		when null then cast(trunc(s.caseid,0)::text||trunc(s.year,0)::text||trunc(s.enrolid,0)::text as numeric)
		else cast(trunc(s.msclmid,0)::text||trunc(s.year,0)::text||trunc(s.enrolid,0)::text as numeric)
	end claim_no, s.seqnum
from truven.ccaes s)
select year, enrolid, claim_no, seqnum,
row_number() over (
	partition by claim_no
	order by seqnum) rownum
from cte1
order by claim_no, seqnum