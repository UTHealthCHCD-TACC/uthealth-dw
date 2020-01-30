explain analyze
with cte0 as
	( 
		-- first I have to get only the rows from s table that are unique by case id
		select distinct on (s."year", s.caseid) s.caseid, s.stdplac, s.enrolid, s.msclmid,
		cast(coalesce(s.msclmid, s.caseid) as text) tmp_src_clm_id, trunc(s."year",0) s_year
		from truven.ccaes s
		--where s."year" between 2015 and 2017
		order by s."year", s.caseid
	)
--select count(*) from cte0
--record count at this stage is 14,107,199 without limiting by year
--record count is 4,053,913 when limiting to 2015-2017
, 
	cte1 as 
	( 
		select 'trvc' dt_src, i.caseid, i.admdate, i.disdate, s1.msclmid, s1.tmp_src_clm_id,
		s1.enrolid, s1.s_year, i.totpay, i.totnet, s1.stdplac
		--select count(*)
		from cte0 s1 join truven.ccaei i 
		on i.caseid = s1.caseid
		and i."year" = s1.s_year
		where i."year" between 2015 and 2017
	)
	--select count(*) from cte1
	--4,053,913 total unique case id's by year between 2015 and 2017 in ccaes
,
	cte2 as 
	(  

		--select c.dt_src, u.uth_claim_id, u.uth_member_id, c.caseid, c.totpay, c.totnet, c.stdplac
		--select count(*)
		select distinct on(u.data_year, u.claim_id_src) c.dt_src, u.uth_claim_id, u.uth_member_id, 
		c.s_year, c.caseid, c.totpay, c.totnet, c.stdplac
		from cte1 c inner join data_warehouse.dim_uth_claim_id u 
		on u.claim_id_src = c.tmp_src_clm_id
		and u.data_year = c.s_year
		where u.data_source='trvc'
		order by u.data_year, u.claim_id_src	
	)
select count(*) from cte2
--3,296,871 records returned

--4,053,913 total unique case id's by year between 2015 and 2017 in ccaes
--select c.dt_src, u.uth_claim_id, u.uth_member_id, c.caseid, c.totpay, c.totnet, c.stdplac
--select count(*)
select distinct on(u.claim_id_src,u.data_year) c.dt_src, u.uth_claim_id, u.uth_member_id, c.caseid, 
c.totpay, c.totnet, c.stdplac
from cte1 c inner join data_warehouse.dim_uth_claim_id u 
on u.claim_id_src = c.tmp_src_clm_id
and u.data_year = c.s_year::text
where u.data_source='trvc'
order by u.claim_id_src, u.data_year

select * from data_warehouse.dim_uth_claim_id
where claim_id_src='350982666.0' and data_year='2015'

select * from data_warehouse.dim_uth_claim_id

select count(distinct caseid) from truven.ccaes
--2,927,580 unique case id's in ccaes

select count(caseid) from truven.ccaes
--418,682,666 total case id's in ccaes

select count(caseid) from truven.ccaei
--14,107,199 total case id's in ccaei

select count(distinct caseid) from truven.ccaei
--2,927,580 total unique case id's in ccaei

with cte5
	as
	( 
		select distinct year, caseid
		from truven.ccaei
		where year between 2015 and 2017
	)
select count(*)
from cte5
--14,107,199 unique combinations of caseid and year
--note that this is the same as total count of records in ccaei

select count(*) from truven.ccaei


group by year, caseid
having count(*)>1

select i.caseid, i.admdate, i.enrolid, s.caseid, s.msclmid, s.enrolid
select count(*)
from truven.ccaes s inner join truven.ccaei i
on s.caseid = i.caseid
and s."year" = i."year"
where i.admdate between '1/1/2015' and '12/31/2017'
--124,915,997

select count(*) from truven.ccaes
--418,682,666


