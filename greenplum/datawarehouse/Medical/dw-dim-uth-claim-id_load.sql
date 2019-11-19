/* this is work in progress
 * finding a set-based way to create new claim ids
 */
with cte1
	as 
	(
		select 'trvc' as dt_src, coalesce(c.msclmid, c.caseid) claim_no, c."year" s_year, c.enrolid, 
		d.member_id_src, d.uth_claim_id
		from truven.ccaes c left outer join data_warehouse.dim_uth_claim_id d
		on d.claim_id_src::numeric = coalesce(c.msclmid, c.caseid)
		and d.member_id_src::numeric = c.enrolid
		and d.data_year::numeric = c."year"
	)
/* should we have uth_member_id in dim_uth_claim_id? */
	insert into data_warehouse.dim_uth_claim_id(claim_id_src, data_source, data_year, member_id_src, uth_claim_id)
	select claim_no, dt_src, s_year, enrolid, 
	into data_warehouse.dim_uth_claim_id m
	from cte1 t
	
select 'trvc' as dt_src, coalesce(c.msclmid, c.caseid) claim_no, c."year", c.enrolid, 
d.member_id_src, d.uth_claim_id
from truven.ccaes c left outer join data_warehouse.dim_uth_claim_id d
on d.claim_id_src::numeric = coalesce(c.msclmid, c.caseid)
and d.member_id_src::numeric = c.enrolid
and d.data_year::numeric = c."year"
where d.data_source = 'trvc'
and d.uth_claim_id is null
and d.claim_id_src is not null
