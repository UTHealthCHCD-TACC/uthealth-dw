select *
from reference_tables.cms_proc_codes cpc 
where description like '%ventilat%';

select *
from reference_tables.cms_proc_codes cpc 
where code between '78580' and '78598'

select *
from reference_tables.hcpcs
where full_desc like '%ventil%';

create table dev.ventilator_cms_hcpcs_proc_codes
as
select a.code, a.full_desc
from reference_tables.hcpcs a
where a.full_desc like '%ventil%' and a.full_desc not like '%wheel%'
union
select b.code, b.description
from reference_tables.cms_proc_codes b 
where b.code between '78580' and '78598';

--Create claims table
--What zip and date and counts
create table tableau.optz_ventilators_by_dod_and_year
as
select d.year, m.dod, count(*)
from data_warehouse.claim_detail d
join data_warehouse.claim_header h on d.uth_member_id = h.uth_member_id and d.uth_claim_id = h.uth_claim_id 
join dev.ventilator_cms_hcpcs_proc_codes v on v.code = d.procedure_cd
join data_warehouse.member_enrollment_monthly m on h.uth_member_id = m.uth_member_id
where h.data_source = 'optz'
group by 1, 2
order by 1, 2;




