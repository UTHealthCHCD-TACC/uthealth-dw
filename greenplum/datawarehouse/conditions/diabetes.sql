-- Diabetes Condition v37

-- ndc table for diabetes drugs
drop table if exists dev.conditions_diabetes_ndc;
create table dev.conditions_diabetes_ndc as 
select distinct ndc from data_warehouse.pharmacy_claims pc
where ahfs_class like '6820%';


-- select all the diabetes icd codes, 

drop table if exists dev.conditions_diabetes_1;
create table dev.conditions_diabetes_1 
(
	uth_member_id int8 NULL,
	uth_claim_id int8 NULL,
	diag_position int4 NULL,
	claim_sequence_number int4 NULL,
	"year" int2 NULL
)
with (appendonly=true, orientation=column, compresstype=zlib) 
distributed by (uth_member_id); 

insert into dev.conditions_diabetes_1
with diab_dx as (
select
	*
from
	conditions.codeset
where
	condition_cd = 'db'
	and cd_type in ('ICD-10', 'ICD-9') 
	and position('%' in cd_value) = 0)
select
	cdx.uth_member_id, cdx.uth_claim_id , cdx.diag_position, cdx.claim_sequence_number, year
from
	data_warehouse.claim_diag cdx
inner join diab_dx on
	diag_cd = diab_dx.cd_value;

insert into dev.conditions_diabetes_1
with diab_dx as (
select
	*
from
	conditions.codeset
where
	condition_cd = 'db'
	and cd_type in ('ICD-10', 'ICD-9') 
    and position('%' in cd_value) > 0)
select
	cdx.uth_member_id, cdx.uth_claim_id , cdx.diag_position, cdx.claim_sequence_number, year
from
	data_warehouse.claim_diag cdx
inner join diab_dx on
	diag_cd like diab_dx.cd_value;


-- if ed or ip with asthma as primary they qualify for that year
drop table if exists dev.conditions_diabetes_2;
create table dev.conditions_diabetes_2 as
select
	ch.uth_member_id ,
	ch.uth_claim_id,
	ch.year,
	diag_position,
	ch.claim_type,
	cd.revenue_cd,
	cd.claim_sequence_number,
	1 as ip_ed_prof_qualifier,
	case when claim_type = 'F' and (revenue_cd like '045%' or revenue_cd like '0981%') then 1
	else 0
	end as ed_qualifier,
	case when claim_type = 'F' and revenue_cd in ('0100', '0101', '0102', '0103', '0104', '0105', '0106', '0107', '0108', '0109', '0110', 
	'0111', '0112', '0113', '0114', '0119', '0120', '0121', '0122', '0123', '0124', '0129', '0130', '0131', '0132', '0133', '0134', '0139',
'0140', '0141', '0142', '0143', '0144', '0149', '0150', '0151', '0152', '0153', '0154', '0159', '0160', '0161', '0162', '0163', '0164', '0165', 
'0166', '0167', '0168', '0169', '0200', '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213',
'0214', '0215', '0216', '0217', '0218', '0219', '0720', '0721', '0722', '0723', '0724', '0725', '0726', '0727', '0728', '0729', '0800', '0801',
'0802', '0803', '0804', '0805', '0806', '0807', '0808', '0809', '0987') then 1
	else 0 
	end as ip_qualifier,
	0 as rx_qualifier
from
	dev.conditions_diabetes_1 ap
inner join data_warehouse.claim_header ch on
	ch.uth_member_id = ap.uth_member_id
	and ch.uth_claim_id = ap.uth_claim_id
inner join data_warehouse.claim_detail cd on
	ap.uth_member_id = cd.uth_member_id
	and ap.uth_claim_id = cd.uth_claim_id
	and ap.claim_sequence_number = cd.claim_sequence_number;


-- add diabetes meds
insert
	into
	dev.conditions_diabetes_2
with db_ndc as (
	select
		uth_member_id,
		uth_rx_claim_id as uth_claim_id,
		year,
		an.ndc
	from
		data_warehouse.pharmacy_claims pc
	inner join 
 dev.conditions_diabetes_ndc an on
		pc.ndc = an.ndc)
select
	uth_member_id,
	uth_claim_id,
	year,
	null as diag_position ,
	'RX' as claim_type,
	null as revenue_cd ,
	null as claim_sequence_number ,
	0 as ip_ed_prof_qualifier,
	0 as ed_qualifier,
	0 as ip_qualifier, 
	1 as rx_qualifier
from
	db_ndc ;



drop table if exists dev.conditions_diabetes_3;
create table dev.conditions_diabetes_3 as (
with claim_agg as (
select
	uth_member_id,
	uth_claim_id,
	year,
	max(ip_ed_prof_qualifier) ip_ed_prof_qualifier,
	max(ed_qualifier) ed_qualifier,
	max(ip_qualifier) ip_qualifier,
	max(rx_qualifier) rx_qualifier
from
	dev.conditions_diabetes_2
group by
	uth_member_id,
	uth_claim_id,
	year)
select
	uth_member_id,
	year,
	sum(ip_ed_prof_qualifier) ip_ed_prof_qualifier,
	max(ed_qualifier) ed_qualifier,
	max(ip_qualifier) ip_qualifier,
	sum(rx_qualifier) rx_qualifier
from
	claim_agg
group by
	uth_member_id,
	year)
distributed by (uth_member_id);

with diabetes_qualified as (select
	*
from
	dev.conditions_diabetes_3
where 	
	case when ed_qualifier = 1 then 1
	when  ip_qualifier = 1 then 1
	when rx_qualifier >= 1 then 1
	when ip_ed_prof_qualifier >=2  then 1
	else 0
	end = 1
order by
	uth_member_id,
	year)
select uth_member_id, min(year) as initial_diabetes_year from diabetes_qualified group by uth_member_id
order by uth_member_id;
