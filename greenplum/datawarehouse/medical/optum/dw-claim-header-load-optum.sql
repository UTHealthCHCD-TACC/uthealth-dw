--------------------------------------------------------------------------------------------------
--- ** OPTD **
--------------------------------------------------------------------------------------------------

---working table
drop table if exists dev.wc_claim_header_optd;

create table dev.wc_claim_header_optd
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_header limit 0
distributed by (member_id_src);


---dim uth claims for optd 
drop table if exists dev.wc_optd_uth_claim;
create table dev.wc_optd_uth_claim
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);


--optd medical
drop table if exists dev.wc_optd_medical;
create table dev.wc_optd_medical 
with(appendonly=true,orientation=column,compresstype=zlib)
as select * from optum_dod.medical
distributed by (patid);

vacuum analyze dev.wc_optd_medical;

vacuum analyze dev.wc_optd_uth_claim;


--admit id for optd only
drop table dev.wc_uth_admission_id_optd; 

create table dev.wc_uth_admission_id_optd 
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_admission_id where data_source = 'optd'
distributed by (member_id_src);


--optd
--insert into data_warehouse.claim_header(
insert into dev.wc_claim_header_optd(
		data_source, 
		year,
		uth_claim_id, 
		uth_member_id,
	    from_date_of_service,
	    claim_type,
	    uth_admission_id,
		total_charge_amount, 
		total_allowed_amount, 
		total_paid_amount,
		claim_id_src,
		member_id_src,
		table_id_src,
		bill_type,
		fiscal_year, 
		cost_factor_year, 
		to_date_of_service
		)	
	select distinct on(b.uth_claim_id)
	'optd', 
	extract(year from (min(a.fst_dt) over(partition by b.uth_claim_id))),
	b.uth_claim_id,
	b.uth_member_id, 
	min(a.fst_dt) over(partition by b.uth_claim_id) as from_date_of_service,
	c.claim_type_code,
	d.uth_admission_id,
	sum((a.charge * c.cost_factor)) over(partition by b.uth_claim_id) as total_charge_amount,
	sum((a.std_cost * c.cost_factor)) over(partition by b.uth_claim_id) as total_allowed_amount, 
	null as total_paid_amount,
	a.clmid,
	a.patid::text, 
	'medical' as table_src,	
	a.bill_type,
	a.year as fiscal, 
	a.std_cost_yr::int as cost_year,
	max(a.lst_dt) over(partition by b.uth_claim_id) as to_date_of_service
from dev.wc_optd_medical a  --*optum_dod.medical a
    join dev.wc_optd_uth_claim b --data_warehouse.dim_uth_claim_id b 
		on a.patid::text = b.member_id_src 
		and a.clmid = b.claim_id_src
	join reference_tables.ref_optum_cost_factor c
		on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1)) 
		and c.standard_price_year = a.std_cost_yr::int
    left outer join dev.wc_uth_admission_id_optd d  --data_warehouse.dim_uth_admission_id d 
       on d.member_id_src = a.patid::text 
      and d.admission_id_src = a.conf_id 
      and d."year" = a."year" 
;



    --va
vacuum analyze dev.wc_claim_header_optd;

--remove existing records from claim header
delete from data_warehouse.claim_header where data_source = 'optd';


---load new records into claim header
insert into data_warehouse.claim_header 
select * from dev.wc_claim_header_optd;




--------------------------------------------------------------------------------------------------
--- ** OPTZ **
--------------------------------------------------------------------------------------------------

---working table
drop table if exists dev.wc_claim_header_optz;

create table dev.wc_claim_header_optz
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_header limit 0
distributed by (member_id_src);


---drop if exist
drop table if exists dev.wc_optz_medical;
create table dev.wc_optz_medical 
with(appendonly=true,orientation=column)
as select * from optum_zip.medical
distributed by (clmid );

---dim uth claim
drop table dev.wc_optz_uth_claim;
create table dev.wc_optz_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (claim_id_src);


vacuum analyze dev.wc_optz_uth_claim;

vacuum analyze dev.wc_optz_medical;


--optz admissions
create table dev.wc_uth_admission_id_optz 
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_admission_id where data_source = 'optz'
distributed by (member_id_src);


---optz claim header
insert into dev.wc_claim_header_optz(
		data_source, 
		year,
		uth_claim_id, 
		uth_member_id,
	    from_date_of_service,
	    claim_type,
	    uth_admission_id,
		total_charge_amount, 
		total_allowed_amount, 
		total_paid_amount,
		claim_id_src,
		member_id_src,
		table_id_src,
		bill_type,
		fiscal_year, 
		cost_factor_year, 
		to_date_of_service
		)
	select distinct on(b.uth_claim_id)
	'optz', 
	extract(year from (min(a.fst_dt) over(partition by b.uth_claim_id))),
	b.uth_claim_id,
	b.uth_member_id, 
	min(a.fst_dt) over(partition by b.uth_claim_id) as from_date_of_service,
	c.claim_type_code,
	d.uth_admission_id,
	sum((a.charge * c.cost_factor)) over(partition by b.uth_claim_id) as total_charge_amount,
	sum((a.std_cost * c.cost_factor)) over(partition by b.uth_claim_id) as total_allowed_amount, 
	null as total_paid_amount,
	a.clmid,
	a.patid::text, 
	'medical' as table_src,	
	a.bill_type,
	a.year as fiscal, 
	a.std_cost_yr::int as cost_year,
	max(a.lst_dt) over(partition by b.uth_claim_id) as to_date_of_service
from dev.wc_optz_medical a    --*from optum_zip.medical a
    join dev.wc_optz_uth_claim b   --*data_warehouse.dim_uth_claim_id b
		on a.patid::text = b.member_id_src 
		and a.clmid = b.claim_id_src
	join reference_tables.ref_optum_cost_factor c 
	    on c.service_type = left(a.tos_cd, (position('.' in a.tos_cd)-1)) 
	   and c.standard_price_year = a.std_cost_yr::int
    left outer join dev.wc_uth_admission_id_optz d    --*data_warehouse.dim_uth_admission_id d
       on d.member_id_src = a.patid::text 
      and d.admission_id_src = a.conf_id 
      and d."year" = a."year" 
     ;
    
    
    select * from dev.wc_claim_header_optz ch where data_source = 'optz' and bill_type is not null;

   
   select * from optum_zip.medical m where bill_type is not null;
    
    --va
vacuum analyze dev.wc_claim_header_optz;

select count(*), count(distinct uth_cla
im_id), year from dev.wc_claim_header_optz group by year order by year;


select count(*), count(distinct clmid), year from optum_zip.medical m group by year order by year;


select count(*), count(distinct uth_claim_id), data_year from data_warehouse.dim_uth_claim_id duci where data_source = 'optz'
group by data_year order by data_year 


select a.*, b.*
from data_warehouse.dim_uth_claim_id a 
left outer join optum_zip.medical b 
   on a.claim_id_src = b.clmid 
  and a.member_id_src = b.patid::text 
where b.patid is null 
  and a.data_source = 'optz'
;


--remove existing records from claim header
delete from data_warehouse.claim_header where data_source = 'optz';





---load new records into claim header
insert into data_warehouse.claim_header 
select * from dev.wc_claim_header_optz;

--va
vacuum analyze data_warehouse.claim_header;


select * from data_warehouse.claim_header ch where data_source = 'optd' and bill_type is not null;


--validate
select count(*), count(distinct uth_claim_id), data_source
from data_warehouse.claim_header 
group by data_source; 


--validate
select count(*), count(distinct uth_claim_id), data_source, year 
from data_warehouse.claim_header 
group by data_source, year 
order by data_source , year ; 


----- optd *CLEANUP
drop table dev.wc_claim_header_optd;

drop table dev.wc_optd_uth_claim;

drop table dev.wc_optd_medical;

drop table dev.wc_uth_admission_id_optd ;


---optz *CLEANUP
drop table dev.wc_claim_header_optz;

drop table dev.wc_optz_uth_claim;

drop table dev.wc_optz_medical;

drop table dev.wc_uth_admission_id_optz ;
