

drop table dev.claim_detail_v1;

create table dev.claim_detail_v1 (  
		id bigserial,
		data_source char(4),
		uth_claim_id bigint, 
		claim_id_src varchar,
		claim_sequence_number int4,
		uth_member_id bigint, 
		provider_id int,
		first_date_of_service date, 
		month_year_id int4, 
		procedure_code text, 
		proc_mod_1 char, 
		proc_mod_2 char, 
		revenue_code char,
		billed_charges float,
		allowed_amount float, 
		copay float, 
		deductible float, 
		coins float, 
		cob float, 
		paid_amount float	
);



select distinct data_source, substring(month_year_id::text,1,4)
from data_warehouse.member_enrollment_monthly
where data_source = 'mdcr';


create table truven.ccaeo_wc as select distinct * from truven.ccaeo;

select * from truven.ccaeo;
		

select *
from truven.ccaeo_wc
where msclmid = 753984668
and year = 2015
and enrolid = 224038101;


select count(*) 
from 
(
select distinct *
from truven.ccaeo_wc
) a;



drop function claim_detail_build();

create or replace function claim_detail_build () returns void
as $FUNC$ 
declare
	r_data_source text;
	r_claim_id_src int;
	r_seqnum int;
	r_year int;
	r_svcdate date;
	r_month_year_id int; 
	r_enrolid bigint;
	r_uth_member_id bigint;
	r_allowed_amount numeric; 
	r_paid_amount numeric;
	r_deductible numeric;
	r_copay numeric;
	r_coins numeric; 
	r_cob numeric;	
begin
	for r_data_source, r_claim_id_src, r_seqnum, r_year, r_svcdate, r_enrolid,
		r_allowed_amount, r_paid_amount, r_deductible, r_copay, r_coins, r_cob
	 in 
		select 'trvc', msclmid, seqnum, "year", svcdate, enrolid,
		       netpay, pay, deduct, copay, coins, cob 
		from truven.ccaeo_wc a 
		where msclmid is not null
		limit 10
	loop 	
		select month_year_id into r_month_year_id from data_warehouse.ref_month_year where month_int = extract(month from r_svcdate) and year_int = r_year;
		select uth_member_id into r_uth_member_id from data_warehouse.dim_member_id_src where data_source = 'trvc' and trunc(member_id_src::numeric,0) = r_enrolid;
		--if xxx then
		
	
		insert into dev.claim_detail_v1 (data_source, claim_id_src, first_date_of_service, month_year_id, uth_member_id ) 
		                         values (r_data_source, r_claim_id_src::text, r_svcdate, r_month_year_id, r_uth_member_id );
		--end if;
	end loop;	
end $FUNC$
language 'plpgsql';


select claim_detail_build();


select * from dev.claim_detail_v1;

delete from dev.claim_detail_v1;
		