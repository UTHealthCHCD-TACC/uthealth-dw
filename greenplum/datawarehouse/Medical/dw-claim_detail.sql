

drop table dev.claim_detail_v2;

create table dev.claim_detail_v2 (  
		data_source char(4),
		uth_claim_id numeric, 
		claim_id_src text,
		claim_sequence_number int4,
		uth_member_id bigint, 
		from_date_of_service date,
		to_date_of_service date,
		month_year_id int4, 
		perf_provider_id int,
		bill_provider_id int,
		ref_provider_id int,
		place_of_service int, 
		network_ind bool,
		network_paid_ind bool,
		admit_date date,
		discharge_date date,
		procedure_cd text,
		procedure_type text,
		proc_mod_1 char(1), 
		proc_mod_2 char(1), 
		revenue_code char (4),
		charge_amount numeric(13,2),
		allowed_amount numeric(13,2),
		paid_amount numeric(13,2),
		copay numeric(13,2),
		deductible numeric(13,2),
		coins numeric(13,2),
		cob numeric(13,2),	
		bill_type_inst char(1),
		bill_type_class char(1),
		bill_type_freq char(1),
		units int4,
		drg_cd text,  --drg vs mdc 
		drg_type text
) with (appendonly=true, orientation = column)
distributed by (uth_claim_id);


analyze dev.claim_detail_v2;

-----------------------------------------------------------------------------------------------

drop table dev.claim_header_v1;

create table dev.claim_header_v1 (
		data_source char(4),
		uth_claim_id numeric, 
		uth_member_id bigint, 
		admit_id text,
		
) with (appendonly=true, orientation = column)
distributed by (uth_claim_id);


analyze dev.claim_header_v1;



select dbo.set_all_perms();

-----------------------------------------------------------------------------------------------




		


select 'trvc', msclmid, seqnum, "year", svcdate, tsvcdat, enrolid,
       netpay, pay, deduct, copay, coins, cob,
       dx1, dx2, dx3, dx4, dxver,
       proc1, proctyp, procmod, revcode, 
       provid, stdplac, ntwkprov, paidntwk,
       qty, fachdid, facprof 
from truven.ccaeo_wc a 
where msclmid is not null
limit 10;


select count(*) from truven.ccaef_wc;


drop function claim_detail_build();

create or replace function claim_detail_build () returns void
as $FUNC$ 
declare
	r_data_source text; 
	r_uth_claim_id int; 
	r_claim_id_src int; 
	r_claim_seq int; 
	r_enrolid bigint; 
	r_uth_member_id bigint;
	r_year int; 
	r_from_date_of_service date; 
	r_to_date_of_service date; 
	r_month_year_id int; 
	r_perf_provider_id int; 
	r_bill_provider_id int;
	r_ref_provider_id int; 
	r_network_ind char(1); 
	r_network_paid_ind char(1); 
	r_procedure_code text; 
	r_procedure_type text; 
	r_procmod char(2);
	r_revenue_cd char(4); 
	r_charge_amount numeric; 
	r_allowed_amount numeric; 
	r_paid_amount numeric; 
	r_copay numeric; 
	r_deductible numeric; 
	r_coins numeric; 
	r_cob numeric;	
	r_bill_type char(3); 
	r_units int4; 
	r_drg_cd text; 
	r_drg_type char(4);
	r_dx1 text; 
	r_dx2 text; 
	r_dx3 text; 
	r_dx4 text; 
	r_dxver text; 
---
	r_fachdid numeric;
	r_facprof char(1);
	r_place_of_service numeric;
begin
	for r_data_source, r_claim_id_src, r_claim_seq, r_year, r_from_date_of_service, r_to_date_of_service, r_enrolid,
		r_allowed_amount, r_paid_amount, r_deductible, r_copay, r_coins, r_cob,
		r_procedure_code, r_procedure_type, r_procmod, r_revenue_cd,
		r_perf_provider_id, r_place_of_service, r_network_ind, r_network_paid_ind,
		r_units, r_fachdid, r_facprof,
		r_dx1, r_dx2, r_dx3, r_dx4, r_dxver
	 in 
		select 'trvx', msclmid, seqnum, "year", svcdate, tsvcdat, enrolid,
		       netpay, pay, deduct, copay, coins, cob,
		       proc1, proctyp, procmod, revcode, 
		       provid, stdplac, ntwkprov, paidntwk,
		       trunc(qty,0), fachdid, facprof, 
		       dx1, dx2, dx3, dx4, dxver
		from truven.ccaeo_wc a 
		where msclmid is not null
		limit 10
		--where year between 2015 and 2017
		
	loop 	
		select month_year_id into r_month_year_id from reference_tables.ref_month_year where month_int = extract(month from r_from_date_of_service) and year_int = r_year;
		select uth_member_id into r_uth_member_id from data_warehouse.dim_uth_member_id where data_source = 'trvc' and trunc(member_id_src::numeric,0) = r_enrolid;
		
		if r_fachdid is not null then
			select substring(billtyp,1,3) into r_bill_type from truven.ccaef_wc a where a.fachdid = r_fachdid;
		end if;
	
	
		insert into dev.claim_detail_v2 (data_source, uth_claim_id, claim_id_src, claim_sequence_number, 
										 uth_member_id, from_date_of_service, to_date_of_service, month_year_id, 
										 perf_provider_id, network_ind, network_paid_ind, 
										 procedure_cd, procedure_type, proc_mod_1, proc_mod_2, 
										 revenue_code, allowed_amount, paid_amount, 
										 deductible, copay, coins, cob, units,
										 bill_type_inst, bill_type_class, bill_type_freq
										 ) 
		                         values (r_data_source, (r_year::text || r_claim_id_src::text || r_uth_member_id::text )::numeric, r_claim_id_src, r_claim_seq, 
		                         		 r_uth_member_id, r_from_date_of_service, r_to_date_of_service, r_month_year_id, 
		                        		 r_perf_provider_id, r_network_ind::bool, r_network_paid_ind::bool,  
		                         		 r_procedure_code, r_procedure_type, substring(r_procmod,1,1), substring(r_procmod,2,1), 
		                        		 r_revenue_cd, r_allowed_amount, r_paid_amount, 
		                        		 r_deductible, r_copay, r_coins, r_cob, r_units,
		                        		 substring(r_bill_type,1,1), substring(r_bill_type,2,1), substring(r_bill_type,3,1)
		                        		 );
		
	end loop;	
end $FUNC$
language 'plpgsql';


select claim_detail_build();


select * from dev.claim_detail_v2 where data_source = 'trvx'; 



delete from dev.claim_detail_v2 where data_source = 'trvx';
		