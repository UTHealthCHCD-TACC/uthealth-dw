------------------------------------------------------------------------------------------
--Truven
------------------------------------------------------------------------------------------
--add records to dim_uth_provider_id
--ccaef -- get provider id
insert into dev.am_dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text 
	from truven.ccaef p
	left join dev.am_dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0; 
	
--ccaes -- get provider id
insert into dev.am_dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text  	
	from truven.ccaes p
	left join dev.am_dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0;

--ccaeo -- get provider id
insert into dev.am_dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.provid as numeric(10,0))::text 
	from truven.ccaeo p
	left join dev.am_dim_uth_provider_id d on d.provider_id_src = cast(p.provid as numeric(10,0))::text  and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.provid is not null
		and p.provid > 0;	

--ccaei -- get physician id	
insert into dev.am_dim_uth_provider_id (data_source, provider_id_src)    	
select distinct 'truv' as data_source, cast(p.physid as numeric(10,0))::text 
	from truven.ccaei p
	left join dev.am_dim_uth_provider_id d on d.provider_id_src = cast(p.physid as numeric(10,0))::text  and d.data_source = 'truv'
	where d.uth_provider_id is null
		and p.physid is not null
		and p.physid > 0;			
	
vacuum analyze dev.am_dim_uth_provider_id;


--select count(*) from dev.am_dim_uth_provider_id where data_source = 'truv'
--2364650

--select count( distinct provid)
--from 
--	(
--		select distinct cast(provid as numeric(10,0))::text as provid  from truven.ccaef 
--		union 
--		select distinct cast(provid as numeric(10,0))::text as provid  from truven.ccaeo 
--		union 
--		select distinct cast(provid as numeric(10,0))::text as provid  from truven.ccaes
--	)x
--2364651
 
-----------------------------------------------------------------------------------------
--add to provider table
insert into dev.am_provider (data_source, uth_provider_id, provider_id_src )
select distinct d.data_source, d.uth_provider_id , d.provider_id_src 
	from dev.am_dim_uth_provider_id d
	left join dev.am_provider p2 on p2.uth_provider_id = d.uth_provider_id 
	where d.data_source = 'truv'
		and p2.uth_provider_id is null;	
	
vacuum analyze dev.am_provider;

--select count(*) from dev.am_provider ap where ap.data_source = 'truv'
--2364650	
-----------------------------------------------------------------------------------------
--replace provid to uth_provider_id
drop table if exists dev.am_claim_detail_provider_src1;	

--ccaeo
select 'truv' as data_source, "year" as data_year, 
		enrolid::text as member_id_src , msclmid::text as claim_id_src , 
		seqnum  as claim_sequence_number, svcdate as from_date_of_service,
		d.uth_provider_id as bill_provider
	into dev.am_claim_detail_provider_src1
	from truven.ccaeo t 
	inner join dev.am_dim_uth_provider_id d on d.provider_id_src = cast(t.provid as numeric(10,0))::text
	where t.provid is not null
		and d.data_source = 'truv';	
	
--ccaes
insert into dev.am_claim_detail_provider_src1 	
select 'truv' as data_source, "year" as data_year, 
		enrolid::text as member_id_src , msclmid::text as claim_id_src , 
		seqnum  as claim_sequence_number, svcdate as from_date_of_service,
		d.uth_provider_id as bill_provider	
	from truven.ccaes t 
	inner join dev.am_dim_uth_provider_id d on d.provider_id_src = cast(t.provid as numeric(10,0))::text
	where t.provid is not null
		and d.data_source = 'truv';		
	
--select * from dev.am_claim_detail_provider_src1	
	
-----------------------------------------------------------------------------------------
--create sequence number 	
drop table if exists dev.am_claim_detail_provider_src2;	
select data_source, data_year, member_id_src, claim_id_src, 
	row_number () over (
			partition by data_year, member_id_src, claim_id_src 
			order by claim_sequence_number
	) as claim_sequence_number,
	from_date_of_service, bill_provider	
into dev.am_claim_detail_provider_src2
from dev.am_claim_detail_provider_src1;

--select * from dev.am_claim_detail_provider_src2	order by data_year, member_id_src, claim_id_src , claim_sequence_number
------------------------------------------------------------------------------------------------------------	
--delete old data
 delete from dev.am_claim_detail_provider
 	where data_source = 'truv'		
------------------------------------------------------------------------------------------------------------		
		
 --replace member_id_src and claim_id_src to uth_member_id and uth_claim_id respectively
insert into dev.am_claim_detail_provider(data_source , data_year , uth_member_id , uth_claim_id , claim_sequence_number , from_date_of_service ,
								 		  bill_provider )
 select s.data_source , s.data_year , d.uth_member_id , d.uth_claim_id , s.claim_sequence_number , s.from_date_of_service ,
 		s.bill_provider 
 	from dev.am_claim_detail_provider_src2 s
 	inner join data_warehouse.dim_uth_claim_id d on d.claim_id_src = s.claim_id_src 
 												and d.member_id_src = s.member_id_src 
 												and d.data_year = s.data_year 
	where d.data_source = 'truv'												

------------------------------------------------------------------------------------------------------------		
select count(*) from dev.am_claim_detail_provider where data_source = 'truv'	
	
 
		 