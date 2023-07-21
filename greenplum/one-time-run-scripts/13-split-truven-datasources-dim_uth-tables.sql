/**************************************
 * This script backs up just the Truven portion of the dim_uth tables. It does the following things:
 * 
 * 1) Copies dim_uth_member_id to backup (just Truven portion) 
 * 2) Copies dim_uth_claim_id to backup (just Truven portion)
 * 3) Copies dim_uth_rx_id to backup (just Truven portion)
 * 4) Grants uthealth_analyst access to these tables
 * 
 * 07/19/23
 * Inserted all other data sources and changed table names to get rid of _truven
 */

--timestamp
select 'Truven dim_uth tables backup started at ' || current_timestamp as message;

drop table if exists backup.dim_uth_member_id_truven;
drop table if exists backup.dim_uth_claim_id_truven;
drop table if exists backup.dim_uth_rx_claim_id_truven;

create table backup.dim_uth_member_id_truven as
select * from data_warehouse.dim_uth_member_id
where data_source = 'truv';

create table backup.dim_uth_claim_id_truven as
select * from data_warehouse.dim_uth_claim_id
where data_source = 'truv';

create table backup.dim_uth_rx_claim_id_truven as
select * from data_warehouse.dim_uth_rx_claim_id
where data_source = 'truv';

--timestamp
select 'Truven dim_uth tables backup completed at ' || current_timestamp as message;

--grant access to uthealth_analyst
grant select on backup.dim_uth_member_id_truven to uthealth_analyst;
grant select on backup.dim_uth_claim_id_truven to uthealth_analyst;
grant select on backup.dim_uth_rx_claim_id_truven to uthealth_analyst;

/*************************************************
 * insert other data sources into backup.dim_uth_member_id_truven for Femi
 */

--dim_uth_member_id
do $$
declare
	data_sources text[] := array['mcpp', 'mcrn', 'mcrt', 'mdcd', 'mhtw', 'optd', 'optz'];
	data_source text;
begin
	foreach data_source in array data_sources
	loop
		raise notice 'Inserting data for % into dim_uth_member_id', data_source;
			execute 'insert into backup.dim_uth_member_id_truven
				select *
				from data_warehouse.dim_uth_member_id
				where data_source = ''' || data_source || ''';';
	end loop;
end $$;

vacuum analyze backup.dim_uth_member_id_truven;

--dim_uth_claim_id
do $$
declare
	data_sources text[] := array['mcpp', 'mcrn', 'mcrt', 'mdcd', 'mhtw', 'optd', 'optz'];
	data_source text;
begin
	foreach data_source in array data_sources
	loop
		raise notice 'Inserting data for % into dim_uth_claim_id', data_source;
			execute 'insert into backup.dim_uth_claim_id_truven
				select *
				from data_warehouse.dim_uth_claim_id
				where data_source = ''' || data_source || ''';';
	end loop;
end $$;

vacuum analyze backup.dim_uth_claim_id_truven;

--dim_uth_rx_claim_id
do $$
declare
	data_sources text[] := array['mcrn', 'mcrt', 'mdcd', 'optd', 'optz'];
	data_source text;
begin
	foreach data_source in array data_sources
	loop
		raise notice 'Inserting data for % into dim_uth_rx_claim_id', data_source;
			execute 'insert into backup.dim_uth_rx_claim_id_truven
				select *
				from data_warehouse.dim_uth_rx_claim_id
				where data_source = ''' || data_source || ''';';
	end loop;
end $$;

vacuum analyze backup.dim_uth_rx_claim_id_truven;


/************************************************
 * Rename tables - get rid of the _truven suffix
 */

alter table backup.dim_uth_member_id_truven rename to dim_uth_member_id;
alter table backup.dim_uth_claim_id_truven rename to dim_uth_claim_id;
alter table backup.dim_uth_rx_claim_id_truven rename to dim_uth_rx_claim_id;

/***********************
 * QA
 *
 */

--member_id
select data_source, count(*) from data_warehouse.dim_uth_member_id
group by data_source order by data_source;
mcrn	4084655
mcrt	5641308
mdcd	13263303
optd	75904567
optz	75904567
truc	139359265
trum	9800500

select data_source, count(*) from backup.dim_uth_member_id
group by data_source order by data_source;
data_source	count
mcrn	4084655
mcrt	5641308
mdcd	13263303
optd	75904567
optz	75904567
truv	146257193

--claim
select data_source, count(*) from data_warehouse.dim_uth_claim_id
group by data_source order by data_source;
mcrn	410527552
mcrt	544030798
mdcd	949947958
optd	3456189530
optz	3456193768
truc	3581916406
trum	713880807

select data_source, count(*) from backup.dim_uth_claim_id
group by data_source order by data_source;
mcrn	410527552
mcrt	544030798
mdcd	949947958
optd	3456189530
optz	3456193768
truv	4429126641

--rx
select data_source, count(*) from data_warehouse.dim_uth_rx_claim_id
group by data_source order by data_source;
data_source	count
mcrn	448661745
mcrt	565733804
mdcd	383825938
optd	2997384374
optz	2997719186
truc	2866022701
trum	669637792
truv	3483310002

select data_source, count(*) from backup.dim_uth_rx_claim_id_truven
group by data_source order by data_source;
mcrn	448661745
mcrt	565733804
mdcd	383825938
optd	2997384374
optz	2997719186
truv	3483310002

--delete 'truv' from rx table
delete from data_warehouse.dim_uth_rx_claim_id
where data_source = 'truv';

vacuum analyze data_warehouse.dim_uth_rx_claim_id;




