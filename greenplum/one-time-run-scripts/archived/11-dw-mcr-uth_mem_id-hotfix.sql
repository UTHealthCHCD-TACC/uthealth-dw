/******************
 * This script ended up not working out b/c uth_member_id needs to be unique and so by necessity even
 * for the same bene_id between medicare_texas and medicare_national they can't share the same
 * uth_member_id soooo we'll just keep things as they are
 */


/*********************************
 * This script addresses the issue where the same bene_id will generate 2 uth_member_ids
 * because said person exists in both medicare_texas and medicare_national
 * 
 * Algorithm:
 * 		1		BACKUP dim_uth_member_id
 * 		2		Make a table of all the medicare bene_id having > 1 uth_member_id
 * 		3		Change the medicare bene_id to that corresponding to mdcrt
 * 		4		Make edits to the dim_uth_member_id table
 * 		5		QA
 */

--back up dim_uth_member_id
drop table if exists backup.dim_uth_member_id;

create table backup.dim_uth_member_id as
select * from data_warehouse.dim_uth_member_id
distributed by (uth_member_id);

--copy out all the medicare bene_ids having >1 uth_member_id
drop table if exists dw_staging.mcr_dim_uth_member_id_hotfix;

create table dw_staging.mcr_dim_uth_member_id_hotfix as
select data_source, member_id_src, uth_member_id
from data_warehouse.dim_uth_member_id
where substring(data_source, 1, 3) = 'mcr' and
	member_id_src in (
		select member_id_src from data_warehouse.dim_uth_claim_id
		where substring(data_source, 1, 3) = 'mcr'
		group by member_id_src
		having count(distinct uth_member_id) > 1
	)
distributed by (member_id_src);

/***************************NOTE**********************
if you do having count(*) > 1 here the results are VERY weird
some groups will return upwards of several thousand rows
why? IDK. Isrrael and I looked at it, couldn't figure it out immediately,
and decided understanding it wasn't necessary for a solution
*****************************************************/

analyze dw_staging.mcr_dim_uth_member_id_hotfix;

--look at it
select * from dw_staging.mcr_dim_uth_member_id_hotfix
order by member_id_src;

/* 2 uth_member_ids per member_id_src
mcrn	ggggggBaaaaAngy	666470652
mcrt	ggggggBaaaaAngy	672159966
mcrt	ggggggBaaaafjug	671789245
mcrn	ggggggBaaaafjug	666416863
mcrn	ggggggBaaaajajw	666514801
mcrt	ggggggBaaaajajw	671649216
mcrn	ggggggBaaaauygf	533040216
mcrt	ggggggBaaaauygf	359356503
mcrt	ggggggBAaaBwBjj	753421211
mcrn	ggggggBAaaBwBjj	1185996223
mcrn	ggggggBaaajawnw	1185964805
mcrt	ggggggBaaajawnw	753283648
*/

update dw_staging.mcr_dim_uth_member_id_hotfix a
set uth_member_id = b.uth_member_id
from dw_staging.mcr_dim_uth_member_id_hotfix b
where a.member_id_src = b.member_id_src
and b.data_source = 'mcrt';

select * from dw_staging.mcr_dim_uth_member_id_hotfix
order by member_id_src;

/* Fixed
mcrn	ggggggBaaaaAngy	672159966
mcrt	ggggggBaaaaAngy	672159966
mcrt	ggggggBaaaafjug	671789245
mcrn	ggggggBaaaafjug	671789245
mcrn	ggggggBaaaajajw	671649216
mcrt	ggggggBaaaajajw	671649216
mcrn	ggggggBaaaauygf	359356503
mcrt	ggggggBaaaauygf	359356503
mcrt	ggggggBAaaBwBjj	753421211
mcrn	ggggggBAaaBwBjj	753421211
*/

--change the uth_member_ids in the live table
update data_warehouse.dim_uth_member_id a
set uth_member_id = b.uth_member_id
from dw_staging.mcr_dim_uth_member_id_hotfix b
where a.data_source = 'mcrn' and
	a.data_source = b.data_source and
	a.member_id_src = b.member_id_src;

/*THIS THROWS AN ERROR

SQL Error [23505]: ERROR: duplicate key value violates unique constraint "dim_uth_member_id_uth_member_id_key"  (seg16 129.114.52.61:6016 pid=68903)
  Detail: Key (uth_member_id)=(753219897) already exists.
  
  */
