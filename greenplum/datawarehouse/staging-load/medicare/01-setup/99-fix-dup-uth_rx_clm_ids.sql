/***************************************************
 * Apparently there are duplicated claims in the dim scripts (>1 uth_claim_id mapping to a single claim_id_src)
 * I'm not entirely sure how or why but I'm going to fix it
 */


/*********************************
 * First see how bad the problem is
 */

--rx claim ids
select "year", data_source,
	count(distinct rx_claim_id_src) as distinct_claims,
	count(*) as count,
	count(*) * 1.0 / count(distinct rx_claim_id_src) as pct_inflation
from data_warehouse.dim_uth_rx_claim_id
where data_source in ('mcrt', 'mcrn')
group by 1, 2 order by 2, 1;

/* code ran 11/6/24 after loading 2021/2022 - pass
2014	mcrn	71319334	71319334	1.00000000000000000000
2015	mcrn	73164346	73164346	1.00000000000000000000
2016	mcrn	75013227	75013227	1.00000000000000000000
2017	mcrn	76007531	76007531	1.00000000000000000000
2018	mcrn	76427412	76427412	1.00000000000000000000
2019	mcrn	76731485	76731485	1.00000000000000000000
2020	mcrn	76567254	76567254	1.00000000000000000000
2014	mcrt	89708569	89708569	1.00000000000000000000
2015	mcrt	91888165	91888165	1.00000000000000000000
2016	mcrt	94528666	94528666	1.00000000000000000000
2017	mcrt	94527213	94527213	1.00000000000000000000
2018	mcrt	96717861	96717861	1.00000000000000000000
2019	mcrt	98363330	98363330	1.00000000000000000000
2020	mcrt	102744159	102744159	1.00000000000000000000
2021	mcrt	100040289	100040289	1.00000000000000000000
2022	mcrt	103606333	103606333	1.00000000000000000000
*/

/********************
 * ID the rows to delete
 */

--rx claims
drop table if exists dw_staging.uth_rx_claim_ids_to_delete;

create table dw_staging.uth_rx_claim_ids_to_delete as
with r as (
	select rx_claim_id_src,
		uth_rx_claim_id,
		row_number() over (partition by rx_claim_id_src order by uth_rx_claim_id) as rn 
	from data_warehouse.dim_uth_rx_claim_id
	where data_source in ('mcrn')
)
select * from r
where rn > 1;

analyze dw_staging.uth_rx_claim_ids_to_delete;

--select * from dw_staging.uth_rx_claim_ids_to_delete;
--select count(*) from dw_staging.uth_rx_claim_ids_to_delete;
--1590

/**************************************
 * Delete duplicated rows - rx
 **************************************/

--dim table
delete from data_warehouse.dim_uth_rx_claim_id a
using dw_staging.uth_rx_claim_ids_to_delete b
where a.data_source in ('mcrn')
and a.uth_rx_claim_id = b.uth_rx_claim_id;

--pharmacy_claims
/* delete from data_warehouse.pharmacy_claims a
using dw_staging.uth_rx_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_rx_claim_id = b.uth_rx_claim_id; */

--vacuum it
vacuum analyze data_warehouse.dim_uth_rx_claim_id;
--vacuum analyze data_warehouse.pharmacy_claims;


/**************************
 * Check again
 */

--rx claims
select "year", data_source,
	count(distinct rx_claim_id_src) as distinct_rx_claims,
	count(*) as count,
	count(*) * 1.0 / count(distinct rx_claim_id_src) as pct_inflation
from data_warehouse.dim_uth_rx_claim_id
where data_source in ('mcrn')
group by 1, 2
order by 1, 2;

2014	mcrn	71319334	71319334	1.00000000000000000000
2015	mcrn	73164346	73164346	1.00000000000000000000
2016	mcrn	75013227	75013227	1.00000000000000000000
2017	mcrn	76007531	76007531	1.00000000000000000000
2018	mcrn	76427412	76427412	1.00000000000000000000
2019	mcrn	76731485	76731485	1.00000000000000000000

/*************************
 * Drop temp tables
 */

drop table if exists dw_staging.uth_rx_claim_ids_to_delete;



