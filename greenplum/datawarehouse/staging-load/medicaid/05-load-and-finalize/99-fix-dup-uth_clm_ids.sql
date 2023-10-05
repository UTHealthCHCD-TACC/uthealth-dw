/***************************************************
 * Apparently there are duplicated claims in the dim scripts (>1 uth_claim_id mapping to a single claim_id_src)
 * I'm not entirely sure how or why but I'm going to fix it
 */


/*********************************
 * First see how bad the problem is
 */

--claims
select data_year, data_source,
	count(distinct claim_id_src) as distinct_claims,
	count(*) as count,
	count(*) * 1.0 / count(distinct claim_id_src) as pct_inflation
from data_warehouse.dim_uth_claim_id
where data_source in ('mdcd', 'mhtw', 'mcpp')
group by 1, 2;

2011	mdcd	18754846	18754846	1.00000000000000000000
2012	mdcd	56903930	56903930	1.00000000000000000000
2013	mdcd	88016520	88016520	1.00000000000000000000
2014	mdcd	89176837	89176837	1.00000000000000000000
2015	mdcd	92492859	92492859	1.00000000000000000000
2016	mdcd	98501568	98501568	1.00000000000000000000
2017	mdcd	106362630	106362631	1.00000000940179835719
2018	mdcd	109416099	109416107	1.00000007311538313937
2019	mdcd	111779745	111779745	1.00000000000000000000
2020	mdcd	103737200	103737200	1.00000000000000000000
2021	mdcd	114635582	114715971	1.00070125696225801863
2022	mdcd	77967379	78137442	1.0021812070917505

--rx claims
select "year", data_source,
	count(distinct rx_claim_id_src) as distinct_rx_claims,
	count(*) as count,
	count(*) * 1.0 / count(distinct rx_claim_id_src) as pct_inflation
from data_warehouse.dim_uth_rx_claim_id
where data_source in ('mdcd', 'mhtw', 'mcpp')
group by 1, 2;

2011	mdcd	2653	5306	2.0000000000000000
2012	mdcd	38289624	38293217	1.0000938374323028
2013	mdcd	41436687	41438917	1.00005381704381916440
2014	mdcd	40291088	40293164	1.00005152504196461510
2015	mdcd	41425811	41427223	1.00003408502974148171
2016	mdcd	40232535	40233611	1.00002674452405248638
2017	mdcd	39713413	39714241	1.00002084937902466353
2018	mdcd	39971540	39972562	1.00002556819176844325
2019	mdcd	38094491	38095181	1.00001811285521573185
2020	mdcd	32984744	32984910	1.00000503262963023148
2021	mhtw	54819	109638	2.0000000000000000
2021	mcpp	57661	115322	2.0000000000000000
2021	mdcd	43584627	55770156	1.2795831888156345
2022	mcpp	121206	242412	2.0000000000000000
2022	mhtw	92652	185304	2.0000000000000000
2022	mdcd	23919004	47838008	2.0000000000000000

--yes that be a problem

/********************
 * ID the rows to delete
 */

--claims
drop table if exists dw_staging.uth_claim_ids_to_delete;

create table dw_staging.uth_claim_ids_to_delete as
with r as (
	select claim_id_src,
		uth_claim_id,
		row_number() over (partition by claim_id_src order by uth_claim_id) as rn 
	from data_warehouse.dim_uth_claim_id
	where data_source in ('mdcd', 'mhtw', 'mcpp')
)
select * from r
where rn > 1;

analyze dw_staging.uth_claim_ids_to_delete;

--select * from dw_staging.uth_claim_ids_to_delete;
--select count(*) from dw_staging.uth_claim_ids_to_delete;
--250461


--rx claims
drop table if exists dw_staging.uth_rx_claim_ids_to_delete;

create table dw_staging.uth_rx_claim_ids_to_delete as
with r as (
	select rx_claim_id_src,
		uth_rx_claim_id,
		row_number() over (partition by rx_claim_id_src order by uth_rx_claim_id) as rn 
	from data_warehouse.dim_uth_rx_claim_id
	where data_source in ('mdcd', 'mhtw', 'mcpp')
)
select * from r
where rn > 1;

analyze dw_staging.uth_rx_claim_ids_to_delete;

--select * from dw_staging.uth_rx_claim_ids_to_delete;
--select count(*) from dw_staging.uth_rx_claim_ids_to_delete;
--36446617

/**************************************
 * Delete duplicated rows - claims
 **************************************/

--dim table
delete from data_warehouse.dim_uth_claim_id a
using dw_staging.uth_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_claim_id = b.uth_claim_id;

--claim_header
delete from data_warehouse.claim_header a
using dw_staging.uth_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_claim_id = b.uth_claim_id;

--claim_detail
delete from data_warehouse.claim_detail a
using dw_staging.uth_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_claim_id = b.uth_claim_id;

--claim_diag
delete from data_warehouse.claim_diag a
using dw_staging.uth_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_claim_id = b.uth_claim_id;

--claim_icd_proc
delete from data_warehouse.claim_icd_proc a
using dw_staging.uth_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_claim_id = b.uth_claim_id;

--vacuum analyze all of them
vacuum analyze data_warehouse.dim_uth_claim_id;
vacuum analyze data_warehouse.claim_header;
vacuum analyze data_warehouse.claim_detail;
vacuum analyze data_warehouse.claim_diag;
vacuum analyze data_warehouse.claim_icd_proc;

/**************************************
 * Delete duplicated rows - rx
 **************************************/

--dim table
delete from data_warehouse.dim_uth_rx_claim_id a
using dw_staging.uth_rx_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_rx_claim_id = b.uth_rx_claim_id;

--pharmacy_claims
delete from data_warehouse.pharmacy_claims a
using dw_staging.uth_rx_claim_ids_to_delete b
where a.data_source in ('mdcd', 'mhtw', 'mcpp')
and a.uth_rx_claim_id = b.uth_rx_claim_id;

--vacuum it
vacuum analyze data_warehouse.dim_uth_rx_claim_id;
vacuum analyze data_warehouse.pharmacy_claims;


/**************************
 * Check again
 */

/*********************************
 * First see how bad the problem is
 */

--claims
select data_year, data_source,
	count(distinct claim_id_src) as distinct_claims,
	count(*) as count,
	count(*) * 1.0 / count(distinct claim_id_src) as pct_inflation
from data_warehouse.dim_uth_claim_id
where data_source in ('mdcd', 'mhtw', 'mcpp')
group by 1, 2
order by 1, 2;

2011	mdcd	18754846	18754846	1.00000000000000000000
2012	mdcd	56903930	56903930	1.00000000000000000000
2013	mdcd	88016520	88016520	1.00000000000000000000
2014	mdcd	89176837	89176837	1.00000000000000000000
2015	mdcd	92492859	92492859	1.00000000000000000000
2016	mdcd	98501568	98501568	1.00000000000000000000
2017	mdcd	106362630	106362630	1.00000000000000000000
2018	mdcd	109416099	109416099	1.00000000000000000000
2019	mdcd	111779745	111779745	1.00000000000000000000
2020	mdcd	103737200	103737200	1.00000000000000000000
2021	mdcd	114635582	114635582	1.00000000000000000000
2022	mdcd	77967379	77967379	1.00000000000000000000

--rx claims
select "year", data_source,
	count(distinct rx_claim_id_src) as distinct_rx_claims,
	count(*) as count,
	count(*) * 1.0 / count(distinct rx_claim_id_src) as pct_inflation
from data_warehouse.dim_uth_rx_claim_id
where data_source in ('mdcd', 'mhtw', 'mcpp')
group by 1, 2
order by 1, 2;

2011	mdcd	2653	2653	1.00000000000000000000
2012	mdcd	38289624	38289624	1.00000000000000000000
2013	mdcd	41436687	41436687	1.00000000000000000000
2014	mdcd	40291088	40291088	1.00000000000000000000
2015	mdcd	41425811	41425811	1.00000000000000000000
2016	mdcd	40232535	40232535	1.00000000000000000000
2017	mdcd	39713413	39713413	1.00000000000000000000
2018	mdcd	39971540	39971540	1.00000000000000000000
2019	mdcd	38094491	38094491	1.00000000000000000000
2020	mdcd	32984744	32984744	1.00000000000000000000
2021	mcpp	57661	57661	1.00000000000000000000
2021	mdcd	43584627	43584627	1.00000000000000000000
2021	mhtw	54819	54819	1.00000000000000000000
2022	mcpp	121206	121206	1.00000000000000000000
2022	mdcd	23919004	23919004	1.00000000000000000000
2022	mhtw	92652	92652	1.00000000000000000000

--boom, baby. ...but the data sources need fixing

/*************************
 * Drop temp tables
 */

drop table if exists dw_staging.uth_claim_ids_to_delete;
drop table if exists dw_staging.uth_rx_claim_ids_to_delete;



