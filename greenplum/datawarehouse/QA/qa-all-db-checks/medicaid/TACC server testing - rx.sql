select * from dw_staging.pharmacy_claims limit 5;

select table_id_src, count(distinct rx_claim_id_src) from dw_staging.pharmacy_claims
group by table_id_src;

select table_id_src, count(rx_claim_id_src) from dw_staging.pharmacy_claims
group by table_id_src;

select rx_claim_id_src, count(rx_claim_id_src) from dw_staging.pharmacy_claims
group by rx_claim_id_src
having count(rx_claim_id_src) > 1
order by count(rx_claim_id_src) desc
limit 10;

/*
rx_claim_id_src		 			count
5179231440018730133020130814	1496
5274808540000000000020140314	1443
5212292880000000000020131116	1050
5273894430000000000020111020	650
5202849000078133430920140730	625
6092604565254405502820110909	612
5157148860017306425520140214	600
5115954670078110710120111219	564
5301058120000000000020120216	551
5247715760090419828020170609	551 */

select * from dw_staging.pharmacy_claims
where rx_claim_id_src = '5179231440018730133020130814';


select * from medicaid.ffs_rx where pcn = '517923144' and ndc = '00187301330' order by rx_dt;





529014218
2013-06-10
45802011222

select * from medicaid.mco_rx where
pcn = '529014218' and rx_fill_dt = '2013-06-10'::date and ndc = '45802011222';


select * from medicaid.ffs_rx where
pcn = '529014218' and rx_fill_dt = '2013-06-10'::date and ndc = '45802011222';





