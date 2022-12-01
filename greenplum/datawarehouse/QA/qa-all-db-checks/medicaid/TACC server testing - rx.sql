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





--gets rx claim ids by year
select fiscal_year::text, count(distinct rx_claim_id_src) as src_id, count(distinct uth_rx_claim_id) as uth_id
from dw_staging.pharmacy_claims
group by fiscal_year
order by fiscal_year; 

select rx_claim_id_src from dw_staging.pharmacy_claims limit 10;

select rx_claim_id_src from dw_staging.pharmacy_claims where rx_claim_id_src = '6021958740057404120520120419'

--chip tables
A188857010006202041120120327 --exists
G966128030014398870120120307 --exists
6227739430007806080520160524 --exists

--ffs tables
5192372125099108141620120113 --does not exist
5312744256382402736420120126 --does not exist
5270947380012107590820120224 --does not exist

--mco tables
5051999955511101960520160105 --exists
5204011940017306822020190917 --exists
6021958740057404120520120419 --exists

--memid____ ndc________ date____
--527094738 00121075908 20120224 <--does not exist in dw_staging.pharmacy_claims
select pcn, ndc, rx_fill_dt from medicaid.ffs_rx where pcn = '527094738' and ndc = '00121075908'; --exists

select count(*) from dev.xz_dwqa_temp;
--3000

--UPLOADED RANDOM SELECTION OF CODES TO SERVER
drop table if exists dev.xz_dwqa_temp5;
select a.*, b.fiscal_year,
	case when b.rx_claim_id_src is not null then 0 else 1 end as missing_from_dw
into dev.xz_dwqa_temp5
from dev.xz_dwqa_temp a left join dw_staging.pharmacy_claims b on a.rx_id = b.rx_claim_id_src;

select * from dev.xz_dwqa_temp5;

select fy, sum(missing_from_dw) as missing, count(*) as total from dev.xz_dwqa_temp5
group by fy
order by fy;

select fiscal_year, sum(missing_from_dw) as missing, count(*) as total from dev.xz_dwqa_temp5
group by fiscal_year
order by fiscal_year;

select src_table, sum(missing_from_dw) as missing, count(*) as total  from dev.xz_dwqa_temp5
group by src_table
order by src_table;

select year_fy, count(*) from medicaid.ffs_rx
group by year_fy
order by year_fy;

--

select * from dev.xz_dwqa_rxid_list;

select a.* from dev.xz_dwqa_rxid_list a left join dw_staging.pharmacy_claims b
on a.rx_id = b.rx_claim_id_src
where b.rx_claim_id_src is null limit 10;

2012	6108673880000000000020120221 --does not exist, member_id not in enrollment table
2012	6108673880000000000020120221
2012	6108673880000000000020120221
2012	6108673880000000000020120221
2012	6093599003172202381020120329 --does not exist, member_id not in enrollment table
2012	5310540480022820299620120123 --does not exist, member_id not in enrollment table
2012	5213918206516206271120120306 --does not exist, member_id not in enrollment table
2012	6112652516057441140120120123 --does not exist, member_id not in enrollment table
2012	5194033321657104121020120801 --does not exist, member_id not in enrollment table

select fiscal_year, rx_claim_id_src from dw_staging.pharmacy_claims where rx_claim_id_src = '5310540480022820299620120123'
--does not exist

--memid____ ndc________ date____
--609359900 31722023810 20120329
--610867388 00000000000 20120221
--531054048 00228202996 20120123
--521391820 65162062711 20120306
--611265251 60574411401 20120123
--519403332 16571041210 20120801
select pcn, ndc, rx_fill_dt from medicaid.ffs_rx where pcn = '609359900' and ndc = '31722023810'; --exists

select * from dw_staging.member_enrollment_yearly where member_id_src = '519403332';

select * from dev.medicaid_dim_uth_rx_id limit 2;

SELECT ordinal_position, column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
where table_schema = 'dw_staging' and table_name = 'pharmacy_claims'
order by ordinal_position;

select count(*) from medicaid.htw_ffs_rx; --392868

select count(*) from dev.xz_dwqa_rxid_list; --392868

--this is from HTW
select a.rx_id
from dev.xz_dwqa_rxid_list a left join dw_staging.pharmacy_claims b on a.rx_id = b.rx_claim_id_src
where b.rx_claim_id_src is null; --only 3 missing

--somehow the HTW tables got loaded. Maybe they're in FFS?

--memid____ ndc________ date____
--712332912 57237000511 20181120 --not in enrollment
--729368962 59762453802 20181213
--712332912 50111033402 20181120

select * from dw_staging.member_enrollment_yearly where member_id_src = '712332912';

select * from dev.xz_dwqa_rxid_list limit 5;
--memid____ ndc________ date____
--218946705 51862000706 20171220
--220458005 00781707787 20180516
--221479505 60505015701 20171029
--222342919 00591521505 20180419
--222752404 67877019910 20180320

select * from dw_staging.pharmacy_claims where rx_claim_id_src = '2189467055186200070620171220';

select * from medicaid.ffs_rx where pcn = '218946705' and ndc = '51862000706';

--conclusion: HTW claims ARE loaded


--comparing across servers

select * from dev.xz_dwqa_temp1; --everything imported as text

drop table if exists dev.xz_dwqa_temp2;

select a.*,
b.fill_date,
b.ndc,
b.days_supply,
b.script_id,
b.refill_count,
b.quantity,
b.provider_npi,
b.pharmacy_id,
b.total_charge_amount,
b.total_paid_amount,
b.member_id_src,
b.fiscal_year,
case when b.fill_date is null and a.spc_fill_date is not null then 1 when b.fill_date::text != a.spc_fill_date then 1 else 0 end as fill_date_mismatch,
case when b.ndc is null and a.spc_ndc is not null then 1 when b.ndc::text != a.spc_ndc then 1 else 0 end as ndc_mismatch,
case when b.days_supply is null and a.spc_days_supply is not null then 1 when b.days_supply::text != a.spc_days_supply then 1 else 0 end as days_supply_mismatch,
case when b.script_id is null and a.spc_script_id is not null then 1 when b.script_id::text != a.spc_script_id then 1 else 0 end as script_id_mismatch,
case when b.refill_count is null and a.spc_refill_count is not null then 1 when b.refill_count::text != a.spc_refill_count then 1 else 0 end as refill_count_mismatch,
case when b.quantity is null and a.spc_quantity is not null then 1 when b.quantity::float != a.spc_quantity::float then 1 else 0 end as quantity_mismatch,
case when b.provider_npi is null and a.spc_provider_npi is not null then 1 when b.provider_npi::text != a.spc_provider_npi then 1 else 0 end as provider_npi_mismatch,
case when b.pharmacy_id is null and a.spc_pharmacy_id is not null then 1 when b.pharmacy_id::text != a.spc_pharmacy_id then 1 else 0 end as pharmacy_id_mismatch,
case when b.total_charge_amount is null and a.spc_total_charge_amount is not null then 1 when b.total_charge_amount::float != a.spc_total_charge_amount::float then 1 else 0 end as total_charge_amount_mismatch,
case when b.total_paid_amount is null and a.spc_total_paid_amount is not null then 1 when b.total_paid_amount::float != a.spc_total_paid_amount::float then 1 else 0 end as total_paid_amount_mismatch,
case when b.member_id_src is null and a.spc_member_id_src is not null then 1 when b.member_id_src::text != a.spc_member_id_src then 1 else 0 end as member_id_src_mismatch,
case when spc_fy = 'HTW' then 0 when b.fiscal_year is null and a.spc_fy is not null then 1 when b.fiscal_year::text != a.spc_fy then 1 else 0 end as fiscal_year_mismatch
into dev.xz_dwqa_temp2
from dev.xz_dwqa_temp1 a left join dw_staging.pharmacy_claims b
on a.rx_id = b.rx_claim_id_src and
	a.spc_total_charge_amount::float = b.total_charge_amount and
	a.spc_total_paid_amount::float = b.total_paid_amount and
	a.spc_refill_count = b.refill_count::text and
	a.spc_days_supply::int = b.days_supply and
	a.spc_script_id = b.script_id and
	a.spc_provider_npi = b.provider_npi and
	a.spc_pharmacy_id = b.pharmacy_id;

select * from dev.xz_dwqa_temp2;

select rx_id, spc_quantity, quantity, quantity_mismatch from dev.xz_dwqa_temp2
where quantity_mismatch = 1;

7084134702420809105520151112	3.500	4	1
5266103140008511320120170328	6.700	7	1
5305568740057403071620150528	144.000	225	1
6018733265926710000220210604	0.300	0	1
5284064955931005792220200817	8.500	9	1
5293337125931005792220180809	8.5	9	1
5230319505931005792220180212	8.500	8	1
5269826375926710000320210802	0.300	0	1
5287090805931005792220180215	8.500	8	1

select * from dw_staging.pharmacy_claims where rx_claim_id_src = '7084134702420809105520151112';
select * from dw_staging.pharmacy_claims where rx_claim_id_src = '5305568740057403071620150528';

select * from dev.xz_dwqa_temp2 where fiscal_year_mismatch = 1;
5219079435926710000220210529
5239935792930001370120160913
5126766895926710000220210319
5219079435926710000220210621
5230721745926710000220210621
5311051125967605801520210307
5216188245926710000220210709
5216181975926710000220210514
5221825705926710000220210713
G404755025274707116020120305

select * from dev.xz_dwqa_temp2 where rx_id = '5219079435926710000220210529';
select * from dw_staging.pharmacy_claims where rx_claim_id_src = '5219079435926710000220210529';
--they seem right but what doesn't match?

--spc values
year	table	filldate	ndc		days	script_id	spc_refill	qty
2021	ffs	2021-05-29	59267100002	1	000000012475	0	0.300		150086	86.01	40.00	521907943

select case when a

on a.rx_id = b.rx_claim_id_src and
	a.spc_total_charge_amount::float = b.total_charge_amount and
	a.spc_total_paid_amount::float = b.total_paid_amount and
	a.spc_refill_count = b.refill_count::text and
	a.spc_days_supply::int = b.days_supply and
	a.spc_script_id = b.script_id and
	a.spc_provider_npi = b.provider_npi and
	a.spc_pharmacy_id = b.pharmacy_id;

select rx_id, spc_total_charge_amount, total_charge_amount, total_charge_amount_mismatch from dev.xz_dwqa_temp2
where total_charge_amount_mismatch = 1;

select rx_id, spc_days_supply, days_supply, days_supply_mismatch from dev.xz_dwqa_temp2
where days_supply_mismatch = 1;

7198170534580202693720170824	7	28	1
J262055020006604943520120611	12	20	1
5104004070037833405320170117	3	21	1
5310095490009310104220150223	10	12	1
5259843080009303091220130628	19	24	1
5277310180078126840120150724	29	28	1
5275594490009363001220110910	60	30	1
5255034140011620011620190820	16	30	1

select * from dw_staging.pharmacy_claims where rx_claim_id_src = '7198170534580202693720170824';

select table_id_src, fiscal_year, member_id_src, ndc, fill_date, quantity from dw_staging.pharmacy_claims where rx_claim_id_src = '5302334066203709991020140102';


--checking on CHIP_FFS_RX

select * from data_warehouse.pharmacy_claims where rx_claim_id_src = 'A632897026340205100120120220'; --not in here
select * from dw_staging.pharmacy_claims where rx_claim_id_src = 'A632897026340205100120120220'; --not in here
select * from medicaid.ffs_rx where TCN = '12051200200029611'; --not in here
select * from medicaid.chip_rx where TCN = '12051200200029611';



