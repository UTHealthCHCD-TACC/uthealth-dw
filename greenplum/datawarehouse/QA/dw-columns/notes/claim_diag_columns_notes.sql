
select * from qa_reporting.claim_diag_column_checks 
order by test_var, data_source, "year" ;



---------
--diag_cd
---------


select diag_cd from data_warehouse.claim_diag 
where diag_cd !~ '^[[:alnum:]]{3,7}$'
                    and diag_cd is not null;
                -- trim extra chars on end? 
/*                 
9
1
2
4
7
V 440
V 440
V 440
V202-
3
3
3
3
67
3
67
M9903--
M9903--
M9903--
M9903--
M9
M9
M9
M9
M9
M9
M9
K625---
M9
J309---
R809---
2
4
V
2
4
A1
A1
02
02
02
02
02
02
02*/
    
                
---------------               
---icd type----
---------------

                
select icd_type from data_warehouse.claim_diag 
where icd_type != '10' and icd_type != '9'
                        and icd_type is not null
                    group by icd_type ;
                
   -- need some logic to fill this in where null 
                        
 /*                   
    5732634
3   2
0   125756352
O   3
5   1
2   1
9   146276745             
 */                 
                
-- truven + medicaid are using 0 instead of 10 
    
select icd_type, count(*)
from data_warehouse.claim_diag 
        where icd_type is not null and data_source = 'truv'
        group by icd_type ;

    -- assuming 0 may be stand in for 10
--0   7076485230
--9   1256875674
--
    
-- medicare it exists in bcarrier line file only which we are not using in dx load 
-- all null right now
select icd_type, count(*)
from data_warehouse.claim_diag 
        where icd_type is not null and data_source = 'mcrn'
        group by icd_type ;    
    
    
    
-- medicaid -- dw file currently uses prim_dx_qal
select prim_dx_qal, count(*) from medicaid.clm_dx 
group by prim_dx_qal; 
            
select * from medicaid.clm_dx where year_fy = 2019 and prim_dx_qal = '0';
-- looked at 2019 in source table was not correct - marked 9 are 10
/*

safe to assume the 9 values in 2020 are probably wrong and replace ??? 

|prim_dx_qal|prim_dx_cd|
|-----------|----------|
|9          |K036      |
|9          |K036      |
|9          |99204     |
|9          |F333      |
|9          |49321     |
|9          |K036      |
|9          |K036      |
|9          |I2510     |
|9          |K625      |
|9          |Z98818    |
|9          |N186      |

select year_fy , prim_dx_qal, count(*) from medicaid.clm_dx group by year_fy, prim_dx_qal;

|year_fy|prim_dx_qal|count   |
|-------|-----------|--------|
|2012   |0          |91      |
|2012   |           |3693945 |
|2012   |9          |41942922|
|2013   |0          |98      |
|2013   |5          |1       |
|2013   |           |481627  |
|2013   |9          |34117002|
|2014   |           |358793  |
|2014   |9          |34671282|
|2014   |0          |342     |
|2015   |           |233866  |
|2015   |9          |32770634|
|2015   |0          |15723   |
|2016   |           |255944  |
|2016   |2          |1       |
|2016   |O          |3       |
|2016   |9          |2773380 |
|2016   |0          |29368398|
|2017   |3          |2       |
|2017   |           |213231  |
|2017   |0          |27179629|
|2017   |9          |705     |
|2018   |9          |511     |
|2018   |           |180407  |
|2018   |0          |25315349|
|2019   |9          |153     |
|2019   |           |183052  |
|2019   |0          |24132265|
|2020   |           |131769  |
|2020   |9          |156     |
|2020   |0          |19744457|


*/
             
---------                    
-- poa---
---------

-- bring up in meeting 
-- present in truven.ccaei missing all for truven in DW -- ccaei not in dx load 
-- optum fine 
-- mcrn not in dw load file 
-- do we need to load this for other things? 

select poa_src, count(*)
    from data_warehouse.claim_diag cd
   where poa_src not in ('0', '1','Y','N','U') 
                        and poa_src is not null and poa_src not like ''
                    group by poa_src ;

select dx_poa_1 from medicaid.clm_dx where dx_poa_1 is not null and dx_poa_1  not like '';
                    
/*               

weird values mostly medicaid

M
E
3
J
 <<<<< these are just spaces of various sorts 
T
Y
4
N
Z
B
K

I
W
7
U
H
5
9
2
F
V                   
*/   
/*

J   25
    1973762565
T   175
4   75
Z   250
B   100
K   50
W   35639667
7   175
H   25
5   100
9   25
2   125
F   25
V   1150*/