select discharge_status, count(*) 
from data_warehouse.claim_detail 
where discharge_status !~ '^\d{2}$' 
and discharge_status is not null
group by discharge_status order by discharge_status ;


select deductible, data_source from data_warehouse.claim_detail
where deductible is not null
order by deductible desc limit 50;

select cob_type, count(*) 
from data_warehouse.claim_detail 
group by cob_type ;

select units from data_warehouse.claim_detail group by units order by units desc;

select place_of_service , count(*) 
from data_warehouse.claim_detail 
group by place_of_service ;

/*

remove weird chars and decimals
`1	2
1  	204
1    	12688141
10	356177
1.0	2170368
10 	1
10   	4

*/
-----------------------------------
-----network_ind
------------------------------------

-----------------------------------
-----bill type class
------------------------------------


select cob from data_warehouse.claim_detail group by cob;

select cob from data_warehouse.claim_detail 
where cob is not null 
order by cob desc limit 50;


-----------------------------------
-----bill type class
------------------------------------

select bill_type_class from data_warehouse.claim_detail 
where bill_type_class !~ '^\d{1}$' and bill_type_class is not null;
/*

mdcd

C
C
P
 
H
H
H
H


blank values are there instead of NULL

*/


select bill_type_class, count(*) from data_warehouse.claim_detail 
where data_source = 'mdcd' and bill_type_class !~ '^\d{1}$' 
group by bill_type_class ;

/*
all single blank char ' '
|bill_type_class|count     |
|---------------|----------|
|               |1633502258|
|F              |1         |
*/

select bill_type_class, count(*) from data_warehouse.claim_detail 
where data_source = 'mdcd' and bill_type_class !~ '^\d{1}$' 
group by bill_type_class ;


-----------------------------------
-----bill type freq
------------------------------------

---- let that be, least important 



select bill_type_freq from data_warehouse.claim_detail 
where bill_type_freq !~ '^[a-zA-Z0-9]{1}$' and bill_type_freq is not null and data_source = 'truv'
group by bill_type_freq ;
--- getting single blank char as result 

select bill_type_freq from data_warehouse.claim_detail 
where bill_type_freq !~ '^[a-zA-Z0-9]{1}$' 
and bill_type_freq is not null and data_source in ('truv','mdcd')
group by bill_type_freq ;
--- getting single blank char as result 



-----------------------------------
-----bill type inst
------------------------------------

-- leave blank be medicaid, okay for others 

select bill_type_inst from data_warehouse.claim_detail 
where bill_type_inst !~ '^\d{1}$' and bill_type_inst is not null;

' ' 

-- medicaid
/*
C
C
C
N
F
F
F
F
F
F
F
F
F
*/

-----------------------------------
-----claim_sequence_number
------------------------------------

select claim_sequence_number from data_warehouse.claim_detail 
where claim_sequence_number not between '1' and '700' and claim_sequence_number is not null;
-- no outside values
-- optum script has null in select for claim_sequence_number 

----all null for truven
--- b/c not in insert script
--- will look at truven sequence number script for truven we have it, it was in archive 

-----------------------------------
-----claim_sequence_number_src
------------------------------------

---- don't validate 

select claim_sequence_number_src from data_warehouse.claim_detail 
where claim_sequence_number_src not between '1' and '700' and claim_id_src is not null;

---leading zeros ... 

select claim_sequence_number_src, data_source from data_warehouse.claim_detail 
where claim_sequence_number_src::int not between '0' and '700' and claim_sequence_number_src is not null;

select max(claim_sequence_number_src) from data_warehouse.claim_detail where data_source = 'optz';


-----------------------------------
-----place of service 
------------------------------------

--- change logic in QA and refresh see whats wrong with it 
--- check 10 because 10 is in reference table 
--- trim and left pad to take care of 1 that should be 01


select place_of_service , count(*) 
from data_warehouse.claim_detail 
group by place_of_service ;

/*

remove weird chars and decimals
`1  2
1   204
1       12688141
10  356177
1.0 2170368
10  1
10      4

*/






select discharge_status, count(*) 
from data_warehouse.claim_detail 
where discharge_status !~ '^\d{2}$' 
and discharge_status is not null
group by discharge_status order by discharge_status ;

-- trim and pad discharge status 
/*

#   257520504
0A  40
0C  4
0M  2
0P  90
0Y  10
1   451908119
2   18446464
3   43922047
4   1423962
5   2197013
6   73972078
+6  26
7   1924883
8   8100
9   533153
*/

--------------------
--cpt_hcpcs
--------------------

2006/01/01


select cpt_hcpcs from data_warehouse.claim_detail cd  where cpt_hcpcs !~ '^[[:alnum:]]{3,7}$'
and cpt_hcpcs is not null;
/*
GT
GT
RX
LT
91
25
550 0
RT
0 00
GP
0 00
GN
10 23
45 16
QW
GP
GO
GP
*/

select cpt_hcpcs from data_warehouse.claim_detail cd  where cpt_hcpcs !~ '^[[:alnum:]]{3,7}$'
and cpt_hcpcs is not null group by cpt_hcpcs ;

----- don't mess with it 

/*              
NA
NA
NA
NA
NA
NA
NA*/

select discharge_status from data_warehouse.claim_detail where discharge_status !~ '^\d{2}$' 
                    and discharge_status is not null;
  
select discharge_status from data_warehouse.claim_detail where discharge_status !~ '^\d{2}$' 
                    and discharge_status is not null and data_source = 'mcrn';                
                
--------------------
---drg
--------------------

  select drg_cd from data_warehouse.claim_detail where drg_cd !~ '^[[:alnum:]]{3,4}$'
                    and drg_cd is not null and data_source = 'truv';              



   select drg_cd from data_warehouse.claim_detail 
   where data_source = 'optd' and "year" = 2019 and drg_cd is not null;
   
   select drg_cd from data_warehouse.claim_detail 
   where drg_cd ~ '^[[:alnum:]]{4}$' and data_source = 'optz' and "year" = 2015;
   
   select drg_cd from data_warehouse.claim_detail 
   where drg_cd ~ '^[[:alnum:]]{4}$' and data_source = 'truv';
   
   select max(length(drg_cd)) from data_warehouse.claim_detail 
   where  drg_cd is not null;
   
--- exists only in optum --- fine that others dont have it 
                
                
