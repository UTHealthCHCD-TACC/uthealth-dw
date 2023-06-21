/**********************************
 * OPTUM QA 6/21/2023 - Prior to Xiaorui rewriting the code
 * 
 * Question: How messed up is it
 * 
 * Answer: VERY.
 */



/****************
 * Enrollment
 */

select count(distinct patid) from optum_dod.mbr_enroll_r;
--75815144

select count(distinct patid) from optum_zip.mbr_enroll;
--75815144

select count(distinct uth_member_id) from data_warehouse.member_enrollment_yearly_1_prt_optd;
--73357870

--what percentage is missing?
select (75815144 - 73357870)/75815144.0;
--0.03241138736081540648

--what about optz?
select count(distinct uth_member_id) from data_warehouse.member_enrollment_yearly_1_prt_optz;
--73359175

--percentage?
select (75815144 - 73359175)/75815144.0;
--0.03239417444092700002 uh-huh

/************
 * Claims
 */

select count(distinct clmid) from optum_dod.medical
where year between 2020 and 2021;
--444468632

select count(distinct clmid) from optum_zip.medical
where year between 2020 and 2021;
--444468632

--DW optum dod
select count(distinct uth_claim_id) from data_warehouse.claim_header_1_prt_optd
where "year" between 2020 and 2021;
--605081392

--what percentage?
select 605081392/444468632.0;
--1.3613590441181010

--DW optum zip
select count(distinct uth_claim_id) from data_warehouse.claim_header_1_prt_optz
where "year" between 2020 and 2021;
--611060083

select 611060083/444468632.0;
--1.3748103668202169
