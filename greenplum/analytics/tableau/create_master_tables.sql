drop table if exists tableau.master_claims;

create table tableau.master_claims
(
data_source text,
year int,
uth_member_id int,
uth_claim_id numeric,
claim_type text,
total_charge_amount numeric,
total_allowed_amount numeric,
total_paid_amount numeric,
dx1 text,
dx2 text,
dx3 text,
dx4 text,
dx5 text,
dx6 text,
dx7 text,
dx8 text,
dx9 text,
dx10 text
)
with (
        appendonly=true, 
        orientation=column, 
        compresstype=zlib, 
        compresslevel=5 
    )
distributed by (uth_member_id)
partition by list(data_source)
(
    partition optz values ('optz'),
    partition truv values ('truv'),
    partition mcrt values ('mcrt'),
    partition mcrn values ('mcrn'),
    partition mdcd values ('mdcd'),
    partition mhtw values ('mhtw'),
    partition mcpp values ('mcpp')
)
;

drop table if exists tableau.master_enrollment;

create table tableau.master_enrollment
(
data_source bpchar(4),
year int,
uth_member_id int,
gender_cd bpchar(1),
race_cd bpchar(1),
age_derived int,
state text,
msa int,
plan_type text,
bus_cd bpchar(4),
total_enrolled_months int,
aimm int,
ami int,
ca int,
cfib int,
chf int,
ckd int,
cliv int,
copd int,
cysf int,
dep int,
epi int,
fbm int,
hemo int,
hep int,
hiv int,
ihd int,
lbp int,
lymp int,
ms int,
nicu int,
pain int,
park int,
pneu int,
ra int,
scd int,
smi int,
str int,
tbi int,
trans int,
trau int,
asth int,
dem int,
diab int,
htn int,
opi int,
tob int,
crg text,
crg_abbreviated bpchar(2),
covid_severity int
)
with (appendoptimized=true, orientation=column, compresstype=zlib)
distributed by (uth_member_id)
partition by list(data_source)
(
    partition optz values ('optz'),
    partition truv values ('truv'),
    partition mcrt values ('mcrt'),
    partition mcrn values ('mcrn'),
    partition mdcd values ('mdcd'),
    partition mhtw values ('mhtw'),
    partition mcpp values ('mcpp')
)
;

vacuum analyze tableau.master_claims;
vacuum analyze tableau.master_enrollment;