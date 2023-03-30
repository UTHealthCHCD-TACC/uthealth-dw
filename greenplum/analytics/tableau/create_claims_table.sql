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
    (partition optz values ('optz'),
    partition truv values ('truv'),
    partition mcrt values ('mcrt'),
    partition mcrn values ('mcrn'),
    partition mdcd values ('mdcd')
    )
    ;

    analyze tableau.master_claims;