create schema iqvia;

-- Transfer ownership to uthealth_admin role
alter schema iqvia owner to uthealth_admin;

-- Give everyone with uthealth_analyst role/permission with access to schema
grant usage on schema iqvia to uthealth_analyst;
