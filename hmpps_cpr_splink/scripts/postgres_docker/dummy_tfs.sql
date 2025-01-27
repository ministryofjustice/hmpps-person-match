CREATE TABLE tf_name_1_std AS (
    SELECT
        name_1_std,
        cast(count(*) as float8) /
            (select count(name_1_std) as total from cleaned_persons)
            AS tf_name_1_std
    from cleaned_persons
    where name_1_std is not null
    group by name_1_std
);
CREATE TABLE tf_name_2_std AS (
    SELECT
        name_2_std,
        cast(count(*) as float8) /
            (select count(name_2_std) as total from cleaned_persons)
            AS tf_name_2_std
    from cleaned_persons
    where name_2_std is not null
    group by name_2_std
);
CREATE TABLE tf_last_name_std AS (
    SELECT
        last_name_std ,
        cast(count(*) as float8) /
            (select count(last_name_std ) as total from cleaned_persons)
            AS tf_last_name_std 
    from cleaned_persons
    where last_name_std is not null
    group by last_name_std
);
CREATE TABLE tf_first_and_last_name_std AS (
    SELECT
        first_and_last_name_std,
        cast(count(*) as float8) /
            (select count(first_and_last_name_std) as total from cleaned_persons)
            AS tf_first_and_last_name_std
    from cleaned_persons
    where first_and_last_name_std is not null
    group by first_and_last_name_std
);
CREATE TABLE tf_date_of_birth AS (
    SELECT
        date_of_birth,
        cast(count(*) as float8) /
            (select count(date_of_birth) as total from cleaned_persons)
            AS tf_date_of_birth
    from cleaned_persons
    where date_of_birth is not null
    group by date_of_birth
);
CREATE TABLE tf_cro_single AS (
    SELECT
        cro_single,
        cast(count(*) as float8) /
            (select count(cro_single) as total from cleaned_persons)
            AS tf_cro_single
    from cleaned_persons
    where cro_single is not null
    group by cro_single
);
CREATE TABLE tf_pnc_single AS (
    SELECT
        pnc_single,
        cast(count(*) as float8) /
            (select count(pnc_single) as total from cleaned_persons)
            AS tf_pnc_single
    from cleaned_persons
    where pnc_single is not null
    group by pnc_single
);