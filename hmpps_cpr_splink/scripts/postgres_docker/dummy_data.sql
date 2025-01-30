-- create some dummy data
CREATE TABLE person (
    id TEXT PRIMARY KEY,

    name_1_std TEXT,
    name_2_std TEXT,
    name_3_std TEXT,
    last_name_std TEXT,
    first_and_last_name_std TEXT,
    forename_std_arr TEXT [],
    last_name_std_arr TEXT [],

    sentence_date_single DATE,
    sentence_date_arr DATE [],

    date_of_birth DATE,
    date_of_birth_arr DATE [],

    postcode_arr TEXT [],
    postcode_outcode_arr TEXT [],

    cro_single TEXT,
    pnc_single TEXT,
    crn TEXT,
    prison_number TEXT,

    source_system TEXT

);

-- some values we can work with
-- not been explicitly run through cleaning
-- so may not be conistent
INSERT INTO
    person (
        id,

        name_1_std,
        name_2_std,
        last_name_std,
        first_and_last_name_std,
        forename_std_arr,
        last_name_std_arr,

        sentence_date_arr,

        date_of_birth,
        date_of_birth_arr,

        postcode_arr,
        postcode_outcode_arr,

        cro_single,
        pnc_single,

        source_system
    )
VALUES
    (
        1,
        'FORENAME',
        'MIDDLE',
        'LAST',
        'FORENAME LAST',
        ARRAY ['FORENAME', 'MIDDLE', 'ANOTHER'],
        ARRAY ['LAST'],
        ARRAY [DATE '1992-10-15', DATE '1999-12-03'],
        DATE '1974-02-16',
        ARRAY [DATE '1974-02-16', DATE '1974-06-16'],
        ARRAY ['AB11YZ'],
        ARRAY ['AB1'],
        'CRO1',
        'PNC1',
        'DELIUS'
    ),
    (
        2,
        'FIRSTS',
        NULL,
        'LASTS',
        'FIRSTS LASTS',
        ARRAY ['FIRSTS'],
        ARRAY ['LASTS'],
        ARRAY [DATE '2013-04-02'],
        DATE '1986-05-21',
        ARRAY [DATE '1986-05-21'],
        ARRAY ['AB23WV'],
        ARRAY ['AB2'],
        'CRO2',
        NULL,
        'NOMIS'
    ),
    (
        3,
        'ZED',
        NULL,
        'ZEE',
        'ZED ZEE',
        ARRAY ['ZED'],
        ARRAY ['ZEE'],
        ARRAY [DATE '2024-01-04'],
        DATE '1986-05-21',
        ARRAY [DATE '1986-05-21'],
        ARRAY ['AB34QE'],
        ARRAY ['AB3'],
        'CRO2',
        'PNC3',
        'DELIUS'
    )
    ;
