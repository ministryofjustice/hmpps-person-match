-- create some dummy data
CREATE TABLE person (
    id INT PRIMARY KEY,
    first_name TEXT,
    middle_names TEXT,
    last_name TEXT,
    source_system TEXT,
    crn TEXT,
    prison_number TEXT,
    date_of_birth DATE,
    sex TEXT,
    ethnicity TEXT,
    first_name_alias_arr TEXT [],
    last_name_aliases TEXT []
);

CREATE TABLE pseudonym (
    id INT PRIMARY KEY,
    fk_person_id INT,
    first_name TEXT,
    last_name TEXT,
    date_of_birth DATE,
    FOREIGN KEY (fk_person_id) REFERENCES person (id)
);

CREATE TABLE address (
    id INT PRIMARY KEY,
    fk_person_id INT,
    postcode TEXT,
    FOREIGN KEY (fk_person_id) REFERENCES person (id)
);

CREATE TABLE reference (
    id INT PRIMARY KEY,
    fk_person_id INT,
    identifier_type TEXT,
    identifier_value TEXT,
    FOREIGN KEY (fk_person_id) REFERENCES person (id)
);

CREATE TABLE sentence_info (
    id INT PRIMARY KEY,
    fk_person_id INT,
    sentence_date DATE,
    FOREIGN KEY (fk_person_id) REFERENCES person (id)
);


INSERT INTO
    person (
        id,
        first_name,
        middle_names,
        last_name,
        source_system,
        crn,
        prison_number,
        date_of_birth,
        sex,
        ethnicity,
        first_name_alias_arr,
        last_name_aliases
    )
VALUES
    (
        1,
        'Forename',
        'some middle names',
        'surannma',
        'delius',
        NULL,
        NULL,
        DATE '1980-01-01',
        'M',
        'W1',
        ARRAY [] :: text [],
        NULL
    ),
    (
        2,
        'Johnny',
        'Jay Jerry',
        'jones',
        'nomis',
        'crn123',
        'pn42',
        DATE '1972-05-29',
        'M',
        'M1',
        ARRAY ['jon', 'john'],
        NULL
    ),
    (
        3,
        'for',
        '',
        'sname',
        'nomis',
        'crn123',
        NULL,
        DATE '1985-03-07',
        'F',
        'W2',
        NULL,
        ARRAY ['']
    ),
    (
        4,
        'will',
        NULL,
        'williams',
        'delius',
        'crn456',
        'pn11',
        DATE '1986-07-07',
        'M',
        'A3',
        NULL,
        ARRAY ['willamson']
    ),
    (
        5,
        'FRIST',
        NULL,
        'smith',
        'delius',
        NULL,
        'pn11',
        NULL,
        'M',
        'A3',
        ARRAY ['first', 'FIRST', 'DUPLICATE_FIRST', 'forts'],
        ARRAY ['smoth', 'smithe', 'smyth']
    ),
    (
        6,
        'ARthur',
        ' ben   charles  ',
        'davis',
        'nomis',
        'crn929',
        'pn11',
        DATE '1963-05-24',
        'M',
        'W1',
        ARRAY [' Art', 'Benjamin'],
        ARRAY ['davies']
    );

INSERT INTO
    pseudonym (
        id,
        fk_person_id,
        first_name,
        last_name,
        date_of_birth
    )
VALUES
    (
        1,
        1,
        'fore',
        NULL,
        NULL
    ),
    (
        2,
        1,
        'four',
        NULL,
        NULL
    ),
    (
        3,
        1,
        'fourname',
        NULL,
        DATE '1980-06-02'
    ),
    (
        4,
        4,
        'bill',
        'billiamson',
        DATE '1986-06-07'
    ),
    (
        5,
        4,
        NULL,
        NULL,
        DATE '1986-07-06'
    ),
    (
        6,
        4,
        'mig_error_bill',
        'duplicate_billiamson',
        NULL
    ),
    (
        7,
        5,
        NULL,
        'smeeth',
        NULL
    );

INSERT INTO
    address (
        id,
        fk_person_id,
        postcode
    )
VALUES
    (
        1,
        1,
        'SW1a 1Aa'
    ),
    (
        2,
        1,
        'SW1 1Aa'
    ),
    (
        3,
        2,
        'wc 2n 5 dn'
    ),
    (
        4,
        2,
        'wc2N 5dN'
    ),
    (
        5,
        3,
        'SE17PB'
    ),
    (
        6,
        4,
        'EC4R 3TN'
    ),
    (
        7,
        4,
        'NF1 1NF'
    ),
    (
        8,
        5,
        'NF1 1NF'
    ),
    (
        9,
        5,
        'SE1 7PB'
    ),
    (
        11,
        6,
        'SE13'
    ),
    (
        12,
        6,
        'SE1'
    ),
    (
        13,
        6,
        'SE'
    );

INSERT INTO
    reference (
        id,
        fk_person_id,
        identifier_type,
        identifier_value
    )
VALUES
    (
        1,
        1,
        'CRO',
        '000000/00Z'
    ),
    (
        2,
        1,
        'CRO',
        'CRO-1'
    ),
    (
        3,
        1,
        'CRO',
        NULL
    ),
    (
        4,
        1,
        'PNC',
        'PNC-1'
    ),
    (
        5,
        2,
        'PNC',
        'PNC-2'
    ),
    (
        6,
        2,
        'PNC',
        'PNC-3'
    ),
    (
        7,
        2,
        'CRO',
        'CRO-3'
    ),
    (
        8,
        1,
        'CRO',
        ''
    ),
    (
        9,
        5,
        'ID',
        'SOMEOTHERID'
    ),
    (
        10,
        5,
        'PNC',
        NULL
    ),
    (
        11,
        5,
        'PNC',
        'PNC7'
    ),
    (
        12,
        5,
        'PNC',
        'PNC9'
    ),
    (
        13,
        5,
        'PNC',
        'PNC9'
    ),
    (
        14,
        5,
        'CRO',
        'CRO9'
    );



INSERT INTO
    sentence_info (
        id,
        fk_person_id,
        sentence_date
    )
VALUES
    (
        1,
        1,
        DATE '2020-04-12'
    ),
    (
        2,
        1,
        DATE '2020-04-18'
    ),
    (
        3,
        2,
        DATE '2020-04-18'
    ),
    (
        4,
        3,
        DATE '2020-04-18'
    ),
    (
        5,
        3,
        NULL
    ),
    (
        6,
        3,
        DATE '1970-01-01'
    ),
    (
        7,
        4,
        DATE '1970-01-01'
    ),
    (
        8,
        4,
        DATE '1975-03-20'
    ),
    (
        9,
        4,
        DATE '1990-01-01'
    ),
    (
        10,
        4,
        DATE '1990-01-02'
    ),
    (
        11,
        5,
        DATE '2015-06-04'
    ),
    (
        12,
        5,
        DATE '2015-04-06'
    ),
    (
        13,
        5,
        NULL
    ),
    (
        14,
        5,
        DATE '2017-09-17'
    )
