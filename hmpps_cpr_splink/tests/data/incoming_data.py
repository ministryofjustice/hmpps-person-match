import duckdb


def get_data_from_candidate_search(con: duckdb.DuckDBPyConnection):
    con.execute(
        """
        CREATE TABLE  candidate_search_return_format (
            id INT32,
            source_system VARCHAR,
            first_name VARCHAR,
            middle_names VARCHAR,
            last_name VARCHAR,
            crn VARCHAR,
            prison_number VARCHAR,
            date_of_birth DATE,
            sex VARCHAR,
            ethnicity VARCHAR,
            first_name_alias_arr VARCHAR[],
            last_name_aliases VARCHAR[],
            date_of_birth_alias_arr DATE[],
            postcode_arr VARCHAR[],
            cro_arr VARCHAR[],
            pnc_arr VARCHAR[],
            sentence_date_arr DATE[]
        )
        """
    )

    con.execute(
        """
        INSERT INTO candidate_search_return_format VALUES (
            1200000,
            'NOMIS',
            'JOHN',
            'JAMES',
            'SMITH',
            NULL,
            'A1111AA',
            '1955-05-05',
            NULL,
            'W',
            ['JONNY', 'JOHN'],
            ['SMYTH', 'SMYTH'],
            ['1955-03-28', '1955-03-28'],
            ['AB1 2CD', 'XY1 9YZ'],
            ['123456/99X'],
            NULL,
            ['2000-12-17', '2001-12-17']
        )
        """
    )


def populate_raw_tables(con: duckdb.DuckDBPyConnection):
    con.execute(
        """
    CREATE TABLE person (
        id INT32,
        source_system VARCHAR,
        first_name VARCHAR,
        middle_names VARCHAR,
        last_name VARCHAR,
        crn VARCHAR,
        prison_number VARCHAR,
        date_of_birth DATE,
        sex VARCHAR,
        ethnicity VARCHAR
    )
    """
    )

    con.execute(
        """
        CREATE TABLE pseudonym (
            fk_person_id INT32,
            first_name VARCHAR,
            last_name VARCHAR,
            date_of_birth DATE
        )
        """
    )

    con.execute(
        """
        CREATE TABLE address (
            fk_person_id INT32,
            postcode VARCHAR
        )
        """
    )

    con.execute(
        """
        CREATE TABLE reference (
            fk_person_id INT32,
            identifier_type VARCHAR,
            identifier_value VARCHAR
        )
        """
    )

    con.execute(
        """
        CREATE TABLE sentence_info (
            fk_person_id INT32,
            sentence_date DATE
        )
        """
    )

    # Insert sample data into the tables
    con.execute(
        """
        INSERT INTO person VALUES (
            1200000,
            'NOMIS',
            'JOHN',
            'JAMES',
            'SMITH',
            NULL,
            'A1111AA',
            '1955-05-05',
            NULL,
            'W'
        )
        """
    )

    con.execute(
        """
        INSERT INTO pseudonym VALUES
            (1200000, 'JONNY', 'SMYTH', '1955-03-28'),
            (1200000, 'JOHN', 'SMYTH', '1955-03-28')
        """
    )

    con.execute(
        """
        INSERT INTO address VALUES
            (1200000, 'AB1 2CD'),
            (1200000, 'XY1 9YZ')
        """
    )

    con.execute(
        """
        INSERT INTO reference VALUES
            (1200000, 'CRO', '123456/99X')
        """
    )

    con.execute(
        """
        INSERT INTO sentence_info VALUES
            (1200000, '2000-12-17'),
            (1200000, '2001-12-17')
        """
    )
