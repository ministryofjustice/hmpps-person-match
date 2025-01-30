# ruff: noqa: E501  (disable line length for this module)


def join_raw_tables_sql(
    person_in="person",
    pseudonym_in="pseudonym",
    address_in="address",
    reference_in="reference",
    sentence_info_in="sentence_info",
    limit: int | None = None,
):
    limit_expr = f"LIMIT {limit}" if limit else ""
    sql = f"""
    WITH
    person_out AS (
        SELECT
        *
        FROM {person_in}
    ),
    pseudonym_agg AS (
        SELECT
            fk_person_id,
            array_agg(first_name) as first_name_aliases,
            array_agg(last_name) as last_name_aliases,
            array_agg(date_of_birth) as date_of_birth_aliases
        FROM {pseudonym_in}
        group by fk_person_id
    ),

    address_agg AS (
        SELECT
            fk_person_id,
            array_agg(postcode) as postcodes
        FROM {address_in}
        group by fk_person_id
    ),
    reference_agg AS (
        SELECT
            fk_person_id,
            array_agg(identifier_value) FILTER (WHERE identifier_type = 'CRO') as cros,
            array_agg(identifier_value) FILTER (WHERE identifier_type = 'PNC') as pncs
        FROM {reference_in}
        group by fk_person_id
    ),
    sentence_info_agg    AS (
        SELECT
            fk_person_id,
            array_agg(sentence_date) as sentence_dates
        FROM {sentence_info_in}
        group by fk_person_id
    )
    SELECT
    p.id,
    p.source_system,
    p.first_name,
    p.middle_names,
    p.last_name,
    p.crn,
    p.prison_number,
    p.date_of_birth,
    p.sex,
    p.ethnicity,
    a.first_name_aliases,
    a.last_name_aliases,
    a.date_of_birth_aliases,
    addr.postcodes,
    r.cros,
    r.pncs,
    s.sentence_dates
    FROM
        person_out p
    LEFT JOIN
        pseudonym_agg a ON p.id = a.fk_person_id
    LEFT JOIN
        address_agg addr ON p.id = addr.fk_person_id
    LEFT JOIN
        reference_agg r ON p.id = r.fk_person_id
    LEFT JOIN
        sentence_info_agg s ON p.id = s.fk_person_id
    ORDER BY p.id
    {limit_expr}
    """
    return sql
