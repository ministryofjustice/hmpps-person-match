# this isn't really app-facing, but also feels like it lives with this stuff
from collections.abc import Mapping, Sequence
from typing import Any

from splink.internals.blocking import (
    BlockingRule,
    _sql_gen_where_condition,
    combine_unique_id_input_columns,
)
from splink.internals.input_column import InputColumn
from splink.internals.pipeline import CTEPipeline
from splink.internals.settings import LinkTypeLiteralType
from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.model.blocking_rules import (
    blocking_rules_for_prediction_tight_for_candidate_search,
)
from hmpps_cpr_splink.cpr_splink.model_cleaning import CLEANED_TABLE_SCHEMA

# TODO: this doesn't work directly, as our indexing doesnt work, but enough for now
# Splink 4.0.7 should have requisite change
_blocking_rules_concrete = list(
    map(
        lambda brc: brc.get_blocking_rule("postgres"),
        blocking_rules_for_prediction_tight_for_candidate_search,
    ),
)
for n, br in enumerate(_blocking_rules_concrete):
    br.add_preceding_rules(_blocking_rules_concrete[:n])

unique_id_input_column = InputColumn("id", sqlglot_dialect_str="postgres")
source_dataset_input_column = InputColumn("source_dataset", sqlglot_dialect_str="postgres")


def _postgres_column_type(column_type: str) -> str:
    return {
        "VARCHAR": "TEXT",
        "VARCHAR[]": "TEXT[]",
    }.get(column_type, column_type)


# modified version of br.create_blocked_pairs_sql
def _create_blocked_pairs_sql(
    blocking_rule: BlockingRule,
    *,
    input_tablename_l: str,
    input_tablename_r: str,
    where_condition: str,
) -> str:
    # TODO: I can probably ditch this kind of generality

    sql = f"""
        select
        l.id AS primary_id,
        l.match_id AS primary_match_id,
        r.*
        from {input_tablename_l} as l
        inner join {input_tablename_r} as r
        on
        ({blocking_rule.blocking_rule_sql})
        """  # noqa: S608
    return sql


def _block_using_rules_sqls(
    *,
    input_tablename_l: str,
    input_tablename_r: str,
    blocking_rules: list[BlockingRule],
    link_type: LinkTypeLiteralType,
) -> dict[str, str]:
    unique_id_input_columns = combine_unique_id_input_columns(source_dataset_input_column, unique_id_input_column)

    where_condition = _sql_gen_where_condition(link_type, unique_id_input_columns)

    br_sqls = []

    for br in blocking_rules:
        sql = _create_blocked_pairs_sql(
            br,
            input_tablename_l=input_tablename_l,
            input_tablename_r=input_tablename_r,
            where_condition=where_condition,
        )
        br_sqls.append(sql)

    sql = " UNION ".join(br_sqls)

    return {"sql": sql, "output_table_name": "__splink__blocked_id_pairs"}


def enqueue_join_term_frequency_tables(
    pipeline: CTEPipeline,
    table_to_join_to: str,
    output_table_name: str,
    default_postcode_tf: float = 1.0,
) -> None:
    """
    Given a CTEPipeline, enqueue SQL to join term frequency tables.

    Resulting table will have the original table's columns, plus a tf_{column}
    for each column we have simple term frequencies for.

    For postcodes we will have two array columns, in matching order:
    * postcode_arr_repacked containing the postcode values
    * postcode_freq_arr containing the corresponding term frequencies

    `default_postcode_tf` sets the term frequency to use for postcodes not found
    in the term frequency table.
    """

    pipeline.enqueue_sql(
        sql=f"""
        SELECT
            match_id,
            UNNEST(postcode_arr) AS postcode
        FROM
            {table_to_join_to}
        """,  # noqa: S608
        output_table_name="exploded_postcodes",
    )
    pipeline.enqueue_sql(
        sql=f"""
        SELECT
            exploded_postcodes.match_id AS match_id,
            exploded_postcodes.postcode AS value,
            COALESCE(pc_tf.tf_postcode, {default_postcode_tf}) AS rel_freq
        FROM
            exploded_postcodes
        LEFT JOIN personmatch.term_frequencies_postcode AS pc_tf
        ON exploded_postcodes.postcode = pc_tf.postcode
        """,  # noqa: S608
        output_table_name="exploded_postcodes_with_term_frequencies",
    )
    pipeline.enqueue_sql(
        sql="""
        SELECT
            match_id,
            array_agg(
                value ORDER BY value
            ) AS postcode_arr_repacked,
            array_agg(
                rel_freq ORDER BY value
            ) AS postcode_freq_arr
        FROM
            exploded_postcodes_with_term_frequencies
        GROUP BY
            match_id
        """,
        output_table_name="postcodes_repacked_with_term_frequencies",
    )

    # join tf tables
    tf_columns = [
        "name_1_std",
        "name_2_std",
        "last_name_std",
        "first_and_last_name_std",
        "date_of_birth",
        "cro_single",
        "pnc_single",
    ]
    join_clauses = []
    select_clauses = ["f.*"]
    for col in tf_columns:
        tf_colname = f"tf_{col}"
        tf_table_name = f"term_frequencies_{col}"
        alias_table_name = tf_colname
        tf_lookup_table_name = f"personmatch.{tf_table_name}"
        join_clauses.append(
            f"LEFT JOIN {tf_lookup_table_name} AS {alias_table_name} ON f.{col} = {alias_table_name}.{col}",
        )
        select_clauses.append(f"{alias_table_name}.{tf_colname} AS {tf_colname}")

    # postcodes
    join_clauses.append(
        f"LEFT JOIN {pipeline.output_table_name} AS pc_name ON f.match_id = pc_name.match_id",
    )
    select_clauses.append("pc_name.postcode_arr_repacked")
    select_clauses.append("pc_name.postcode_freq_arr")

    sql_join = " ".join(join_clauses)
    sql_select = ", ".join(select_clauses)
    sql = f"""
        SELECT {sql_select}
        FROM {table_to_join_to} AS f
        {sql_join}
    """  # noqa: S608

    pipeline.enqueue_sql(sql=sql, output_table_name=output_table_name)


async def candidate_search(primary_record_id: str, connection_pg: AsyncSession) -> Sequence[RowMapping]:
    """
    Given a primary record id, return a table of these records
    along with the primary, ready to be scored.

    Requires a duckdb connexion with a postgres database attached as 'pg_db'
    """
    pipeline = CTEPipeline()

    cleaned_table_name = "personmatch.person"

    table_name_primary = "primary_record"
    sql = f"SELECT * FROM {cleaned_table_name} WHERE match_id = :mid"  # noqa: S608
    pipeline.enqueue_sql(sql=sql, output_table_name=table_name_primary)

    sql_info = _block_using_rules_sqls(
        input_tablename_l=table_name_primary,
        input_tablename_r=cleaned_table_name,
        blocking_rules=_blocking_rules_concrete,
        link_type="link_only",
    )
    pipeline.enqueue_sql(**sql_info)

    enqueue_join_term_frequency_tables(
        pipeline,
        table_to_join_to=pipeline.output_table_name,
        output_table_name="blocked_pairs_with_tfs",
    )

    sql = pipeline.generate_cte_pipeline_sql()
    res = await connection_pg.execute(text(sql), {"mid": primary_record_id})

    return res.mappings().fetchall()


async def candidate_search_for_record(
    primary_record: Mapping[str, Any],
    connection_pg: AsyncSession,
) -> Sequence[RowMapping]:
    pipeline = CTEPipeline()
    cleaned_table_name = "personmatch.person"
    table_name_primary = "primary_record"

    primary_record_with_id = {"id": 0, **primary_record}
    primary_select = ",\n".join(
        f"CAST(:search_{column_name} AS {_postgres_column_type(column_type)}) AS {column_name}"
        for column_name, column_type in CLEANED_TABLE_SCHEMA
    )
    pipeline.enqueue_sql(
        sql=f"SELECT {primary_select}",
        output_table_name=table_name_primary,
    )

    sql_info = _block_using_rules_sqls(
        input_tablename_l=table_name_primary,
        input_tablename_r=cleaned_table_name,
        blocking_rules=_blocking_rules_concrete,
        link_type="link_only",
    )
    pipeline.enqueue_sql(**sql_info)

    cleaned_columns = ", ".join(column_name for column_name, _ in CLEANED_TABLE_SCHEMA)
    pipeline.enqueue_sql(
        sql=f"""
        SELECT {cleaned_columns} FROM {table_name_primary}
        UNION ALL
        SELECT {cleaned_columns} FROM {pipeline.output_table_name}
        """,  # noqa: S608
        output_table_name="primary_with_blocked_candidates",
    )
    enqueue_join_term_frequency_tables(
        pipeline,
        table_to_join_to=pipeline.output_table_name,
        output_table_name="blocked_pairs_with_tfs",
    )

    sql = pipeline.generate_cte_pipeline_sql()
    parameters = {
        f"search_{column_name}": primary_record_with_id[column_name]
        for column_name, _ in CLEANED_TABLE_SCHEMA
    }
    result = await connection_pg.execute(text(sql), parameters)
    return result.mappings().fetchall()
