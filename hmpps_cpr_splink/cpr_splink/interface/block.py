# this isn't really app-facing, but also feels like it lives with this stuff

from splink.internals.blocking import (
    BlockingRule,
    _sql_gen_where_condition,
    combine_unique_id_input_columns,
)
from splink.internals.input_column import InputColumn
from splink.internals.pipeline import CTEPipeline
from splink.internals.settings import LinkTypeLiteralType
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql

from ..model.blocking_rules import (
    blocking_rules_for_prediction_tight_for_candidate_search,
)
from .db import postgres_db_connector

# TODO: this doesn't work directly, as our indexing doesnt work, but enough for now
# Splink 4.0.7 should have requisite change
_blocking_rules_concrete = list(
    map(
        lambda brc: brc.get_blocking_rule("postgres"),
        blocking_rules_for_prediction_tight_for_candidate_search,
    )
)
for n, br in enumerate(_blocking_rules_concrete):
    br.add_preceding_rules(_blocking_rules_concrete[:n])

_id_col = InputColumn("id", sqlglot_dialect_str="postgres")
_sd_col = InputColumn("source_dataset", sqlglot_dialect_str="postgres")


# modified version of br.create_blocked_pairs_sql
def _create_blocked_pairs_sql(
    blocking_rule: BlockingRule,
    *,
    source_dataset_input_column: InputColumn | None,
    unique_id_input_column: InputColumn,
    input_tablename_l: str,
    input_tablename_r: str,
    where_condition: str,
) -> str:
    # TODO: I can probably ditch this kind of generality
    uid_l_expr = _composite_unique_id_from_nodes_sql([unique_id_input_column], "l")

    sql = f"""
        select
        '{blocking_rule.match_key}' as match_key,
        {uid_l_expr} AS primary_id,
        r.*
        from {input_tablename_l} as l
        inner join {input_tablename_r} as r
        on
        ({blocking_rule.blocking_rule_sql})
        {where_condition}
        {
        blocking_rule.exclude_pairs_generated_by_all_preceding_rules_sql(
            "dummy",
            "dummy",
        )
    }
        """
    return sql


def _block_using_rules_sqls(
    *,
    input_tablename_l: str,
    input_tablename_r: str,
    blocking_rules: list[BlockingRule],
    link_type: "LinkTypeLiteralType",
    source_dataset_input_column: InputColumn | None,
    unique_id_input_column: InputColumn,
) -> dict[str, str]:
    unique_id_input_columns = combine_unique_id_input_columns(
        source_dataset_input_column, unique_id_input_column
    )

    where_condition = _sql_gen_where_condition(link_type, unique_id_input_columns)

    br_sqls = []

    for br in blocking_rules:
        sql = _create_blocked_pairs_sql(
            br,
            unique_id_input_column=unique_id_input_column,
            source_dataset_input_column=source_dataset_input_column,
            input_tablename_l=input_tablename_l,
            input_tablename_r=input_tablename_r,
            where_condition=where_condition,
        )
        br_sqls.append(sql)

    sql = " UNION ALL ".join(br_sqls)

    return {"sql": sql, "output_table_name": "__splink__blocked_id_pairs"}


def candidate_search(primary_record_id: str) -> str:
    """
    Given a primary record id, return a table of these records
    along with the primary, ready to be scored.
    """
    # TODO: more careful about inserting id
    pipeline = CTEPipeline()

    # TODO: table name from?
    cleaned_table_name = "person"

    table_name_primary = "primary_record"
    sql = (
        f"SELECT *, 'a_primary' AS source_dataset "
        f"FROM {cleaned_table_name} WHERE id = '{primary_record_id}'"
    )
    pipeline.enqueue_sql(sql=sql, output_table_name=table_name_primary)

    # need source dataset to be later alphabetically to get the right condition
    table_name_potential_candidates = "person_ws"
    sql = f"SELECT *, 'z_candidates' AS source_dataset FROM {cleaned_table_name}"
    pipeline.enqueue_sql(sql=sql, output_table_name=table_name_potential_candidates)

    sql_info = _block_using_rules_sqls(
        input_tablename_l=table_name_primary,
        input_tablename_r=table_name_potential_candidates,
        blocking_rules=_blocking_rules_concrete,
        link_type="link_only",
        source_dataset_input_column=_sd_col,
        unique_id_input_column=_id_col,
    )
    pipeline.enqueue_sql(**sql_info)

    # TODO: materialise?
    # TODO: do I need less generic name if concurrent queries?
    # TODO: cleanup. Can I make this function return a context manager?
    sql = (
        f"CREATE OR REPLACE VIEW {pipeline.output_table_name} AS "
        f"{pipeline.generate_cte_pipeline_sql()}"
    )
    # TODO: in a schema?
    with postgres_db_connector() as conn, conn.cursor() as cur:
        cur.execute(sql)

    return pipeline.output_table_name
