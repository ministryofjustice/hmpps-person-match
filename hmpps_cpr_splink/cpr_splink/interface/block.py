# this isn't really app-facing, but also feels like it lives with this stuff
from collections.abc import Sequence

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
        '{blocking_rule.match_key}' as match_key,
        l.id AS primary_id,
        l.match_id AS primary_match_id,
        r.*
        from {input_tablename_l} as l
        inner join {input_tablename_r} as r
        on
        ({blocking_rule.blocking_rule_sql})
        {
        blocking_rule.exclude_pairs_generated_by_all_preceding_rules_sql(
            "dummy",
            "dummy",
        )
    }
        """  # noqa: S608
    return sql


def _block_using_rules_sqls(
    *,
    input_tablename_l: str,
    input_tablename_r: str,
    blocking_rules: list[BlockingRule],
    link_type: "LinkTypeLiteralType",
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

    sql = " UNION ALL ".join(br_sqls)

    return {"sql": sql, "output_table_name": "personmatch.__splink__blocked_id_pairs"}


async def candidate_search(primary_record_id: str, connection_pg: AsyncSession) -> Sequence[RowMapping]:
    """
    Given a primary record id, return a table of these records
    along with the primary, ready to be scored.

    Requires a duckdb connexion with a postgres database attached as 'pg_db'
    """
    pipeline = CTEPipeline()

    # TODO: table name from?
    cleaned_table_name = "personmatch.person"

    table_name_primary = "primary_record"
    sql = f"SELECT *, 'a_primary' AS source_dataset FROM {cleaned_table_name} WHERE match_id = :mid"  # noqa: S608
    pipeline.enqueue_sql(sql=sql, output_table_name=table_name_primary)

    # need source dataset to be later alphabetically to get the right condition
    table_name_potential_candidates = "person_ws"
    sql = f"SELECT *, 'z_candidates' AS source_dataset FROM {cleaned_table_name}"  # noqa: S608
    pipeline.enqueue_sql(sql=sql, output_table_name=table_name_potential_candidates)

    sql_info = _block_using_rules_sqls(
        input_tablename_l=table_name_primary,
        input_tablename_r=table_name_potential_candidates,
        blocking_rules=_blocking_rules_concrete,
        link_type="link_only",
    )
    pipeline.enqueue_sql(**sql_info)

    sql = pipeline.generate_cte_pipeline_sql()
    res = await connection_pg.execute(text(sql), {"mid": primary_record_id})

    return res.mappings().fetchall()
