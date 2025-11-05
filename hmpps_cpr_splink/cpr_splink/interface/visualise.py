from datetime import date, datetime
from typing import Any

from duckdb import DuckDBPyRelation
from splink import DuckDBAPI
from splink.internals.pipeline import CTEPipeline
from splink.internals.realtime import compare_records
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import enqueue_join_term_frequency_tables
from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres
from hmpps_cpr_splink.cpr_splink.model.model import (
    MODEL_PATH,
)

from .score import insert_data_into_duckdb

JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


def _serialise_value(value: object) -> JSONValue:
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, (list, tuple)):
        return [_serialise_value(item) for item in value]

    if isinstance(value, dict):
        return {k: _serialise_value(v) for k, v in value.items()}

    return str(value)


def _serialise_row(mapping: dict[str, Any]) -> dict[str, JSONValue]:
    return {key: _serialise_value(value) for key, value in mapping.items()}


def _ddb_relation_to_serialised_dicts(relation: DuckDBPyRelation) -> list[dict[str, Any]]:
    res_list_dicts = [dict(zip(relation.columns, row, strict=True)) for row in relation.fetchall()]
    return [_serialise_row(row) for row in res_list_dicts]


async def get_nodes_edges(
    match_ids: list[str],
    pg_db_url: URL,
    connection_pg: AsyncSession,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with duckdb_connected_to_postgres(pg_db_url) as connection_duckdb:
        tablename = "records_to_check"

        pipeline = CTEPipeline()
        pipeline.enqueue_sql(
            sql="SELECT * FROM personmatch.person WHERE match_id = ANY(:match_ids)",
            output_table_name="people_to_check_cluster_status",
        )
        enqueue_join_term_frequency_tables(
            pipeline,
            table_to_join_to=pipeline.output_table_name,
            output_table_name=tablename,
        )

        sql = pipeline.generate_cte_pipeline_sql()
        res = await connection_pg.execute(text(sql), {"match_ids": match_ids})

        nodes_table_name = insert_data_into_duckdb(connection_duckdb, res.mappings().fetchall(), tablename)
        nodes_as_dict = _ddb_relation_to_serialised_dicts(connection_duckdb.table(nodes_table_name))

        db_api = DuckDBAPI(connection_duckdb)
        scores = compare_records(
            nodes_table_name,
            nodes_table_name,
            settings=MODEL_PATH,
            db_api=db_api,
            sql_cache_key="get_clusters_compare_sql",
            join_condition="l.id < r.id",
        )
        scores_ddb = scores.as_duckdbpyrelation()

        edges_as_dict = _ddb_relation_to_serialised_dicts(scores_ddb)

        return nodes_as_dict, edges_as_dict
