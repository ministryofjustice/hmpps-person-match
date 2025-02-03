import altair as alt
import pandas as pd
from splink.internals.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_data,
)


def blocking_performance_chart(linker, predictions):
    nodes = [v.physical_name for v in linker._input_tables_dict.values()]
    s_ob = linker._settings_obj
    blocking_rules = s_ob._blocking_rules_to_generate_predictions

    # Comparisons per blocking rule
    counts = cumulative_comparisons_to_be_scored_from_blocking_rules_data(
        table_or_tables=nodes,
        blocking_rules=[br.blocking_rule_sql for br in blocking_rules],
        db_api=linker._db_api,
        link_type=s_ob._link_type,
        unique_id_column_name=s_ob.column_info_settings.unique_id_column_name,
        source_dataset_column_name=s_ob.column_info_settings.source_dataset_column_name,
    )

    # Set granularity of the chart
    precision = 0.25
    threshold_values = [round(x * precision, 2) for x in range(int(0.0 / precision), int(10 / precision) + 1)]

    union_queries = []

    for t in threshold_values:
        query_part = f"""
        SELECT
            match_key,
            {t} AS mw_threshold,
            COUNT(*) AS n_edges
        FROM {predictions.physical_name}
        WHERE match_weight >= {t}
        GROUP BY match_key
        """
        union_queries.append(query_part)

    sql = "\nUNION ALL\n".join(union_queries) + "\nORDER BY match_key, mw_threshold"

    # Matches per blocking rule (at a given threshold)
    matches = linker.misc.query_sql(sql)

    # Merge the two dataframes
    chart_df = pd.merge(matches, counts, on="match_key")
    chart_df["match_key"] = chart_df["match_key"].astype(int)
    chart_df = chart_df.rename(columns={"row_count": "total_edges", "n_edges": "matches"})

    cols = ["match_key", "mw_threshold", "total_edges", "matches", "blocking_rule"]
    chart_df = chart_df[cols]
    chart_df["non_matches"] = chart_df["total_edges"] - chart_df["matches"]

    chart_def = {
        "config": {
            "view": {"continuousWidth": 200, "continuousHeight": 300},
            "concat": {"spacing": -5},
        },
        "hconcat": [
            {
                "layer": [
                    {
                        "mark": {"type": "rule", "x": 100, "size": 15},
                        "title": {
                            "text": "Non-matches",
                            "anchor": "start",
                            "fontSize": 20,
                        },
                    },
                    {
                        "mark": {
                            "type": "text",
                            "align": "right",
                            "x": "width",
                            "xOffset": -2,
                        },
                        "encoding": {
                            "text": {
                                "field": "fdr",
                                "format": ".0%",
                                "type": "quantitative",
                            },
                        },
                    },
                    {
                        "mark": {"type": "text", "fontSize": 12},
                        "encoding": {
                            "text": {"field": "p_lt_threshold"},
                            "x": {"value": 1},
                            "y": {"value": -10},
                            "color": {"value": "black"},
                        },
                        "transform": [{"filter": "datum.match_key == 0"}],
                    },
                ],
                "encoding": {
                    "color": {
                        "field": "precision",
                        "type": "quantitative",
                        "scale": {"reverse": True, "scheme": "reds", "domain": [0, 1]},
                        "legend": None,
                    },
                    "tooltip": [
                        {
                            "field": "blocking_rule",
                            "title": "Blocking rule",
                            "type": "nominal",
                        },
                        {
                            "field": "non_matches",
                            "format": ".3s",
                            "title": "Matches",
                            "type": "nominal",
                        },
                        {
                            "field": "non_matches",
                            "format": ".3s",
                            "title": "Non-matches",
                            "type": "nominal",
                        },
                        {
                            "field": "precision",
                            "format": ".2%",
                            "title": "Precision",
                            "type": "nominal",
                        },
                    ],
                    "x": {
                        "field": "non_matches",
                        "scale": {
                            "domainMin": 1,
                            "domainMax": {"expr": "max_non_matches"},
                            "nice": True,
                            "reverse": True,
                            "type": "log",
                        },
                        "type": "quantitative",
                        "axis": {"format": "s", "tickCount": 5},
                        "title": "Comparisons",
                    },
                    "y": {"axis": None, "field": "match_key", "type": "nominal"},
                },
            },
            {
                "mark": {"type": "text", "fontSize": 12},
                "encoding": {
                    "text": {"field": "match_key", "type": "nominal"},
                    "y": {"axis": None, "field": "match_key", "type": "nominal"},
                },
                "title": {"text": ["Match", "key"], "offset": 5, "orient": "top"},
                "view": {"stroke": None},
            },
            {
                "layer": [
                    {
                        "mark": {"type": "rule", "size": 15, "opacity": 1},
                        "title": {"text": "Matches", "anchor": "end", "fontSize": 20},
                    },
                    {
                        "mark": {
                            "type": "text",
                            "align": "left",
                            "x": "width",
                            "xOffset": 2,
                        },
                        "encoding": {
                            "text": {
                                "field": "precision",
                                "format": ".0%",
                                "type": "quantitative",
                            },
                        },
                    },
                    {
                        "mark": {"type": "text", "fontSize": 12},
                        "encoding": {
                            "text": {"field": "p_ge_threshold"},
                            "x": {"value": "width"},
                            "y": {"value": -10},
                            "color": {"value": "black"},
                        },
                        "transform": [{"filter": "datum.match_key == 0"}],
                    },
                ],
                "encoding": {
                    "color": {
                        "field": "precision",
                        "type": "quantitative",
                        "scale": {"scheme": "greens", "domain": [0, 1]},
                        "legend": None,
                    },
                    "tooltip": [
                        {
                            "field": "blocking_rule",
                            "title": "Blocking rule",
                            "type": "nominal",
                        },
                        {
                            "field": "matches",
                            "format": ".3s",
                            "title": "Matches",
                            "type": "nominal",
                        },
                        {
                            "field": "non_matches",
                            "format": ".3s",
                            "title": "Non-matches",
                            "type": "nominal",
                        },
                        {
                            "field": "precision",
                            "format": ".2%",
                            "title": "Precision",
                            "type": "nominal",
                        },
                    ],
                    "x": {
                        "field": "matches",
                        "scale": {
                            "domainMin": 1,
                            "domainMax": {"expr": "max_matches"},
                            "nice": True,
                            "type": "log",
                        },
                        "type": "quantitative",
                        "axis": {"format": "s", "tickCount": 5},
                        "title": "Comparisons",
                    },
                    "y": {"axis": None, "field": "match_key", "type": "nominal"},
                },
            },
        ],
        "params": [
            {
                "name": "t_mw",
                "bind": {
                    "input": "range",
                    "max": 20,
                    "min": 0,
                    "name": "Match weight threshold",
                    "step": 0.25,
                },
                "value": 5,
            },
            {"name": "t_p", "expr": "format(pow(2,t_mw)/(1+pow(2,t_mw)), '.3')"},
            {"name": "max_matches", "expr": "data('data_0')[0]['max_matches']"},
            {"name": "max_non_matches", "expr": "data('data_0')[0]['max_non_matches']"},
        ],
        "resolve": {"scale": {"color": "independent", "x": "independent", "y": "shared"}},
        "title": {
            "text": "Blocking rule performance",
            "anchor": "middle",
            "fontSize": 30,
            "offset": 20,
            "subtitle": "Number of matches and non-matches returned by each blocking rule",  # NOQA: E501
        },
        "transform": [
            {
                "joinaggregate": [
                    {"op": "max", "field": "non_matches", "as": "max_non_matches"},
                    {"op": "max", "field": "matches", "as": "max_matches"},
                ],
            },
            {
                "calculate": "'p < '+format(pow(2, datum.mw_threshold)/(1+pow(2, datum.mw_threshold)), '.3f')",  # NOQA: E501
                "as": "p_lt_threshold",
            },
            {
                "calculate": "'p >= '+format(pow(2, datum.mw_threshold)/(1+pow(2, datum.mw_threshold)), '.3f')",  # NOQA: E501
                "as": "p_ge_threshold",
            },
            {"filter": "(datum.mw_threshold == t_mw)"},
            {"calculate": "datum.matches/datum.total_edges", "as": "precision"},
            {"calculate": "datum.non_matches/datum.total_edges", "as": "fdr"},
        ],
        "$schema": "https://vega.github.io/schema/vega-lite/v5.4.1.json",
        "data": {"values": []},
    }
    chart_def["data"]["values"] = chart_df.to_dict(orient="records")

    chart = alt.Chart.from_dict(chart_def)

    return chart
