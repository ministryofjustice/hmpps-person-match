from __future__ import annotations


class Table:
    def __init__(
        self,
        name: str,
        *select_expressions,
        from_table: str | Table,
        post_from_clauses: str = "",
    ):
        """
        Initialises a Table object to represent a SQL query.

        Args:
            name (str): The name of the resulting table or CTE.
            *select_expressions (str | TransformedColumn): A variable number of
                expressions to include in the SELECT clause. Can be:
                - String literals representing raw SQL expressions (e.g., "*",
                  "column_name")
                - TransformedColumn objects containing SQL transformations
            from_table (str | Table): The source table or query for this table. Can be a
                string representing a table name or another `Table` object.
            post_from_clauses (str, optional): Additional SQL clauses to append after
                the FROM clause (e.g., WHERE, JOIN).
        """
        self.select_expressions = select_expressions
        self.from_condition = from_table
        self.name = name
        self.post_from_clauses = post_from_clauses

    @property
    def select_list_with_alias(self) -> str:
        col_exprs = [expr if isinstance(expr, str) else expr.select_expression for expr in self.select_expressions]
        return ",\n\n".join(col_exprs)

    @property
    def select_statement_without_lineage(self) -> str:
        return f"SELECT {self.select_list_with_alias}\nFROM {self.from_condition}\n{self.post_from_clauses}"

    @property
    def cte_select_statement(self) -> str:
        return f"{self.name} AS (\n{self.select_statement_without_lineage}\n)"

    @property
    def with_lineage(self) -> list[Table]:
        """
        All the Tables that are ascendant to this
        """
        if isinstance(self.from_condition, str):
            return []
        return self.from_condition.with_lineage + [self.from_condition]

    @property
    def select_statement_with_lineage(self) -> str:
        lin = self.with_lineage
        if not lin:
            return self.select_statement_without_lineage
        sql = (
            "WITH " + ",\n".join(tab.cte_select_statement for tab in lin) + "\n" + self.select_statement_without_lineage
        )
        return sql

    @property
    def create_table_sql(self) -> str:
        return f"CREATE TABLE {self.name} AS (\n{self.select_statement_with_lineage}\n)"

    def __str__(self) -> str:
        return self.name
