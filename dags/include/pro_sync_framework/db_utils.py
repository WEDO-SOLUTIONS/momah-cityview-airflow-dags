import logging
from typing import List, Dict, Any, Tuple, Optional

from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

class DbHook:
    """
    A database abstraction layer to support Oracle and Postgres with dialect-specific SQL.
    """
    def __init__(self, db_conn_id: str):
        self.db_conn_id = db_conn_id
        conn = BaseHook.get_connection(db_conn_id)
        self.conn_type = conn.conn_type
        self.hook = self._get_db_hook()

    def _get_db_hook(self) -> BaseHook:
        if self.conn_type == 'oracle':
            return OracleHook(oracle_conn_id=self.db_conn_id)
        elif self.conn_type == 'postgres':
            return PostgresHook(postgres_conn_id=self.db_conn_id)
        else:
            raise NotImplementedError(f"Database type '{self.conn_type}' is not supported.")

    def get_column_names(self, table_name: str) -> List[str]:
        """Inspects a table/view and returns a list of lowercase column names."""
        sql = f"SELECT * FROM {table_name} WHERE 1=0"
        conn = self.hook.get_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            return [desc[0].lower() for desc in cur.description]

    def get_all_pks(self, table_name: str, pk_columns: List[str]) -> List[Tuple]:
        """Fetches all primary key values from the source table."""
        pk_cols_str = ", ".join(pk_columns)
        sql = f"SELECT {pk_cols_str} FROM {table_name}"
        return self.hook.get_records(sql)

# CORRECT ✅
def get_paginated_query_and_params(
    self,
    table_name: str,
    pk_columns: List[str],
    limit: int,
    last_pk_values: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Generates SQL and parameters for efficient keyset pagination.
    Handles both single and composite primary keys with case-insensitive columns.
    """
    # FIX: Removed quotes around column names
    order_by_clause = ", ".join(pk_columns)
    params = {"limit": limit}
    where_clause = ""

    if last_pk_values:
        clauses = []
        for i in range(len(pk_columns)):
            col = pk_columns[i]
            param_name = f"last_pk_{i}"

            # FIX: Removed quotes around column name
            current_level_clause = f'{col} > :{param_name}'
            params[param_name] = last_pk_values[col]

            if i > 0:
                prev_cols = pk_columns[:i]
                # FIX: Removed quotes around column names
                prev_level_clauses = " AND ".join(
                    [f'{p_col} = :last_pk_{p_idx}' for p_idx, p_col in enumerate(prev_cols)]
                )
                clauses.append(f"({prev_level_clauses} AND {current_level_clause})")
            else:
                clauses.append(f"({current_level_clause})")

        where_clause = f"WHERE {' OR '.join(clauses)}"

    base_sql = f"SELECT * FROM {table_name} {where_clause} ORDER BY {order_by_clause}"

    if self.conn_type == 'oracle':
        final_sql = f"{base_sql} FETCH NEXT :limit ROWS ONLY"
    elif self.conn_type == 'postgres':
        final_sql = f"{base_sql} LIMIT :limit"
    else:
        final_sql = base_sql

    return final_sql, params