import logging
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException

from include.pro_sync_framework.config import DagConfig
from include.pro_sync_framework.db_utils import DbHook
from include.pro_sync_framework.helpers import validate_and_convert_row, generate_composite_pk
from hooks.pro_hook import ProHook

log = logging.getLogger(__name__)

try:
    config = DagConfig('compined_kpi_national')
except Exception as e:
    log.error(f"Failed to load DAG configuration: {e}")
    raise

@dag(
    dag_id=config.dag_id_sync,
    start_date=datetime.fromisoformat(config.start_date),
    schedule=config.schedule,
    catchup=False,
    max_active_runs=1,
    tags=config.tags,
    doc_md="""
    ### Sync Data to Urbi Pro (Refactored)
    Synchronizes data from a source DB view to a Urbi Pro Dynamic Asset.
    1. Triggers an update on the asset schema/filters.
    2. Compares current DB IDs with the last known set to find and delete removed records.
    3. Streams and upserts all current records using efficient keyset pagination.
    4. Saves the current set of IDs for the next run.
    """
)
def sync_data_dag():
    
    trigger_schema_update = TriggerDagRunOperator(
        task_id="update_asset_schema",
        trigger_dag_id=config.dag_id_manage,
        conf={"operation": "UPDATE"},
        wait_for_completion=True,
        deferrable=True,
        failed_states=["failed"],
    )

    @task
    def process_deletes() -> List[str]:
        """Finds and deletes records that no longer exist in the source view."""
        asset_id = Variable.get(config.var_asset_id)
        push_token = Variable.get(config.var_push_token)
        last_known_ids = set(Variable.get(config.var_known_ids, default_var=[], deserialize_json=True))

        db_hook = DbHook(db_conn_id=config.db_conn_id)
        current_pk_rows = db_hook.get_all_pks(config.db_view_name, config.primary_key_columns)
        current_ids = {generate_composite_pk(dict(zip(config.primary_key_columns, row)), config.primary_key_columns) for row in current_pk_rows}

        ids_to_delete = list(last_known_ids - current_ids)
        
        if ids_to_delete:
            log.info(f"Found {len(ids_to_delete)} records to delete.")
            api_hook = ProHook(http_conn_id=config.api_conn_id)
            for i in range(0, len(ids_to_delete), config.api_push_size):
                batch = ids_to_delete[i : i + config.api_push_size]
                api_hook.push_data(
                    asset_id=asset_id,
                    access_token=push_token,
                    data=batch,
                    retry_count=config.api_retry_count,
                    retry_delay=config.api_retry_delay,
                    is_delete=True
                )
        else:
            log.info("No records to delete.")
            
        return list(current_ids)

    @task
    def sync_upserts():
        """Streams records from the DB using keyset pagination and upserts them to the API."""
        asset_id = Variable.get(config.var_asset_id)
        push_token = Variable.get(config.var_push_token)
        db_hook = DbHook(config.db_conn_id)
        api_hook = ProHook(config.api_conn_id)
        
        last_pk_values = None
        total_pushed = 0

        while True:
            sql, params = db_hook.get_paginated_query_and_params(
                table_name=config.db_view_name,
                pk_columns=config.primary_key_columns,
                limit=config.db_fetch_size,
                last_pk_values=last_pk_values
            )
            
            records = db_hook.hook.get_records(sql, parameters=params)

            if not records:
                log.info("No more records to process from the database.")
                break

            cols = [d[0].lower() for d in db_hook.hook.get_description()]
            features = []
            for row in records:
                row_dict = dict(zip(cols, row))
                feature = validate_and_convert_row(row_dict, config)
                if feature:
                    features.append(feature)
            
            if features:
                for i in range(0, len(features), config.api_push_size):
                    batch = features[i : i + config.api_push_size]
                    api_hook.push_data(
                        asset_id=asset_id,
                        access_token=push_token,
                        data=batch,
                        retry_count=config.api_retry_count,
                        retry_delay=config.api_retry_delay,
                        is_delete=False
                    )
                    total_pushed += len(batch)

            # Update keyset for next iteration
            last_row = dict(zip(cols, records[-1]))
            last_pk_values = {pk: last_row[pk] for pk in config.primary_key_columns}
        
        log.info(f"Upsert process complete. Total records pushed: {total_pushed}")

    @task
    def update_known_ids(current_ids: list):
        """Saves the current list of IDs to a variable for the next run."""
        Variable.set(config.var_known_ids, current_ids, serialize_json=True)
        log.info(f"Updated known IDs variable with {len(current_ids)} records.")

    # Define DAG flow
    current_ids_list = process_deletes()
    upsert_task = sync_upserts()
    update_ids_task = update_known_ids(current_ids_list)

    trigger_schema_update >> current_ids_list >> upsert_task >> update_ids_task

sync_data_dag()