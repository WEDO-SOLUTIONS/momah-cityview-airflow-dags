import logging
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.exceptions import AirflowException

# Import the framework components
from include.pro_sync_framework.config import DagConfig
from include.pro_sync_framework.db_utils import DbHook
from include.pro_sync_framework.helpers import build_schema_from_db, build_payload
from plugins.hooks.pro_hook import ProHook

log = logging.getLogger(__name__)

# Load configuration for the 'external_entities' DAG
try:
    config = DagConfig('external_entities')
except Exception as e:
    log.error(f"Failed to load DAG configuration: {e}")
    # DAG will not be parsed if config fails
    raise

@dag(
    dag_id=config.dag_id_manage,
    start_date=datetime.fromisoformat(config.start_date),
    schedule=None,
    catchup=False,
    tags=config.tags,
    doc_md="""
    ### Manage Urbi Pro Dynamic Asset (Refactored)
    - **CREATE**: Creates the dynamic asset based on the DB schema.
    - **UPDATE**: Rebuilds schema & filters and updates the existing asset.
    - **CLEAR_ALL_DATA**: Deletes all data from the asset.
    """,
    params={
        "operation": {"type": "string", "enum": ["CREATE", "UPDATE", "CLEAR_ALL_DATA"]},
    },
)
def manage_asset_dag():
    @task
    def execute_operation(**context):
        op = context["params"]["operation"]
        api_hook = ProHook(http_conn_id=config.api_conn_id)
        
        asset_id = Variable.get(config.var_asset_id, default_var=None)
        if op in ["UPDATE", "CLEAR_ALL_DATA"] and not asset_id:
            raise AirflowException(f"Asset ID not found in Variable '{config.var_asset_id}' for {op} operation.")

        if op in ["CREATE", "UPDATE"]:
            db_hook = DbHook(db_conn_id=config.db_conn_id)
            attributes, filters = build_schema_from_db(config, db_hook)
            payload = build_payload(config, attributes, filters)
            
            response = api_hook.create_or_update_asset(
                payload=payload,
                asset_id=asset_id if op == "UPDATE" else None,
                retry_count=config.api_retry_count,
                retry_delay=config.api_retry_delay
            )

            if op == "CREATE":
                new_id = response.get("asset_id")
                new_token = response.get("access_token")
                if not new_id or not new_token:
                    raise AirflowException("API response missing 'asset_id' or 'access_token'.")
                
                Variable.set(config.var_asset_id, new_id)
                Variable.set(config.var_push_token, new_token)
                Variable.set(config.var_known_ids, [], serialize_json=True)
                log.info(f"Asset created successfully. ID: {new_id}")

        elif op == "CLEAR_ALL_DATA":
            api_hook.clear_all_asset_data(asset_id, config.api_retry_count, config.api_retry_delay)
            Variable.set(config.var_known_ids, [], serialize_json=True)
            log.info(f"Successfully cleared all data for asset ID: {asset_id}")

    execute_operation()

manage_asset_dag()