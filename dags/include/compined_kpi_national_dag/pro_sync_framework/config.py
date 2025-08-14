import os
import json
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv
from airflow.exceptions import AirflowConfigException

class DagConfig:
    """Loads, validates, and provides access to all DAG configurations from a .env file."""

    def __init__(self, dag_prefix: str, config_dir: str = "dags/configs"):
        self.dag_prefix = dag_prefix.lower()
        dags_root_path = Path(__file__).parent.parent.parent
        config_path = dags_root_path / "configs" / f"{self.dag_prefix}.env"

        if not config_path.exists():
            raise AirflowConfigException(f"Configuration file not found at: {config_path}")

        load_dotenv(dotenv_path=config_path, override=True)
        self._load_configs()
        self._define_variable_names()

    def _load_configs(self):
        """Loads all required variables from the environment."""
        # DAG settings
        self.schedule = os.getenv("DAG_SCHEDULE", None)
        self.start_date = os.getenv("START_DATE", "2025-01-01")
        self.tags = [tag.strip() for tag in os.getenv("DAG_TAGS", "").split(",")]

        # Connection and batch settings
        self.db_conn_id = self._get_required_env("DB_CONN_ID")
        self.api_conn_id = self._get_required_env("API_CONN_ID")
        self.db_fetch_size = int(os.getenv("DB_FETCH_SIZE", 5000))
        self.api_push_size = int(os.getenv("API_PUSH_SIZE", 100))
        self.api_retry_count = int(os.getenv("API_RETRY_COUNT", 3))
        self.api_retry_delay = int(os.getenv("API_RETRY_DELAY_SECONDS", 30))

        # Source and destination settings
        self.db_view_name = self._get_required_env("DB_VIEW_NAME")
        pk_cols_str = self._get_required_env("PRIMARY_KEY_COLUMNS")
        self.primary_key_columns = [col.strip().lower() for col in pk_cols_str.split(",")]

        # Asset JSON config
        asset_config_str = self._get_required_env("ASSET_CONFIG")
        try:
            self.asset_config = json.loads(asset_config_str)
        except json.JSONDecodeError as e:
            raise AirflowConfigException(f"Invalid JSON in ASSET_CONFIG: {e}")

    def _define_variable_names(self):
        """Dynamically create the names for Airflow Variables."""
        self.var_asset_id = f"{self.dag_prefix}_asset_id"
        self.var_push_token = f"{self.dag_prefix}_push_token"
        self.var_known_ids = f"{self.dag_prefix}_known_ids"
        self.dag_id_manage = f"{self.dag_prefix}_manage_asset"
        self.dag_id_sync = f"{self.dag_prefix}_sync_data"

    def _get_required_env(self, key: str) -> str:
        """Utility to get a required environment variable."""
        value = os.getenv(key)
        if not value:
            raise AirflowConfigException(f"Required configuration '{key}' is missing from .env file.")
        return value