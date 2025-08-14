import logging
import math
import time
from typing import Any, Dict, List, Optional

from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

class ProHook(HttpHook):
    """A custom hook to interact with the Urbi Pro Dynamic Asset API with built-in retries."""

    def __init__(self, http_conn_id: str, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)

    def _get_auth_and_brand_headers(self, use_master_token: bool = True, custom_token: Optional[str] = None) -> Dict[str, str]:
        """Builds standard headers, using the master token from the connection by default."""
        conn = self.get_connection(self.http_conn_id)
        brand = conn.extra_dejson.get('x_brand_header', '2gis')
        
        token = custom_token
        if use_master_token:
            token = conn.password
        
        if not token:
            raise AirflowException("API token could not be determined.")
            
        return {
            'Authorization': f'Bearer {token}',
            'X-Brand': brand,
            'Content-Type': 'application/json'
        }

    def _sanitize_payload(self, obj: Any) -> Any:
        """Recursively sanitizes a payload, removing non-JSON-serializable values."""
        if isinstance(obj, dict):
            return {k: self._sanitize_payload(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._sanitize_payload(v) for v in obj]
        elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    def _run_with_retry(self, method: str, endpoint: str, retry_count: int, retry_delay: int, **kwargs) -> Any:
        """Wrapper for self.run() that includes a retry mechanism."""
        self.method = method
        for attempt in range(retry_count + 1):
            try:
                response = self.run(endpoint=endpoint, **kwargs)
                response.raise_for_status()
                log.info(f"{method} {endpoint} -> {response.status_code} (Attempt {attempt + 1})")
                return response.json() if response.text else {}
            except Exception as e:
                log.warning(f"Attempt {attempt + 1} failed for {method} {endpoint}: {e}")
                if attempt >= retry_count:
                    log.error(f"All {retry_count + 1} attempts failed. Raising exception.")
                    raise AirflowException(f"{method} {endpoint} failed: {e}")
                time.sleep(retry_delay)

    def create_or_update_asset(self, payload: Dict[str, Any], retry_count: int, retry_delay: int, asset_id: Optional[str] = None) -> Dict[str, Any]:
        """Creates or updates an asset using the master token."""
        method = 'PUT' if asset_id else 'POST'
        endpoint = "/dynamic_asset"
        if asset_id:
            payload['id'] = asset_id
        
        safe_payload = self._sanitize_payload(payload)
        headers = self._get_auth_and_brand_headers(use_master_token=True)
        
        return self._run_with_retry(
            method=method,
            endpoint=endpoint,
            retry_count=retry_count,
            retry_delay=retry_delay,
            json=safe_payload,
            headers=headers
        )

    def clear_all_asset_data(self, asset_id: str, retry_count: int, retry_delay: int) -> None:
        """Deletes all data objects from an asset using the master token."""
        endpoint = f"/dynamic_asset/{asset_id}/data/all"
        headers = self._get_auth_and_brand_headers(use_master_token=True)
        
        self._run_with_retry(
            method='DELETE',
            endpoint=endpoint,
            retry_count=retry_count,
            retry_delay=retry_delay,
            headers=headers
        )

    def push_data(self, asset_id: str, access_token: str, data: List[Any], retry_count: int, retry_delay: int, is_delete: bool = False) -> None:
        """Pushes a chunk of data (upserts or deletes) using the asset-specific token."""
        if not data:
            log.info("No data to push or delete, skipping API call.")
            return

        endpoint = f"/dynamic_asset/{asset_id}/data"
        headers = self._get_auth_and_brand_headers(use_master_token=False, custom_token=access_token)
        
        if is_delete:
            method = 'DELETE'
            # ✨ FIX: The payload is the raw list of IDs, not a dictionary.
            payload = data
        else:
            method = 'PUT'
            payload = {'type': 'FeatureCollection', 'features': data}

        safe_payload = self._sanitize_payload(payload)

        self._run_with_retry(
            method=method,
            endpoint=endpoint,
            retry_count=retry_count,
            retry_delay=retry_delay,
            json=safe_payload,
            headers=headers
        )